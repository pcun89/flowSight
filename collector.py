"""
collector.py
UDP listener that accepts newline-delimited JSON flow records for demo.
Each flow record JSON example:
{"src_ip":"192.168.1.10","dst_ip":"8.8.8.8","src_port":54321,"dst_port":53,"protocol":17,"bytes":120,"packets":1,"ts":1690000000}
"""

import socket
import threading
import json
import logging
from typing import Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_LISTEN_HOST = "0.0.0.0"
DEFAULT_LISTEN_PORT = 9999
BUFFER_SIZE = 65535


class UDPFlowCollector:
    """
    UDP listener that calls on_flow(dict) for each parsed flow JSON.
    Time complexity: O(1) per received flow (parsing + callback).
    Data structures: uses socket and calls back to aggregator.
    """

    def __init__(self, host: str = DEFAULT_LISTEN_HOST, port: int = DEFAULT_LISTEN_PORT, on_flow: Callable = None):
        self.host = host
        self.port = port
        self.on_flow = on_flow
        self._sock = None
        self._stop_event = threading.Event()

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        logger.info(f"UDPFlowCollector started on {self.host}:{self.port}")

    def stop(self):
        self._stop_event.set()
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass

    def _run(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((self.host, self.port))
        while not self._stop_event.is_set():
            try:
                data, addr = self._sock.recvfrom(BUFFER_SIZE)
                if not data:
                    continue
                # handle possibly multiple lines
                text = data.decode("utf-8", errors="ignore")
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        flow = json.loads(line)
                        if self.on_flow:
                            self.on_flow(flow)
                    except json.JSONDecodeError:
                        logger.debug(f"Invalid JSON from {addr}: {line[:80]}")
                    except Exception as e:
                        logger.exception(f"Error handling flow: {e}")
            except Exception as e:
                logger.exception(f"Socket recv error: {e}")
                break
