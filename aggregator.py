"""
aggregator.py
Aggregate flows into counters and expose top-talkers.

Data structures used:
- collections.Counter (dict-like) for bytes per src_ip and per (src_ip,dst_ip).
- small min-heap (heapq) for retrieving top-K efficiently if needed.

Time complexities:
- update_flow: O(1) amortized per counter increment.
- top_k_by_bytes: O(N log K) where N = number of tracked keys, K = requested top size.
"""

from collections import Counter, defaultdict
import heapq
import threading
from typing import Dict, Tuple, List


class FlowAggregator:
    def __init__(self):
        # counters
        self._lock = threading.Lock()
        self.bytes_by_src = Counter()           # src_ip -> total bytes
        self.packets_by_src = Counter()         # src_ip -> total packets
        self.bytes_by_pair = Counter()          # (src_ip,dst_ip) -> total bytes
        self.flow_count = 0

    def update_flow(self, flow: Dict):
        """
        Update internal counters from a single flow record dict.
        Expected keys: src_ip, dst_ip, bytes, packets
        Time: O(1)
        """
        src = flow.get("src_ip")
        dst = flow.get("dst_ip")
        b = int(flow.get("bytes", 0))
        p = int(flow.get("packets", 0))
        with self._lock:
            if src:
                self.bytes_by_src[src] += b
                self.packets_by_src[src] += p
            if src and dst:
                self.bytes_by_pair[(src, dst)] += b
            self.flow_count += 1

    def top_k_src(self, k: int = 10) -> List[Tuple[str, int]]:
        """
        Return top-k source IPs by bytes descending.
        Implementation: use heapq.nlargest on Counter.items (O(N + k log N) or O(N log k) depending).
        Time complexity: O(N log k)
        """
        with self._lock:
            return heapq.nlargest(k, self.bytes_by_src.items(), key=lambda x: x[1])

    def top_k_pairs(self, k: int = 10) -> List[Tuple[Tuple[str, str], int]]:
        with self._lock:
            return heapq.nlargest(k, self.bytes_by_pair.items(), key=lambda x: x[1])

    def snapshot_metrics(self):
        with self._lock:
            return {
                "flow_count": self.flow_count,
                "unique_srcs": len(self.bytes_by_src),
                "unique_pairs": len(self.bytes_by_pair)
            }

    def reset(self):
        with self._lock:
            self.bytes_by_src.clear()
            self.packets_by_src.clear()
            self.bytes_by_pair.clear()
            self.flow_count = 0
