"""
storage.py
Simple SQLite persistence for periodic snapshots of top talkers.

Tables:
- snapshot (id PK, ts INTEGER, metric TEXT, key TEXT, bytes INTEGER)
"""

import sqlite3
import time
from typing import List, Tuple

DB_PATH = "flowsight.db"


def init_db(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        metric TEXT,
        key TEXT,
        bytes INTEGER
    )""")
    conn.commit()
    conn.close()


def persist_top_list(metric: str, top_list: List[Tuple[str, int]], ts: int = None, db_path: str = DB_PATH):
    """
    Persist a top-k list. top_list element: (key, bytes)
    """
    ts = ts or int(time.time())
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for key, b in top_list:
        c.execute("INSERT INTO snapshot (ts, metric, key, bytes) VALUES (?, ?, ?, ?)",
                  (ts, metric, str(key), int(b)))
    conn.commit()
    conn.close()


def recent_snapshots(limit: int = 100, db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT ts, metric, key, bytes FROM snapshot ORDER BY ts DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows
