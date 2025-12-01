# FlowSight — Lightweight Flow Collector & Top-Talkers Analyzer

FlowSight is a compact Python project for collecting flow records (demo mode: newline-delimited JSON via UDP), aggregating bytes/packets per source/destination, computing top talkers, persisting periodic snapshots to SQLite, and exposing a small Flask API/dashboard.

---

## Features

- UDP flow collector (demo mode using JSON) — easy to feed with `nc` or scripts
- Aggregation of bytes/packets per `src_ip` and `(src_ip, dst_ip)` pair
- Retrieve top-K talkers (sources and pairs)
- Periodic persistence of top lists into SQLite (`flowsight.db`)
- Simple Flask UI and JSON API

---

## Quick Start

1. Create a virtual environment and install deps:
```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
