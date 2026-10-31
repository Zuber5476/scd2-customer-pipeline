import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ref_customer_segment (
    segment TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
    input_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    dimension_rows_written INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS source_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    email TEXT,
    city TEXT,
    segment TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    first_seen_run_id TEXT NOT NULL,
    FOREIGN KEY (segment) REFERENCES ref_customer_segment(segment),
    FOREIGN KEY (first_seen_run_id) REFERENCES pipeline_runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_source_events_customer_time
    ON source_events(customer_id, effective_at, event_id);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    email TEXT,
    city TEXT,
    segment TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT NOT NULL,
    is_current INTEGER NOT NULL CHECK (is_current IN (0,1)),
    record_hash TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    FOREIGN KEY (source_event_id) REFERENCES source_events(event_id),
    UNIQUE(customer_id, effective_from),
    CHECK (effective_from < effective_to)
);
CREATE INDEX IF NOT EXISTS idx_dim_customer_natural_key
    ON dim_customer(customer_id, is_current);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    raw_record TEXT NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    rejected_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_rejects_run ON rejects(run_id);
"""

SEGMENTS = ("consumer", "small_business", "enterprise")


def connect(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT OR IGNORE INTO ref_customer_segment(segment) VALUES (?)",
        [(segment,) for segment in SEGMENTS],
    )
    conn.commit()
    return conn
