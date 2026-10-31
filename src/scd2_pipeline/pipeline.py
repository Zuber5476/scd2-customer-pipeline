import csv
import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .db import connect
from .validation import EXPECTED_COLUMNS, payload_hash, validate_record

logger = logging.getLogger(__name__)
OPEN_END = "9999-12-31T00:00:00Z"
STATE_FIELDS = ("customer_name", "email", "city", "segment")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def deterministic_id(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _reject(conn, run_id, source_file, row_number, raw, code, message):
    conn.execute(
        """INSERT INTO rejects(run_id, source_file, source_row_number, raw_record,
           error_code, error_message, rejected_at) VALUES (?,?,?,?,?,?,?)""",
        (run_id, source_file, row_number, json.dumps(raw, sort_keys=False), code, message, utc_now()),
    )


def _rebuild_customer(conn, customer_id: str) -> int:
    events = conn.execute(
        """SELECT event_id, customer_name, email, city, segment, effective_at
           FROM source_events WHERE customer_id=? ORDER BY effective_at, event_id""",
        (customer_id,),
    ).fetchall()

    transitions = []
    previous_state = None
    for event in events:
        state = tuple(event[field] for field in STATE_FIELDS)
        if state == previous_state:
            continue
        transitions.append((event, state))
        previous_state = state

    conn.execute("DELETE FROM dim_customer WHERE customer_id=?", (customer_id,))
    written = 0
    for index, (event, state) in enumerate(transitions):
        effective_to = transitions[index + 1][0]["effective_at"] if index + 1 < len(transitions) else OPEN_END
        if event["effective_at"] >= effective_to:
            raise ValueError(f"Non-increasing effective interval for customer_id={customer_id}")
        state_digest = hashlib.sha256(json.dumps(state, separators=(",", ":")).encode()).hexdigest()
        customer_sk = deterministic_id(customer_id, event["effective_at"], state_digest)
        conn.execute(
            """INSERT INTO dim_customer(customer_sk, customer_id, customer_name, email, city,
               segment, effective_from, effective_to, is_current, record_hash, source_event_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (customer_sk, customer_id, *state, event["effective_at"], effective_to,
             int(effective_to == OPEN_END), state_digest, event["event_id"]),
        )
        written += 1
    return written


def process_csv(csv_path: Path | str, database_path: Path | str) -> dict:
    """Load a CSV batch. Valid rows and dimension changes commit atomically."""
    csv_path = Path(csv_path)
    started = utc_now()
    run_id = str(uuid.uuid4())
    source_file = csv_path.name
    conn = connect(database_path)
    conn.execute(
        "INSERT INTO pipeline_runs(run_id,source_file,started_at,status) VALUES (?,?,?,'running')",
        (run_id, source_file, started),
    )
    conn.commit()
    input_count = inserted_count = duplicate_count = rejected_count = 0
    dimension_rows_written = 0
    started_clock = time.perf_counter()

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != EXPECTED_COLUMNS:
                raise ValueError(
                    f"Unexpected CSV schema. Expected {EXPECTED_COLUMNS}; received {reader.fieldnames}"
                )
            with conn:
                affected_customers = set()
                batch_event_ids = {}
                for row_number, raw in enumerate(reader, start=2):
                    input_count += 1
                    payload, error_code, error_message = validate_record(raw)
                    if error_code:
                        _reject(conn, run_id, source_file, row_number, raw, error_code, error_message)
                        rejected_count += 1
                        continue

                    digest = payload_hash(payload)
                    event_id = payload["event_id"]
                    if event_id in batch_event_ids:
                        if batch_event_ids[event_id] == digest:
                            duplicate_count += 1
                        else:
                            _reject(conn, run_id, source_file, row_number, raw,
                                    "CONFLICTING_EVENT_ID", "event_id was repeated with a different payload")
                            rejected_count += 1
                        continue
                    batch_event_ids[event_id] = digest

                    existing = conn.execute(
                        "SELECT payload_hash FROM source_events WHERE event_id=?", (event_id,)
                    ).fetchone()
                    if existing:
                        if existing["payload_hash"] == digest:
                            duplicate_count += 1
                        else:
                            _reject(conn, run_id, source_file, row_number, raw,
                                    "CONFLICTING_EVENT_ID", "event_id already exists with a different payload")
                            rejected_count += 1
                        continue

                    same_instant = conn.execute(
                        """SELECT event_id, payload_hash FROM source_events
                           WHERE customer_id=? AND effective_at=?""",
                        (payload["customer_id"], payload["effective_at"]),
                    ).fetchone()
                    if same_instant:
                        _reject(conn, run_id, source_file, row_number, raw,
                                "CONFLICTING_EFFECTIVE_TIMESTAMP",
                                f"Customer already has event {same_instant['event_id']} at this effective timestamp")
                        rejected_count += 1
                        continue

                    conn.execute(
                        """INSERT INTO source_events(event_id,customer_id,customer_name,email,city,segment,
                           effective_at,payload_hash,source_file,source_row_number,first_seen_run_id)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (payload["event_id"], payload["customer_id"], payload["customer_name"],
                         payload["email"], payload["city"], payload["segment"], payload["effective_at"],
                         digest, source_file, row_number, run_id),
                    )
                    inserted_count += 1
                    affected_customers.add(payload["customer_id"])

                for customer_id in sorted(affected_customers):
                    dimension_rows_written += _rebuild_customer(conn, customer_id)

                ended = utc_now()
                conn.execute(
                    """UPDATE pipeline_runs SET ended_at=?,status='succeeded',input_count=?,
                       inserted_count=?,duplicate_count=?,rejected_count=?,dimension_rows_written=?
                       WHERE run_id=?""",
                    (ended, input_count, inserted_count, duplicate_count, rejected_count,
                     dimension_rows_written, run_id),
                )
        elapsed = round(time.perf_counter() - started_clock, 6)
        logger.info("run_id=%s status=succeeded input=%d inserted=%d duplicate=%d rejected=%d elapsed_s=%s",
                    run_id, input_count, inserted_count, duplicate_count, rejected_count, elapsed)
        return {"run_id": run_id, "status": "succeeded", "input_count": input_count,
                "inserted_count": inserted_count, "duplicate_count": duplicate_count,
                "rejected_count": rejected_count, "dimension_rows_written": dimension_rows_written,
                "elapsed_seconds": elapsed}
    except Exception as exc:
        conn.rollback()
        ended = utc_now()
        conn.execute(
            """UPDATE pipeline_runs SET ended_at=?,status='failed',input_count=?,
               inserted_count=0,duplicate_count=0,rejected_count=0,dimension_rows_written=0,
               failure_reason=? WHERE run_id=?""",
            (ended, input_count, str(exc), run_id),
        )
        conn.commit()
        logger.exception("run_id=%s failed: %s", run_id, exc)
        return {"run_id": run_id, "status": "failed", "input_count": input_count,
                "inserted_count": 0, "duplicate_count": 0, "rejected_count": 0,
                "dimension_rows_written": 0, "failure_reason": str(exc)}
    finally:
        conn.close()
