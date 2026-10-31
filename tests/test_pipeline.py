import sqlite3
from scd2_pipeline.db import connect
from scd2_pipeline.pipeline import process_csv, OPEN_END


def rows(db, sql, params=()):
    conn = connect(db)
    try:
        return [dict(row) for row in conn.execute(sql, params)]
    finally:
        conn.close()


def test_happy_path_creates_effective_dated_versions(tmp_path, write_csv, event):
    db = tmp_path / "warehouse.db"
    source = write_csv("events.csv", [
        event("e1", at="2025-01-01T00:00:00Z"),
        event("e2", city="Hyderabad", at="2025-02-01T00:00:00Z"),
    ])
    result = process_csv(source, db)
    assert result["status"] == "succeeded"
    versions = rows(db, "SELECT * FROM dim_customer ORDER BY effective_from")
    assert len(versions) == 2
    assert versions[0]["effective_to"] == "2025-02-01T00:00:00Z"
    assert versions[0]["is_current"] == 0
    assert versions[1]["effective_to"] == OPEN_END
    assert versions[1]["is_current"] == 1


def test_empty_input_succeeds(tmp_path, write_csv):
    db = tmp_path / "empty.db"
    source = write_csv("empty.csv", [])
    result = process_csv(source, db)
    assert result["status"] == "succeeded"
    assert result["input_count"] == 0
    assert result["inserted_count"] == 0


def test_missing_required_field_is_quarantined(tmp_path, write_csv, event):
    db = tmp_path / "null.db"
    bad = event("e1"); bad["customer_id"] = ""
    result = process_csv(write_csv("null.csv", [bad]), db)
    assert result["rejected_count"] == 1
    assert rows(db, "SELECT error_code FROM rejects")[0]["error_code"] == "MISSING_REQUIRED_FIELD"
    assert rows(db, "SELECT * FROM source_events") == []


def test_duplicate_event_in_same_batch_is_idempotent(tmp_path, write_csv, event):
    db = tmp_path / "dupe.db"
    first = event("e1")
    result = process_csv(write_csv("dupe.csv", [first, dict(first)]), db)
    assert result["inserted_count"] == 1
    assert result["duplicate_count"] == 1
    assert len(rows(db, "SELECT * FROM dim_customer")) == 1


def test_rerunning_same_file_does_not_create_versions(tmp_path, write_csv, event):
    db = tmp_path / "rerun.db"
    source = write_csv("same.csv", [event("e1")])
    first = process_csv(source, db)
    second = process_csv(source, db)
    assert first["inserted_count"] == 1
    assert second["inserted_count"] == 0
    assert second["duplicate_count"] == 1
    assert len(rows(db, "SELECT * FROM dim_customer")) == 1


def test_repeated_identical_state_does_not_create_redundant_version(tmp_path, write_csv, event):
    db = tmp_path / "repeat.db"
    source = write_csv("repeat.csv", [
        event("e1", at="2025-01-01T00:00:00Z"),
        event("e2", at="2025-02-01T00:00:00Z"),
    ])
    process_csv(source, db)
    versions = rows(db, "SELECT * FROM dim_customer")
    assert len(versions) == 1
    assert versions[0]["source_event_id"] == "e1"


def test_late_arriving_event_rebuilds_customer_timeline(tmp_path, write_csv, event):
    db = tmp_path / "late.db"
    first = write_csv("first.csv", [
        event("e1", at="2025-01-01T00:00:00Z"),
        event("e3", city="Chennai", at="2025-03-01T00:00:00Z"),
    ])
    process_csv(first, db)
    late = write_csv("late.csv", [event("e2", city="Hyderabad", at="2025-02-01T00:00:00Z")])
    result = process_csv(late, db)
    assert result["status"] == "succeeded"
    versions = rows(db, "SELECT * FROM dim_customer ORDER BY effective_from")
    assert [v["effective_from"] for v in versions] == [
        "2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z", "2025-03-01T00:00:00Z"]
    assert [v["city"] for v in versions] == ["Tirupati", "Hyderabad", "Chennai"]
    assert versions[-1]["is_current"] == 1


def test_invalid_reference_key_is_rejected(tmp_path, write_csv, event):
    db = tmp_path / "ref.db"
    bad = event("e1", segment="vip")
    result = process_csv(write_csv("bad_ref.csv", [bad]), db)
    assert result["rejected_count"] == 1
    assert rows(db, "SELECT error_code FROM rejects")[0]["error_code"] == "INVALID_REFERENCE_KEY"


def test_conflicting_events_at_same_effective_timestamp_are_rejected(tmp_path, write_csv, event):
    db = tmp_path / "conflict.db"
    source = write_csv("conflict.csv", [
        event("e1", city="Tirupati"), event("e2", city="Hyderabad")
    ])
    result = process_csv(source, db)
    assert result["inserted_count"] == 1
    assert result["rejected_count"] == 1
    assert rows(db, "SELECT error_code FROM rejects")[0]["error_code"] == "CONFLICTING_EFFECTIVE_TIMESTAMP"


def test_malformed_extra_field_is_quarantined(tmp_path, event):
    db = tmp_path / "malformed.db"
    path = tmp_path / "malformed.csv"
    path.write_text(
        "event_id,customer_id,customer_name,email,city,segment,effective_at\n"
        "e1,c1,Ada,ada@example.test,Tirupati,consumer,2025-01-01T00:00:00Z,EXTRA\n",
        encoding="utf-8",
    )
    result = process_csv(path, db)
    assert result["rejected_count"] == 1
    assert rows(db, "SELECT error_code FROM rejects")[0]["error_code"] == "MALFORMED_ROW"


def test_schema_drift_fails_run_without_loading_data(tmp_path):
    db = tmp_path / "drift.db"
    path = tmp_path / "drift.csv"
    path.write_text("event_id,customer_id,unexpected_column\ne1,c1,x\n", encoding="utf-8")
    result = process_csv(path, db)
    assert result["status"] == "failed"
    assert "Unexpected CSV schema" in result["failure_reason"]
    assert rows(db, "SELECT * FROM source_events") == []


def test_partial_failure_rolls_back_and_retry_succeeds(tmp_path, write_csv, event):
    db = tmp_path / "retry.db"
    source = write_csv("retry.csv", [event("e1")])
    conn = connect(db)
    conn.execute("CREATE TRIGGER fail_dimension BEFORE INSERT ON dim_customer BEGIN SELECT RAISE(ABORT, 'simulated failure'); END")
    conn.commit(); conn.close()
    failed = process_csv(source, db)
    assert failed["status"] == "failed"
    assert rows(db, "SELECT * FROM source_events") == []
    conn = connect(db); conn.execute("DROP TRIGGER fail_dimension"); conn.commit(); conn.close()
    retried = process_csv(source, db)
    assert retried["status"] == "succeeded"
    assert len(rows(db, "SELECT * FROM dim_customer")) == 1


def test_optional_null_attributes_are_stored_as_sql_null(tmp_path, write_csv, event):
    db = tmp_path / "optional_null.db"
    record = event("e1", email="", city="")
    result = process_csv(write_csv("optional_null.csv", [record]), db)
    version = rows(db, "SELECT email, city FROM dim_customer")[0]
    assert result["status"] == "succeeded"
    assert version["email"] is None
    assert version["city"] is None


def test_fractional_second_timestamps_are_not_truncated(tmp_path, write_csv, event):
    db = tmp_path / "precision.db"
    source = write_csv("precision.csv", [
        event("e1", at="2025-01-01T00:00:00.100Z"),
        event("e2", city="Hyderabad", at="2025-01-01T00:00:00.900Z"),
    ])
    result = process_csv(source, db)
    versions = rows(db, "SELECT effective_from, effective_to FROM dim_customer ORDER BY effective_from")
    assert result["status"] == "succeeded"
    assert len(versions) == 2
    assert versions[0]["effective_from"] == "2025-01-01T00:00:00.100000Z"
    assert versions[0]["effective_to"] == "2025-01-01T00:00:00.900000Z"


def test_conflicting_reuse_of_event_id_is_rejected(tmp_path, write_csv, event):
    db = tmp_path / "event_id_conflict.db"
    first = event("same_id")
    changed = event("same_id", city="Hyderabad")
    result = process_csv(write_csv("event_id_conflict.csv", [first, changed]), db)
    assert result["inserted_count"] == 1
    assert result["rejected_count"] == 1
    assert rows(db, "SELECT error_code FROM rejects")[0]["error_code"] == "CONFLICTING_EVENT_ID"
