import csv
import pytest
from pathlib import Path
from scd2_pipeline.validation import EXPECTED_COLUMNS

@pytest.fixture
def write_csv(tmp_path):
    def _write(name, rows, columns=None):
        path = tmp_path / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns or EXPECTED_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        return path
    return _write

@pytest.fixture
def event():
    def _event(event_id="e1", customer_id="c1", name="Ada", city="Tirupati",
              segment="consumer", at="2025-01-01T00:00:00Z", email="ada@example.test"):
        return {"event_id": event_id, "customer_id": customer_id, "customer_name": name,
                "email": email, "city": city, "segment": segment, "effective_at": at}
    return _event
