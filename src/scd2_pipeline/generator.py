import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .validation import EXPECTED_COLUMNS


def generate_dataset(output_path: Path | str, records: int = 1000, seed: int = 42) -> Path:
    """Generate deterministic customer events. Includes deliberate bad/duplicate rows."""
    if records < 1:
        raise ValueError("records must be at least 1")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    customers = max(1, records // 3)
    segments = ["consumer", "small_business", "enterprise"]
    for index in range(records):
        customer_number = (index % customers) + 1
        event_time = base + timedelta(days=index // customers, minutes=(index % customers) * 3)
        rows.append({
            "event_id": f"evt_{index + 1:07d}",
            "customer_id": f"cust_{customer_number:06d}",
            "customer_name": f"Customer {customer_number:06d}",
            "email": f"customer{customer_number:06d}@example.test",
            "city": rng.choice(["Tirupati", "Hyderabad", "Bengaluru", "Chennai"]),
            "segment": segments[(index // customers) % len(segments)],
            "effective_at": event_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
        })

    # Add deliberately labelled negative examples without changing the requested valid count.
    if records >= 10:
        bad = dict(rows[-1]); bad["event_id"] = "invalid_missing_customer"; bad["customer_id"] = ""
        rows.append(bad)
        bad_ref = dict(rows[-2]); bad_ref["event_id"] = "invalid_segment"; bad_ref["segment"] = "vip_unknown"
        rows.append(bad_ref)
        duplicate = dict(rows[0]); rows.append(duplicate)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return output_path
