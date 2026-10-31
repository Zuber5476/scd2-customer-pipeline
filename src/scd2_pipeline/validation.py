import hashlib
import json
from datetime import datetime, timezone
from typing import Any

EXPECTED_COLUMNS = [
    "event_id", "customer_id", "customer_name", "email",
    "city", "segment", "effective_at",
]


def canonical_timestamp(value: str) -> str:
    """Parse an ISO-8601 timestamp and normalize it to UTC with Z suffix."""
    if not value or not value.strip():
        raise ValueError("effective_at is required")
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError("effective_at must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("effective_at must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="auto").replace("+00:00", "Z")


def canonical_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": record["event_id"].strip(),
        "customer_id": record["customer_id"].strip(),
        "customer_name": record["customer_name"].strip(),
        "email": (record.get("email") or "").strip() or None,
        "city": (record.get("city") or "").strip() or None,
        "segment": record["segment"].strip().lower(),
        "effective_at": canonical_timestamp(record["effective_at"]),
    }


def payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def validate_record(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    if None in record:
        return None, "MALFORMED_ROW", "Record contains more fields than the declared CSV schema"
    missing_columns = [column for column in EXPECTED_COLUMNS if column not in record]
    if missing_columns:
        return None, "SCHEMA_MISMATCH", f"Missing columns: {', '.join(missing_columns)}"

    for field in ("event_id", "customer_id", "customer_name", "segment", "effective_at"):
        if not record.get(field) or not str(record[field]).strip():
            return None, "MISSING_REQUIRED_FIELD", f"{field} is required"

    try:
        payload = canonical_payload(record)
    except (ValueError, TypeError) as exc:
        return None, "INVALID_TIMESTAMP", str(exc)

    if payload["segment"] not in {"consumer", "small_business", "enterprise"}:
        return None, "INVALID_REFERENCE_KEY", f"Unknown customer segment: {payload['segment']}"
    if payload["email"] and ("@" not in payload["email"] or payload["email"].startswith("@") or payload["email"].endswith("@")):
        return None, "INVALID_EMAIL", "email must contain a local part and domain separated by @"
    return payload, None, None
