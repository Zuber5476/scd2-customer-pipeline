# Data Dictionary

## Source CSV: `customer_events.csv`

Grain: one customer attribute-change event per row. `event_id` is the stable source event key; `customer_id` is the natural/business key. The event's `effective_at` is the time the change became true in the business domain, not the ingestion time.

| Column | Type | Required | Rule |
|---|---|---:|---|
| event_id | string | Yes | Stable unique event identifier; same ID + same payload is a duplicate; same ID + different payload is rejected |
| customer_id | string | Yes | Stable natural key for a customer |
| customer_name | string | Yes | Non-empty customer display name |
| email | string | No | Null/empty allowed; simple local@domain validation when present |
| city | string | No | Null/empty allowed |
| segment | string | Yes | Foreign key to `ref_customer_segment`; allowed: consumer, small_business, enterprise |
| effective_at | ISO-8601 timestamp | Yes | Must include timezone; normalized to UTC |

## Tables

- `source_events`: immutable accepted event ledger. Primary key `event_id`; retains source file, source row number, and first-seen run ID.
- `dim_customer`: SCD Type 2 dimension. Grain: one distinct consecutive customer state over a validity interval. `customer_sk` is a deterministic SHA-256-derived surrogate key. `effective_to` is exclusive; current rows use `9999-12-31T00:00:00Z` and `is_current=1`.
- `ref_customer_segment`: controlled segment reference values.
- `pipeline_runs`: one operational record per execution, including counts, timestamps, status, and failure reason.
- `rejects`: one row per quarantined source row, with original record, source location, error code, and run ID.

## Nullability and relationships

- `source_events.segment` references `ref_customer_segment.segment`.
- `dim_customer.source_event_id` references the event that introduced the version.
- `dim_customer.customer_id` intentionally has no FK to a separate customer master: the event stream itself creates customer identities.
- `email` and `city` are nullable attributes; missing values are normalized to SQL NULL.
- One accepted event ID is unique. One customer may have many events and many dimension versions.
- Each customer represented in the dimension must have exactly one current row. Validity intervals for a customer must not overlap.

## Derived fields

- `payload_hash`: hash of canonical source event values, used for idempotency/conflict detection.
- `record_hash`: hash of the dimension state attributes, used for change detection.
- `customer_sk`: deterministic ID from customer ID, effective start, and state hash.
- `effective_to`: next distinct state transition's effective timestamp, or the open-ended sentinel.
- `is_current`: 1 only for the open-ended version.
