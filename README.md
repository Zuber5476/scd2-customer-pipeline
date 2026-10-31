# SCD Type 2 Customer Dimension Pipeline

A reproducible Data Engineering project implementing a customer SCD Type 2 dimension with effective dates, current-row flags, change detection, duplicate handling, late-arriving events, quarantine, run auditing, reconciliation, and automated tests.

## Quick start

Requirements: Python 3.11 or newer.

```bash
python -m venv .venv
python -m pip install -e ".[test]"
scd2 run --input data/sample/customer_events.csv --db warehouse/sample.db
scd2 report --db warehouse/sample.db --output reports/sample_validation_report.json
python -m pytest -q
```

No cloud account, API key, or external database is required.

## Architecture

CSV / deterministic generator -> schema and row validation -> immutable source event ledger -> SCD2 timeline rebuild for affected customer IDs -> customer dimension and run audit -> reconciliation reports and SQL checks.

The event ledger stores accepted source changes. For each affected customer, accepted events are sorted by `effective_at` and the dimension timeline is rebuilt. This favors correctness and explainability over maximum throughput.

## SCD Type 2 rules

- Natural key: `customer_id`; source event key: `event_id`.
- Tracked attributes: customer name, email, city, and segment.
- Changed tracked attributes close the previous row and insert a new version.
- Identical consecutive states do not create redundant dimension versions.
- `effective_from` is inclusive; `effective_to` is exclusive.
- Current row uses `9999-12-31T00:00:00Z` and `is_current = 1`.
- Timestamps require a timezone and are normalized to UTC.
- Late events trigger a recalculation of the affected customer's timeline.
- Identical duplicate event IDs are ignored; conflicting payloads are rejected.
- Invalid rows are quarantined with source and run metadata.

See `docs/data_dictionary.md` and `docs/design_tradeoffs.md` for details.

## Commands

```bash
scd2 generate --output data/generated/events_100k.csv --records 100000 --seed 42
scd2 run --input data/generated/events_100k.csv --db warehouse/large.db
scd2 report --db warehouse/large.db --output reports/large_report.json
```

## Tests and evidence

Run `python -m pytest -q`. The recorded test run passed 15 tests. Sample inputs, expected dimension output, validation reports, run metrics, and a 100K benchmark are included in this repository.

## Operational behavior

Each invocation records a unique `run_id`. Accepted events, rejects, dimension rebuilds, and success metrics are committed atomically. Event IDs and payload hashes make reruns idempotent. SQLite uses WAL mode and foreign-key checks; concurrent writers to one database are not supported.

## Known limitations

This is a single-process SQLite implementation, not a distributed streaming platform. Same-effective-time conflicts use a documented first-accepted-event rule. Large histories currently rebuild all events for each affected customer; incremental interval maintenance is the next optimization.

Do not commit secrets or real customer data.