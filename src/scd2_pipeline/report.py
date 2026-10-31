import json
from pathlib import Path

from .db import connect


def build_report(database_path: Path | str, output_path: Path | str) -> dict:
    conn = connect(database_path)
    try:
        current_count = conn.execute("SELECT COUNT(*) FROM dim_customer WHERE is_current=1").fetchone()[0]
        history_count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
        customer_count = conn.execute("SELECT COUNT(DISTINCT customer_id) FROM dim_customer").fetchone()[0]
        reject_count = conn.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
        run_rows = [dict(row) for row in conn.execute(
            "SELECT run_id,source_file,status,input_count,inserted_count,duplicate_count,rejected_count,"
            "dimension_rows_written,failure_reason FROM pipeline_runs ORDER BY started_at"
        )]
        # Every customer has exactly one current row and no overlapping intervals.
        current_violations = conn.execute(
            "SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING SUM(is_current) != 1"
        ).fetchall()
        overlap_violations = conn.execute("""
            SELECT a.customer_id FROM dim_customer a JOIN dim_customer b
              ON a.customer_id=b.customer_id AND a.customer_sk < b.customer_sk
             AND a.effective_from < b.effective_to AND b.effective_from < a.effective_to
        """).fetchall()
        report = {
            "source_event_count": conn.execute("SELECT COUNT(*) FROM source_events").fetchone()[0],
            "dimension_row_count": history_count,
            "customer_count": customer_count,
            "current_row_count": current_count,
            "reject_count": reject_count,
            "current_row_violations": [row[0] for row in current_violations],
            "overlapping_interval_violations": [row[0] for row in overlap_violations],
            "runs": run_rows,
        }
        report["validation_passed"] = not report["current_row_violations"] and not report["overlapping_interval_violations"]
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        conn.close()
