-- 1. Exactly one current row per customer.
SELECT customer_id, SUM(is_current) AS current_rows
FROM dim_customer
GROUP BY customer_id
HAVING SUM(is_current) <> 1;

-- 2. Detect overlapping validity intervals.
SELECT a.customer_id, a.customer_sk, b.customer_sk
FROM dim_customer a
JOIN dim_customer b
  ON a.customer_id = b.customer_id
 AND a.customer_sk < b.customer_sk
 AND a.effective_from < b.effective_to
 AND b.effective_from < a.effective_to;

-- 3. Source events, dimension versions, and rejects.
SELECT
  (SELECT COUNT(*) FROM source_events) AS accepted_source_events,
  (SELECT COUNT(*) FROM dim_customer) AS dimension_versions,
  (SELECT COUNT(*) FROM rejects) AS rejected_records;

-- 4. Inspect a customer's full history.
SELECT customer_id, customer_name, city, segment,
       effective_from, effective_to, is_current, source_event_id
FROM dim_customer
WHERE customer_id = 'cust_001'
ORDER BY effective_from;

-- 5. Operational run audit.
SELECT run_id, source_file, status, input_count, inserted_count,
       duplicate_count, rejected_count, dimension_rows_written, failure_reason
FROM pipeline_runs
ORDER BY started_at DESC;
