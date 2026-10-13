-- Duplicate rate per batch
SELECT batch_id, source_filename, record_count, duplicate_count, conflict_count,
       ROUND(100.0 * duplicate_rate, 3) AS duplicate_rate_pct
FROM fact_batch_quality
ORDER BY started_at;
