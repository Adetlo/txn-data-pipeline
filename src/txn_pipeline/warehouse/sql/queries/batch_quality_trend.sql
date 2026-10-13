-- Batch quality trend: pass rate, rejections and reconciliation outcome per batch
SELECT started_at, batch_id, status, record_count, accepted_count, rejected_count, duplicate_count,
       ROUND(100.0 * quality_pass_rate, 2) AS quality_pass_rate_pct, reconciliation_balanced
FROM fact_batch_quality
ORDER BY started_at;
