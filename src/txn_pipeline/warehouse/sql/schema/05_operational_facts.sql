-- Operational facts: batch quality (from metadata) and quarantine (from Parquet).
CREATE OR REPLACE TABLE fact_batch_quality AS
SELECT batch_id, source_name, source_filename, source_hash, status, actor, pipeline_version,
       attempt, started_at, completed_at,
       record_count, accepted_count, rejected_count, duplicate_count, conflict_count, warning_count,
       reconciliation_balanced,
       CASE WHEN record_count > 0 THEN CAST(accepted_count AS DOUBLE) / record_count END AS quality_pass_rate,
       CASE WHEN record_count > 0 THEN CAST(duplicate_count AS DOUBLE) / record_count END AS duplicate_rate
FROM src_batches;

CREATE OR REPLACE TABLE fact_quarantine AS
SELECT batch_id, record_number, transaction_id, rule_id, all_rule_ids, severity, stage,
       quarantined_at, CAST(quarantined_at AS DATE) AS quarantined_date, source_filename, source_file_hash
FROM src_quarantine;
