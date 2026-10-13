-- Account dimensions use a deterministic 63-bit hash surrogate so a full rebuild yields the
-- same keys (ADR 0007 in docs/WAREHOUSE_MODEL.md). Sentinel row -1 = unknown counterparty.
CREATE OR REPLACE TABLE dim_account AS
SELECT CAST(hash(account_id) >> 1 AS BIGINT)  AS account_key,
       account_id,
       starts_with(account_id, 'ACC-')         AS is_internal,
       MIN(booking_date)                       AS first_seen_date,
       MAX(booking_date)                       AS last_seen_date,
       COUNT(*)                                AS transaction_count
FROM stg_curated
GROUP BY account_id;

CREATE OR REPLACE TABLE dim_counterparty AS
SELECT CAST(hash(counterparty_id) >> 1 AS BIGINT) AS counterparty_key,
       counterparty_id,
       starts_with(counterparty_id, 'ACC-')       AS is_internal,
       MIN(booking_date)                          AS first_seen_date,
       MAX(booking_date)                          AS last_seen_date,
       COUNT(*)                                   AS transaction_count
FROM stg_curated
WHERE counterparty_id IS NOT NULL
GROUP BY counterparty_id
UNION ALL
SELECT -1, 'UNKNOWN', false, NULL, NULL,
       (SELECT COUNT(*) FROM stg_curated WHERE counterparty_id IS NULL);
