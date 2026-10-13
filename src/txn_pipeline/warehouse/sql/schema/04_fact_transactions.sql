-- Grain: one accepted, canonical financial transaction (transaction_id is unique).
CREATE OR REPLACE TABLE fact_transactions AS
SELECT CAST(hash(c.transaction_id) >> 1 AS BIGINT)            AS transaction_key,
       c.transaction_id,                                        -- degenerate dimension
       CAST(strftime(c.booking_date, '%Y%m%d') AS INTEGER)     AS booking_date_key,
       CAST(strftime(c.value_date, '%Y%m%d') AS INTEGER)       AS value_date_key,
       cur.currency_key,
       ch.channel_key,
       tt.transaction_type_key,
       CAST(hash(c.account_id) >> 1 AS BIGINT)                  AS account_key,
       CASE WHEN c.counterparty_id IS NULL THEN -1
            ELSE CAST(hash(c.counterparty_id) >> 1 AS BIGINT) END       AS counterparty_key,
       c.amount,                                                -- DECIMAL(18,4)
       c.amount_minor,                                          -- exact minor units
       c.status,
       c.direction,
       c.amount_band,
       c.country,
       c.country_region,
       c.beneficiary_country,
       c.merchant_category,
       c.is_reversal,
       c.original_transaction_id,
       c.reference,                                             -- degenerate dimension
       c.transaction_timestamp,
       c.dq_warning_count,
       c.batch_id,                                              -- lineage
       c.source_file_hash,
       c.pipeline_version
FROM stg_curated c
JOIN dim_currency cur         ON cur.currency_code = c.currency
JOIN dim_channel ch           ON ch.channel_code = c.channel
JOIN dim_transaction_type tt  ON tt.transaction_type_code = c.transaction_type;
