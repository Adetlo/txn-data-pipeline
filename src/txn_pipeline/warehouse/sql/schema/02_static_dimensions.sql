-- Static dimensions are loaded from the contract vocabularies (registered as Arrow tables
-- by the builder) so the warehouse never invents codes the contract does not know.
CREATE OR REPLACE TABLE dim_currency AS
SELECT CAST(row_number() OVER (ORDER BY currency_code) AS INTEGER) AS currency_key,
       currency_code, currency_name, CAST(minor_unit_exponent AS TINYINT) AS minor_unit_exponent
FROM src_currency;

CREATE OR REPLACE TABLE dim_channel AS
SELECT CAST(row_number() OVER (ORDER BY channel_code) AS INTEGER) AS channel_key, channel_code
FROM src_channel;

CREATE OR REPLACE TABLE dim_transaction_type AS
SELECT CAST(row_number() OVER (ORDER BY transaction_type_code) AS INTEGER) AS transaction_type_key,
       transaction_type_code,
       CASE WHEN transaction_type_code IN ('DEPOSIT', 'REFUND', 'INTEREST') THEN 'INBOUND'
            WHEN transaction_type_code = 'TRANSFER' THEN 'INTERNAL_OR_OUTBOUND'
            ELSE 'OUTBOUND' END AS default_direction
FROM src_transaction_type;
