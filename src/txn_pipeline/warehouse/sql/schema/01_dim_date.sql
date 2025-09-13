-- dim_date: smart key YYYYMMDD (no lookup needed to build fact rows).
CREATE OR REPLACE TABLE dim_date AS
WITH bounds AS (
    SELECT LEAST(MIN(booking_date), MIN(value_date)) AS min_d,
           GREATEST(MAX(booking_date), MAX(value_date)) AS max_d
    FROM stg_curated
),
days AS (
    SELECT CAST(d AS DATE) AS full_date
    FROM bounds, generate_series(CAST(min_d AS TIMESTAMP), CAST(max_d AS TIMESTAMP), INTERVAL 1 DAY) AS t(d)
)
SELECT CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
       full_date,
       CAST(year(full_date) AS INTEGER)              AS year,
       CAST(quarter(full_date) AS TINYINT)           AS quarter,
       CAST(month(full_date) AS TINYINT)             AS month,
       strftime(full_date, '%B')                     AS month_name,
       CAST(day(full_date) AS TINYINT)               AS day,
       CAST(isodow(full_date) AS TINYINT)            AS iso_day_of_week,
       strftime(full_date, '%A')                     AS day_name,
       CAST(weekofyear(full_date) AS TINYINT)        AS iso_week,
       isodow(full_date) >= 6                        AS is_weekend
FROM days
ORDER BY full_date;
