-- Daily transaction volume (count of accepted transactions per booking date)
SELECT d.full_date AS booking_date, d.day_name, COUNT(*) AS transactions
FROM fact_transactions f JOIN dim_date d ON d.date_key = f.booking_date_key
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date;
