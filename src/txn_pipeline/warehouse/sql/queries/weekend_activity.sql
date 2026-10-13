-- Weekend vs weekday activity by currency
SELECT c.currency_code, d.is_weekend, COUNT(*) AS transactions, SUM(f.amount) AS total_amount
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.booking_date_key
JOIN dim_currency c ON c.currency_key = f.currency_key
GROUP BY c.currency_code, d.is_weekend
ORDER BY c.currency_code, d.is_weekend;
