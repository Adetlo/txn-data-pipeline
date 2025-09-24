-- Daily transaction value by currency (exact DECIMAL sums; never mixed across currencies)
SELECT d.full_date AS booking_date, c.currency_code, COUNT(*) AS transactions, SUM(f.amount) AS total_amount
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.booking_date_key
JOIN dim_currency c ON c.currency_key = f.currency_key
GROUP BY d.full_date, c.currency_code
ORDER BY d.full_date, c.currency_code;
