-- Transaction types ranked by count, with share of all accepted transactions
SELECT t.transaction_type_code, COUNT(*) AS transactions,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM fact_transactions f JOIN dim_transaction_type t ON t.transaction_type_key = f.transaction_type_key
GROUP BY t.transaction_type_code
ORDER BY transactions DESC;
