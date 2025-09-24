-- Channel distribution
SELECT ch.channel_code, COUNT(*) AS transactions,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM fact_transactions f JOIN dim_channel ch ON ch.channel_key = f.channel_key
GROUP BY ch.channel_code
ORDER BY transactions DESC;
