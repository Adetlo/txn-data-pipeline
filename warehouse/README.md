# warehouse/

The DuckDB star schema is built by `txn-pipeline warehouse-build` (and by every
`txn-pipeline run` unless `--no-warehouse` is given). The SQL lives **inside the
package** so it ships with the wheel and the container:

* build DDL, executed in file-name order:
  `src/txn_pipeline/warehouse/sql/schema/*.sql`
  (`00_staging`, `01_dim_date`, `02_static_dimensions`, `03_dim_account`,
  `04_fact_transactions`, `05_operational_facts`);
* bundled analytical queries: `src/txn_pipeline/warehouse/sql/queries/*.sql`,
  listed by `txn-pipeline query`.

The model, keys and rebuild semantics are described in
[docs/WAREHOUSE_MODEL.md](../docs/WAREHOUSE_MODEL.md).

## Running queries

With the CLI:

```bash
uv run txn-pipeline query daily_value_by_currency --limit 30
uv run txn-pipeline query rejected_trend --json
```

With the DuckDB CLI (open read-only while no pipeline run is writing):

```bash
duckdb -readonly data/warehouse/txn_warehouse.duckdb \
  < src/txn_pipeline/warehouse/sql/queries/top_transaction_types.sql
```

With Python:

```python
import duckdb

con = duckdb.connect("data/warehouse/txn_warehouse.duckdb", read_only=True)
con.sql(open("src/txn_pipeline/warehouse/sql/queries/weekend_activity.sql").read()).show()
```

## Ad-hoc examples

```sql
-- Largest internal transfers by account
SELECT a.account_id, cp.counterparty_id, f.amount, c.currency_code, f.transaction_id
FROM fact_transactions f
JOIN dim_account a USING (account_key)
JOIN dim_counterparty cp USING (counterparty_key)
JOIN dim_currency c USING (currency_key)
WHERE f.direction = 'INTERNAL'
ORDER BY f.amount_minor DESC
LIMIT 10;
```

```sql
-- Accepted rows that carry a WARNING finding, per rule and month
SELECT d.year, d.month, f.dq_warning_count, COUNT(*) AS rows
FROM fact_transactions f JOIN dim_date d ON d.date_key = f.booking_date_key
WHERE f.dq_warning_count > 0
GROUP BY ALL ORDER BY 1, 2;
```

```sql
-- Quarantine rows joined to the batch that rejected them
SELECT q.rule_id, b.source_filename, COUNT(*) AS rejected
FROM fact_quarantine q JOIN fact_batch_quality b USING (batch_id)
GROUP BY ALL ORDER BY rejected DESC;
```
