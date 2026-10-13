# Examples

| File | Purpose |
|------|---------|
| `transaction_record.json` | One canonical input record (`financial-transaction/1`), the shape every reader maps to |
| `corebank_ledger_export.jsonl` | Five journal entries shaped like a [corebank-ledger](https://github.com/Adetlo/corebank-ledger) demo export, for the `--adapter corebank-ledger` option (one entry has an unsupported currency and is quarantined on purpose) |
| `reconciliation_report.json` | A real reconciliation report produced by the demo run |
| `commands.md` | Common CLI invocations |

```bash
# validate the corebank-shaped export without registering a batch
uv run txn-pipeline validate --input examples/corebank_ledger_export.jsonl --adapter corebank-ledger

# run it as a batch under its own source name
uv run txn-pipeline run --input examples/corebank_ledger_export.jsonl --adapter corebank-ledger --source COREBANK_LEDGER_DEMO
```
