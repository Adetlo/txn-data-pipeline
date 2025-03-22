from __future__ import annotations

from tests.conftest import valid_row
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import DuplicateKind, Stage
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.duplicates import RegistryEntry, deduplicate, differing_fields
from txn_pipeline.validation.results import ValidatedRecord


def _item(n: int, **overrides: object) -> ValidatedRecord:
    data = valid_row(**overrides)
    return ValidatedRecord(n, data, sha256_record(data), TransactionRecord.model_validate(data))


def test_unique_records_pass_through() -> None:
    result = deduplicate([_item(1), _item(2, transaction_id="TXN-2")])
    assert [a.record_number for a in result.accepted] == [1, 2]
    assert result.duplicates == [] and result.conflicts == []
    assert set(result.payload_hashes) == {1, 2}


def test_exact_duplicate_keeps_first() -> None:
    result = deduplicate([_item(1), _item(2), _item(3)])
    assert [a.record_number for a in result.accepted] == [1]
    assert [(d.record_number, d.kind, d.duplicate_of_record_number) for d in result.duplicates] == [
        (2, DuplicateKind.EXACT_DUPLICATE, 1),
        (3, DuplicateKind.EXACT_DUPLICATE, 1),
    ]
    assert result.exact_duplicate_count == 2
    assert result.counts.as_dict() == {}


def test_idempotent_duplicate_same_payload_different_raw() -> None:
    result = deduplicate([_item(1), _item(2, currency="gbp", reference=" DEMO-TRANSFER-001 ")])
    assert len(result.accepted) == 1
    assert result.duplicates[0].kind is DuplicateKind.IDEMPOTENT_DUPLICATE
    assert result.idempotent_duplicate_count == 1


def test_intra_batch_conflict_quarantines_every_version() -> None:
    result = deduplicate([_item(1), _item(2, amount="250.01"), _item(3)])
    assert result.accepted == [] and result.duplicates == []
    assert [c.record_number for c in result.conflicts] == [1, 2, 3]
    for c in result.conflicts:
        assert c.rule_id == rules.DQ006_DUPLICATE_TRANSACTION
        assert c.stage is Stage.DEDUPLICATION
        assert "amount" in c.reason
        assert c.transaction_id == "TXN-SYN-20261013-000001"
    assert result.counts.as_dict() == {rules.DQ006_DUPLICATE_TRANSACTION: 3}


def test_cross_batch_duplicate_is_dropped() -> None:
    first = _item(1)
    registry = {first.transaction_id: RegistryEntry(first.record.payload_hash(), "batch-A")}
    result = deduplicate([first, _item(2)], registry=registry, current_batch_id="batch-B")
    assert result.accepted == []
    assert all(d.kind is DuplicateKind.CROSS_BATCH_DUPLICATE for d in result.duplicates)
    assert [d.duplicate_of_batch_id for d in result.duplicates] == ["batch-A", "batch-A"]
    assert result.cross_batch_duplicate_count == 2


def test_cross_batch_conflict_quarantines_new_record_only() -> None:
    registry = {"TXN-SYN-20261013-000001": RegistryEntry("0" * 64, "batch-A")}
    result = deduplicate([_item(1)], registry=registry, current_batch_id="batch-B")
    assert result.accepted == []
    assert len(result.conflicts) == 1
    assert "batch batch-A" in result.conflicts[0].reason


def test_same_batch_registry_rows_are_ignored_on_restart() -> None:
    item = _item(1)
    registry = {item.transaction_id: RegistryEntry("0" * 64, "batch-B")}
    result = deduplicate([item], registry=registry, current_batch_id="batch-B")
    assert [a.record_number for a in result.accepted] == [1]


def test_differing_fields() -> None:
    a = TransactionRecord.model_validate(valid_row())
    b = TransactionRecord.model_validate(valid_row(amount="1.00", status="PENDING"))
    assert differing_fields(a, b) == ["amount", "status"]
    assert differing_fields(a, a) == []
