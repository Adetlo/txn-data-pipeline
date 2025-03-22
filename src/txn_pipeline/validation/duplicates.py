"""Duplicate and conflict detection (docs/DATA_QUALITY.md, "Duplicates").

Within a batch, records are grouped by ``transaction_id``:

* identical raw rows                      -> ``EXACT_DUPLICATE`` (first kept, rest dropped)
* same id, same canonical payload hash     -> ``IDEMPOTENT_DUPLICATE`` (first kept, rest dropped)
* same id, different payload hashes        -> **conflict**: *every* record with that id is
  quarantined under ``DQ006`` because the pipeline cannot know which version is right.

Across batches, the metadata ``transaction_registry`` (transaction_id -> payload hash of
the curated version) is consulted:

* already curated with the same hash       -> ``CROSS_BATCH_DUPLICATE`` (dropped)
* already curated with a different hash    -> conflict (only the new record is quarantined;
  the previously curated row stands).

Registry entries written by the *same* batch (a restart) are ignored so that re-running a
failed batch re-emits its own rows instead of treating them as duplicates.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import DuplicateKind, Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.validation.results import (
    DuplicateRecord,
    Finding,
    Rejection,
    RuleCounts,
    ValidatedRecord,
)


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    payload_hash: str
    batch_id: str


@dataclass(slots=True)
class DedupResult:
    accepted: list[ValidatedRecord] = field(default_factory=list)
    duplicates: list[DuplicateRecord] = field(default_factory=list)
    conflicts: list[Rejection] = field(default_factory=list)
    counts: RuleCounts = field(default_factory=RuleCounts)
    payload_hashes: dict[int, str] = field(default_factory=dict)  # record_number -> hash

    @property
    def exact_duplicate_count(self) -> int:
        return sum(1 for d in self.duplicates if d.kind is DuplicateKind.EXACT_DUPLICATE)

    @property
    def idempotent_duplicate_count(self) -> int:
        return sum(1 for d in self.duplicates if d.kind is DuplicateKind.IDEMPOTENT_DUPLICATE)

    @property
    def cross_batch_duplicate_count(self) -> int:
        return sum(1 for d in self.duplicates if d.kind is DuplicateKind.CROSS_BATCH_DUPLICATE)


def differing_fields(a: TransactionRecord, b: TransactionRecord) -> list[str]:
    pa, pb = a.business_payload(), b.business_payload()
    return sorted(k for k in pa if pa[k] != pb.get(k))


def _conflict(item: ValidatedRecord, detail: str, other: str) -> Rejection:
    finding = Finding(
        rules.DQ006_DUPLICATE_TRANSACTION,
        Severity.ERROR,
        f"transaction_id {item.transaction_id} conflicts with {other}: {detail}",
        "transaction_id",
    )
    return Rejection(
        record_number=item.record_number,
        raw=item.raw,
        rule_id=finding.rule_id,
        reason=f"{finding.rule_id}: {finding.message}",
        stage=Stage.DEDUPLICATION,
        findings=(finding,),
        transaction_id=item.transaction_id,
    )


def deduplicate(
    records: Iterable[ValidatedRecord],
    *,
    registry: Mapping[str, RegistryEntry] | None = None,
    current_batch_id: str | None = None,
) -> DedupResult:
    """Classify records into accepted / duplicates / conflicts. Order of first sight is kept."""
    result = DedupResult()
    registry = registry or {}
    groups: dict[str, list[ValidatedRecord]] = defaultdict(list)
    ordered_ids: list[str] = []
    for item in records:
        if item.transaction_id not in groups:
            ordered_ids.append(item.transaction_id)
        groups[item.transaction_id].append(item)
        result.payload_hashes[item.record_number] = item.record.payload_hash()

    for tx_id in ordered_ids:
        group = groups[tx_id]
        first = group[0]
        first_hash = result.payload_hashes[first.record_number]
        hashes = {result.payload_hashes[g.record_number] for g in group}

        if len(hashes) > 1:
            # Intra-batch conflict: quarantine every version.
            for item in group:
                own_hash = result.payload_hashes[item.record_number]
                others = [g for g in group if result.payload_hashes[g.record_number] != own_hash]
                diff = differing_fields(item.record, others[0].record)
                result.conflicts.append(
                    _conflict(
                        item,
                        f"differing fields {diff}",
                        f"record #{others[0].record_number} in the same batch",
                    )
                )
                result.counts.add(rules.DQ006_DUPLICATE_TRANSACTION)
            continue

        # Cross-batch check applies to the surviving representative only.
        known = registry.get(tx_id)
        if known is not None and known.batch_id != current_batch_id:
            if known.payload_hash == first_hash:
                for item in group:
                    result.duplicates.append(
                        DuplicateRecord(
                            record_number=item.record_number,
                            transaction_id=tx_id,
                            kind=DuplicateKind.CROSS_BATCH_DUPLICATE,
                            duplicate_of_record_number=None,
                            duplicate_of_batch_id=known.batch_id,
                        )
                    )
                continue
            for item in group:
                result.conflicts.append(
                    _conflict(
                        item,
                        "payload differs from the already curated version",
                        f"batch {known.batch_id}",
                    )
                )
                result.counts.add(rules.DQ006_DUPLICATE_TRANSACTION)
            continue

        result.accepted.append(first)
        for item in group[1:]:
            kind = (
                DuplicateKind.EXACT_DUPLICATE
                if item.raw_hash == first.raw_hash
                else DuplicateKind.IDEMPOTENT_DUPLICATE
            )
            result.duplicates.append(
                DuplicateRecord(
                    record_number=item.record_number,
                    transaction_id=tx_id,
                    kind=kind,
                    duplicate_of_record_number=first.record_number,
                    duplicate_of_batch_id=current_batch_id,
                )
            )
    return result
