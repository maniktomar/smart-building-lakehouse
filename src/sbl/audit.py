"""Audit log: one row in ops.audit_runs per step and table, whether it succeeded or failed."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from sbl.lakehouse import Lakehouse

AUDIT_TABLE = "audit_runs"

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("step", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("ended_at", TimestampType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("status", StringType(), False),
    StructField("message", StringType(), True),
])


def new_run_id() -> str:
    """Batch id for one pipeline run, e.g. 20261021T010203Z-1a2b3c."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def utc_now() -> datetime:
    # Spark TimestampType stores UTC; a naive UTC datetime avoids any local-time conversion.
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass
class AuditRecord:
    run_id: str
    step: str
    target_table: str
    started_at: datetime = field(default_factory=utc_now)
    ended_at: datetime | None = None
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    status: str = "RUNNING"
    message: str | None = None


@contextmanager
def audited(lh: Lakehouse, run_id: str, step: str, target_table: str) -> Iterator[AuditRecord]:
    """Record a step in ops.audit_runs. Exceptions are logged as FAILED and then re-raised,
    so the job still fails loudly."""
    rec = AuditRecord(run_id, step, target_table)
    try:
        yield rec
        rec.status = "SUCCESS"
    except Exception as exc:
        rec.status = "FAILED"
        rec.message = f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        rec.ended_at = utc_now()
        row = [(rec.run_id, rec.step, rec.target_table, rec.started_at, rec.ended_at,
                rec.rows_read, rec.rows_written, rec.rows_quarantined, rec.status, rec.message)]
        lh.write(lh.spark.createDataFrame(row, AUDIT_SCHEMA), "ops", AUDIT_TABLE)
        print(f"[audit] {rec.step:<10} {rec.target_table:<28} {rec.status:<7} "
              f"read={rec.rows_read} written={rec.rows_written} "
              f"quarantined={rec.rows_quarantined} {rec.message or ''}")
