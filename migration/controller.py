"""The operator's side: move the migration forward one step at a time, with checks before each step."""
from __future__ import annotations

from dataclasses import dataclass

from migration.db import connect
from migration.store import DONE, DUAL_WRITE, READ_NEW, REMOVE_OLD, WRITE_NEW


class MigrationNotReady(RuntimeError):
    pass


@dataclass(frozen=True)
class BatchResult:
    batches: int
    rows: int


def set_state(
    database: str,
    *,
    phase: int | None = None,
    dual_write_percent: int | None = None,
    experiment_on: bool | None = None,
) -> None:
    """Change the migration state. Waits for requests already in flight, which hold a shared lock on it."""
    conn = connect(database, autocommit=True)
    try:
        conn.cursor().execute(
            "UPDATE dbo.MigrationState SET "
            "Phase = COALESCE(?, Phase), "
            "DualWritePercent = COALESCE(?, DualWritePercent), "
            "ExperimentOn = COALESCE(?, ExperimentOn) "
            "WHERE StateId = 1",
            phase, dual_write_percent, experiment_on,
        )
    finally:
        conn.close()


def get_state(database: str) -> tuple[int, int, bool]:
    return tuple(_scalar_row(database, "SELECT Phase, DualWritePercent, ExperimentOn FROM dbo.MigrationState"))


def start_dual_writes(database: str, percent: int) -> None:
    set_state(database, phase=DUAL_WRITE, dual_write_percent=percent)


def backfill(database: str, batch_size: int = 500) -> BatchResult:
    """Copy every customer the dual writes haven't reached yet, one locked batch at a time."""
    phase, percent, _ = get_state(database)
    if phase != DUAL_WRITE or percent != 100:
        raise MigrationNotReady("Backfill only after every write is dual written (phase 1 at 100%)")

    conn = connect(database, autocommit=True)
    after = batches = rows = 0
    try:
        cur = conn.cursor()
        while True:
            last, copied = cur.execute(
                "SET NOCOUNT ON; DECLARE @Last INT, @Copied INT; "
                "EXEC dbo.BackfillBatch ?, ?, @Last OUTPUT, @Copied OUTPUT; SELECT @Last, @Copied;",
                after, batch_size,
            ).fetchone()
            if last is None:
                return BatchResult(batches, rows)
            after, batches, rows = last, batches + 1, rows + copied
    finally:
        conn.close()


def unmigrated_customers(database: str) -> int:
    return _scalar(database, "SELECT COUNT(*) FROM dbo.Customers WHERE MigratedAt IS NULL")


def store_differences(database: str) -> int:
    # Under READ COMMITTED a scan can pass a customer, then see that customer's dual write land in the
    # new table, and report a difference that never existed. A snapshot compares one point in time.
    return _scalar(
        database,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT COUNT(*) FROM dbo.StoreDifferences",
    )


def experiment_mismatches(database: str) -> int:
    return _scalar(database, "SELECT COUNT(*) FROM dbo.ExperimentMismatches")


def start_experiment(database: str) -> None:
    set_state(database, experiment_on=True)


def start_reading_new(database: str) -> None:
    """Phase 2. Only once the backfill is done and the two stores match row for row."""
    phase, percent, _ = get_state(database)
    if phase != DUAL_WRITE or percent != 100:
        raise MigrationNotReady("Reads move only after dual writes reach 100%")
    if unmigrated_customers(database):
        raise MigrationNotReady("Some customers haven't been copied yet; run the backfill")
    if store_differences(database):
        raise MigrationNotReady("The old and new stores disagree; see dbo.StoreDifferences")
    if experiment_mismatches(database):
        raise MigrationNotReady("The read experiment found mismatches; see dbo.ExperimentMismatches")
    set_state(database, phase=READ_NEW, experiment_on=False)


def start_writing_new(database: str) -> None:
    """Phase 3: the new store becomes the source of truth and the old one an archive."""
    _require_phase(database, READ_NEW)
    set_state(database, phase=WRITE_NEW)


def start_removing_old(database: str) -> None:
    """Phase 4: stop writing the old store and start emptying it."""
    _require_phase(database, WRITE_NEW)
    set_state(database, phase=REMOVE_OLD)


def empty_old_store(database: str, batch_size: int = 500) -> BatchResult:
    _require_phase(database, REMOVE_OLD)
    conn = connect(database, autocommit=True)
    batches = rows = 0
    try:
        cur = conn.cursor()
        while True:
            emptied = cur.execute(
                "SET NOCOUNT ON; DECLARE @Emptied INT; EXEC dbo.EmptyOldBatch ?, @Emptied OUTPUT; SELECT @Emptied;",
                batch_size,
            ).fetchval()
            if not emptied:
                return BatchResult(batches, rows)
            batches, rows = batches + 1, rows + emptied
    finally:
        conn.close()


def old_rows_left(database: str) -> int:
    return _scalar(database, "SELECT COUNT(*) FROM dbo.Customers WHERE SubscriptionsJson IS NOT NULL")


def finish(database: str) -> None:
    """Phase 5: nothing reads or writes the old column any more, so drop it."""
    _require_phase(database, REMOVE_OLD)
    if old_rows_left(database):
        raise MigrationNotReady("The old column still has data; run empty_old_store")
    set_state(database, phase=DONE)
    conn = connect(database, autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute("DROP VIEW dbo.StoreDifferences")
        cur.execute("ALTER TABLE dbo.Customers DROP COLUMN SubscriptionsJson")
    finally:
        conn.close()


def _require_phase(database: str, phase: int) -> None:
    current = get_state(database)[0]
    if current != phase:
        raise MigrationNotReady(f"Expected phase {phase}, found phase {current}")


def _scalar_row(database: str, sql: str) -> tuple:
    conn = connect(database, autocommit=True)
    try:
        return tuple(conn.cursor().execute(sql).fetchone())
    finally:
        conn.close()


def _scalar(database: str, sql: str) -> int:
    return _scalar_row(database, sql)[0]
