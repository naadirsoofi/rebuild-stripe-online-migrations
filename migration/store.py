"""Subscription reads and writes that follow whatever phase the migration is in."""
from __future__ import annotations

import json
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pyodbc

from migration.db import connect
from migration.experiment import run_experiment

# Phase 0 is before the migration and phase 5 is after it. Phases 1 to 4 are the four in Stripe's post.
BEFORE, DUAL_WRITE, READ_NEW, WRITE_NEW, REMOVE_OLD, DONE = range(6)

MAX_ATTEMPTS = 5
DEADLOCK_SQLSTATE = "40001"

T = TypeVar("T")


@dataclass(frozen=True)
class MigrationState:
    phase: int
    dual_write_percent: int
    experiment_on: bool

    def writes_old(self) -> bool:
        return self.phase <= WRITE_NEW

    def writes_new(self, customer_id: int) -> bool:
        if self.phase == DUAL_WRITE:
            # Ramp up by customer, so a customer never drops out of dual writing as the percent rises.
            return customer_id % 100 < self.dual_write_percent
        return self.phase >= READ_NEW

    def new_is_source_of_truth(self) -> bool:
        return self.phase >= WRITE_NEW


@dataclass(frozen=True)
class Subscription:
    id: str
    plan: str
    status: str


class OldReadPathRetired(RuntimeError):
    """Raised when anything reads the old JSON column from phase 3 on, like Stripe's hard error."""


class SubscriptionNotFound(LookupError):
    pass


class Stats:
    """Thread-safe counters shared by every store in a run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Counter[str] = Counter()

    def add(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counts[name] += n

    def get(self, name: str) -> int:
        with self._lock:
            return self._counts[name]

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


OLD_ROWS_SQL = """
SELECT LOWER(CONVERT(VARCHAR(36), s.SubscriptionId)), s.PlanCode, s.Status
FROM dbo.Customers AS c
CROSS APPLY dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s
WHERE c.CustomerId = ?
"""

NEW_ROWS_SQL = """
SELECT LOWER(CONVERT(VARCHAR(36), SubscriptionId)), PlanCode, Status
FROM dbo.Subscriptions
WHERE CustomerId = ?
"""

LAZY_COPY_SQL = """
INSERT dbo.Subscriptions (SubscriptionId, CustomerId, PlanCode, Status, CreatedAt)
SELECT s.SubscriptionId, c.CustomerId, s.PlanCode, s.Status, s.CreatedAt
FROM dbo.Customers AS c
CROSS APPLY dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s
WHERE c.CustomerId = ?
  AND NOT EXISTS (SELECT 1 FROM dbo.Subscriptions AS t WHERE t.SubscriptionId = s.SubscriptionId);

UPDATE dbo.Customers SET MigratedAt = SYSUTCDATETIME() WHERE CustomerId = ?;
"""

OLD_APPEND_SQL = """
UPDATE dbo.Customers
SET SubscriptionsJson = JSON_MODIFY(ISNULL(SubscriptionsJson, N'[]'), 'append $', JSON_QUERY(?))
WHERE CustomerId = ?
"""

OLD_CANCEL_SQL = """
UPDATE c
SET SubscriptionsJson = (
    SELECT LOWER(CONVERT(VARCHAR(36), s.SubscriptionId)) AS id,
           s.PlanCode AS [plan],
           CASE WHEN s.SubscriptionId = ? THEN 'canceled' ELSE s.Status END AS status,
           s.CreatedAt AS created_at
    FROM dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s
    FOR JSON PATH)
FROM dbo.Customers AS c
WHERE c.CustomerId = ?
"""


class SubscriptionStore:
    """One per thread: it holds a single connection."""

    def __init__(self, database: str, stats: Stats | None = None) -> None:
        self._conn = connect(database)
        self.stats = stats or Stats()

    def close(self) -> None:
        self._conn.close()

    def create_customer(self, email: str) -> int:
        def op(cur: pyodbc.Cursor, state: MigrationState) -> int:
            if state.writes_old():
                sql = "INSERT dbo.Customers (Email, SubscriptionsJson) OUTPUT INSERTED.CustomerId VALUES (?, N'[]')"
            else:
                sql = "INSERT dbo.Customers (Email) OUTPUT INSERTED.CustomerId VALUES (?)"
            customer_id = cur.execute(sql, email).fetchval()
            if state.writes_new(customer_id):
                cur.execute("UPDATE dbo.Customers SET MigratedAt = SYSUTCDATETIME() WHERE CustomerId = ?", customer_id)
            return customer_id

        return self._run(op)

    def add_subscription(self, customer_id: int, plan: str) -> str:
        sub = Subscription(str(uuid.uuid4()), plan, "active")
        created_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")
        old_json = json.dumps({**asdict(sub), "created_at": created_at})

        def op(cur: pyodbc.Cursor, state: MigrationState) -> None:
            self._prepare_write(cur, state, customer_id)
            self._write_both(
                state,
                customer_id,
                old=lambda: cur.execute(OLD_APPEND_SQL, old_json, customer_id),
                new=lambda: cur.execute(
                    "INSERT dbo.Subscriptions (SubscriptionId, CustomerId, PlanCode, Status, CreatedAt) "
                    "VALUES (?, ?, ?, ?, ?)",
                    sub.id, customer_id, sub.plan, sub.status, created_at,
                ),
            )

        self._run(op)
        return sub.id

    def cancel_subscription(self, customer_id: int, subscription_id: str) -> None:
        def op(cur: pyodbc.Cursor, state: MigrationState) -> None:
            self._prepare_write(cur, state, customer_id)
            # Whichever store is the source of truth decides whether the cancel is valid.
            if state.new_is_source_of_truth():
                current = self._read_new(cur, customer_id)
            else:
                current = self._read_old(cur, state, customer_id)
            if not any(s.id == subscription_id and s.status == "active" for s in current):
                raise SubscriptionNotFound(f"No active subscription {subscription_id} for customer {customer_id}")
            self._write_both(
                state,
                customer_id,
                old=lambda: cur.execute(OLD_CANCEL_SQL, subscription_id, customer_id),
                new=lambda: cur.execute(
                    "UPDATE dbo.Subscriptions SET Status = 'canceled' WHERE SubscriptionId = ? AND CustomerId = ?",
                    subscription_id, customer_id,
                ),
            )

        self._run(op)

    def list_subscriptions(self, customer_id: int) -> list[Subscription]:
        def op(cur: pyodbc.Cursor, state: MigrationState) -> list[Subscription]:
            if state.phase <= DUAL_WRITE:
                migrated = cur.execute(
                    "SELECT MigratedAt FROM dbo.Customers WITH (REPEATABLEREAD) WHERE CustomerId = ?", customer_id
                ).fetchval() is not None
                if not (state.experiment_on and migrated):
                    return self._read_old(cur, state, customer_id)
                result = run_experiment(
                    control=lambda: self._read_old(cur, state, customer_id),
                    candidate=lambda: self._read_new(cur, customer_id),
                    same=same_subscriptions,
                )
                self.stats.add("experiment_matches" if result.matched else "experiment_mismatches")
                if result.candidate_error is not None:
                    self.stats.add("experiment_errors")
                if not result.matched:
                    cur.execute(
                        "INSERT dbo.ExperimentMismatches (CustomerId, ControlJson, CandidateJson) VALUES (?, ?, ?)",
                        customer_id, _to_json(result.control), _to_json(result.candidate),
                    )
                return result.control

            if state.phase == REMOVE_OLD:
                # Stripe emptied the old array whenever a subscription was loaded. Same idea here.
                cur.execute(
                    "UPDATE dbo.Customers SET SubscriptionsJson = NULL "
                    "WHERE CustomerId = ? AND SubscriptionsJson IS NOT NULL",
                    customer_id,
                )
            return self._read_new(cur, customer_id)

        return self._run(op)

    def _prepare_write(self, cur: pyodbc.Cursor, state: MigrationState, customer_id: int) -> None:
        """Lock the customer row and, the first time it's dual written, copy it to the new store."""
        row = cur.execute(
            "SELECT MigratedAt FROM dbo.Customers WITH (UPDLOCK, ROWLOCK) WHERE CustomerId = ?", customer_id
        ).fetchone()
        if row is None:
            raise LookupError(f"No customer {customer_id}")
        if state.writes_new(customer_id) and row.MigratedAt is None:
            cur.execute(LAZY_COPY_SQL, customer_id, customer_id)
            self.stats.add("lazy_copies")

    @staticmethod
    def _write_both(
        state: MigrationState, customer_id: int, old: Callable[[], object], new: Callable[[], object]
    ) -> None:
        writes = []
        if state.writes_old():
            writes.append(old)
        if state.writes_new(customer_id):
            writes.append(new)
        # From phase 3 the new store is written first and the old one is kept as an archive.
        if state.new_is_source_of_truth():
            writes.reverse()
        for write in writes:
            write()

    @staticmethod
    def _read_old(cur: pyodbc.Cursor, state: MigrationState, customer_id: int) -> list[Subscription]:
        if state.new_is_source_of_truth():
            raise OldReadPathRetired("The old subscriptions column is retired; read dbo.Subscriptions")
        return [Subscription(*row) for row in cur.execute(OLD_ROWS_SQL, customer_id).fetchall()]

    @staticmethod
    def _read_new(cur: pyodbc.Cursor, customer_id: int) -> list[Subscription]:
        return [Subscription(*row) for row in cur.execute(NEW_ROWS_SQL, customer_id).fetchall()]

    def _run(self, op: Callable[[pyodbc.Cursor, MigrationState], T]) -> T:
        """Run one request in its own transaction, retrying if SQL Server picks it as a deadlock victim."""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            cur = self._conn.cursor()
            try:
                state = MigrationState(*cur.execute(
                    "SELECT Phase, DualWritePercent, ExperimentOn FROM dbo.MigrationState WITH (REPEATABLEREAD) "
                    "WHERE StateId = 1"
                ).fetchone())
                result = op(cur, state)
                self._conn.commit()
                return result
            except pyodbc.Error as exc:
                self._conn.rollback()
                if exc.args and exc.args[0] == DEADLOCK_SQLSTATE and attempt < MAX_ATTEMPTS:
                    self.stats.add("deadlock_retries")
                    time.sleep(0.01 * attempt)
                    continue
                raise
            except BaseException:
                self._conn.rollback()
                raise
            finally:
                cur.close()
        raise AssertionError("unreachable")


def same_subscriptions(a: list[Subscription], b: list[Subscription]) -> bool:
    return sorted(a, key=lambda s: s.id) == sorted(b, key=lambda s: s.id)


def _to_json(subs: list[Subscription] | None) -> str | None:
    return None if subs is None else json.dumps([asdict(s) for s in subs])
