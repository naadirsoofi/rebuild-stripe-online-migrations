"""Runs the whole migration while worker threads keep reading and writing.

Each worker owns its own customers and keeps its own record of what their subscriptions should be,
so every read in every phase can be checked exactly, and nothing can go missing unnoticed.
"""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from migration import controller
from migration.db import connect, create_database
from migration.store import Stats, Subscription, SubscriptionStore, same_subscriptions

PLANS = ("basic", "pro", "team")


class Worker(threading.Thread):
    def __init__(self, database: str, stats: Stats, stop: threading.Event, seed_customers: int, seed: int) -> None:
        super().__init__(daemon=True)
        self.database = database
        self.stats = stats
        self.stop = stop
        self.seed_customers = seed_customers
        self.rng = random.Random(seed)
        self.seeded = threading.Event()
        self.ledger: dict[int, dict[str, Subscription]] = {}
        self.error_samples: list[str] = []

    def run(self) -> None:
        store = SubscriptionStore(self.database, self.stats)
        try:
            for _ in range(self.seed_customers):
                customer_id = self._create(store)
                for _ in range(self.rng.randint(0, 3)):
                    self._add(store, customer_id)
            self.seeded.set()
            while not self.stop.is_set():
                self._step(store)
        finally:
            self.seeded.set()
            store.close()

    def _step(self, store: SubscriptionStore) -> None:
        roll = self.rng.random()
        customer_id = self.rng.choice(list(self.ledger))
        try:
            if roll < 0.10:
                self._create(store)
            elif roll < 0.45:
                self._add(store, customer_id)
            elif roll < 0.60:
                active = [s for s in self.ledger[customer_id].values() if s.status == "active"]
                if active:
                    sub = self.rng.choice(active)
                    store.cancel_subscription(customer_id, sub.id)
                    self.ledger[customer_id][sub.id] = Subscription(sub.id, sub.plan, "canceled")
                    self.stats.add("cancels")
            else:
                got = store.list_subscriptions(customer_id)
                self.stats.add("reads")
                if not same_subscriptions(got, list(self.ledger[customer_id].values())):
                    self.stats.add("wrong_reads")
        except Exception as exc:  # noqa: BLE001 - count it, keep the load going
            self.stats.add("errors")
            if len(self.error_samples) < 5:
                self.error_samples.append(f"{type(exc).__name__}: {exc}")

    def _create(self, store: SubscriptionStore) -> int:
        customer_id = store.create_customer(f"customer{self.rng.randint(1, 10**9)}@example.com")
        self.ledger[customer_id] = {}
        self.stats.add("creates")
        return customer_id

    def _add(self, store: SubscriptionStore, customer_id: int) -> None:
        plan = self.rng.choice(PLANS)
        sub_id = store.add_subscription(customer_id, plan)
        self.ledger[customer_id][sub_id] = Subscription(sub_id, plan, "active")
        self.stats.add("adds")


@dataclass
class Report:
    steps: list[tuple[str, float, str]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    customers: int = 0
    subscriptions: int = 0
    lost_or_wrong: int = 0
    error_samples: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.lost_or_wrong == 0
            and self.counts.get("errors", 0) == 0
            and self.counts.get("wrong_reads", 0) == 0
            and self.counts.get("experiment_mismatches", 0) == 0
        )


def run_migration(
    database: str,
    workers: int = 8,
    step_seconds: float = 3.0,
    seed_customers: int = 250,
    batch_size: int = 200,
    log: Callable[[str], None] = print,
) -> Report:
    create_database(database)
    stats, stop, report = Stats(), threading.Event(), Report()
    threads = [Worker(database, stats, stop, seed_customers, seed=i) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.seeded.wait()

    def step(name: str, action: Callable[[], object] | None = None, hold: bool = True) -> None:
        started = time.perf_counter()
        detail = action() if action else None
        if hold:
            time.sleep(step_seconds)
        elapsed = time.perf_counter() - started
        note = "" if detail is None else str(detail)
        report.steps.append((name, elapsed, note))
        log(f"{name:<44} {elapsed:6.2f}s  {note}")

    try:
        step("Phase 0: old store only")
        for percent in (10, 50, 100):
            step(f"Phase 1: dual writes for {percent}% of customers",
                 lambda p=percent: controller.start_dual_writes(database, p))
        step("Backfill the rest", lambda: controller.backfill(database, batch_size), hold=False)
        step("Check: customers not copied", lambda: controller.unmigrated_customers(database), hold=False)
        step("Check: rows that differ between stores", lambda: controller.store_differences(database), hold=False)
        step("Read experiment (old vs new, keep old)", lambda: controller.start_experiment(database))
        step("Phase 2: read from the new store", lambda: controller.start_reading_new(database))
        step("Phase 3: write new first, old as archive", lambda: controller.start_writing_new(database))
        step("Phase 4: stop writing the old store", lambda: controller.start_removing_old(database))
        step("Empty the old column", lambda: controller.empty_old_store(database, batch_size), hold=False)
        step("Phase 5: drop the old column", lambda: controller.finish(database))
    finally:
        stop.set()
        for t in threads:
            t.join()

    expected: dict[str, Subscription] = {}
    for t in threads:
        for subs in t.ledger.values():
            expected.update(subs)
        report.error_samples.extend(t.error_samples)

    conn = connect(database, autocommit=True)
    try:
        rows = conn.cursor().execute(
            "SELECT LOWER(CONVERT(VARCHAR(36), SubscriptionId)), PlanCode, Status FROM dbo.Subscriptions"
        ).fetchall()
        report.customers = conn.cursor().execute("SELECT COUNT(*) FROM dbo.Customers").fetchval()
    finally:
        conn.close()
    actual = {row[0]: Subscription(*row) for row in rows}
    report.subscriptions = len(actual)
    report.lost_or_wrong = sum(1 for k in expected.keys() | actual.keys() if expected.get(k) != actual.get(k))
    report.counts = stats.snapshot()
    return report
