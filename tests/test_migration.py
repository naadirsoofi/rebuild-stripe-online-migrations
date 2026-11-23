from __future__ import annotations

from typing import Iterator

import pytest

from migration import controller
from migration.db import connect, create_database
from migration.load import run_migration
from migration.store import (
    BEFORE,
    READ_NEW,
    WRITE_NEW,
    MigrationState,
    OldReadPathRetired,
    Subscription,
    SubscriptionNotFound,
    SubscriptionStore,
)

TEST_DATABASE = "RebuildOnlineMigrations_Test"


@pytest.fixture
def db() -> str:
    create_database(TEST_DATABASE)
    return TEST_DATABASE


@pytest.fixture
def store(db: str) -> Iterator[SubscriptionStore]:
    s = SubscriptionStore(db)
    yield s
    s.close()


def query(db: str, sql: str, *params: object) -> list[tuple]:
    conn = connect(db, autocommit=True)
    try:
        return [tuple(r) for r in conn.cursor().execute(sql, *params).fetchall()]
    finally:
        conn.close()


def execute(db: str, sql: str) -> None:
    conn = connect(db, autocommit=True)
    try:
        conn.cursor().execute(sql)
    finally:
        conn.close()


def old_ids(db: str, customer_id: int) -> set[str]:
    return {r[0] for r in query(
        db,
        "SELECT LOWER(CONVERT(VARCHAR(36), s.SubscriptionId)) FROM dbo.Customers AS c "
        "CROSS APPLY dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s WHERE c.CustomerId = ?",
        customer_id,
    )}


def new_rows(db: str, customer_id: int) -> dict[str, str]:
    return dict(query(
        db,
        "SELECT LOWER(CONVERT(VARCHAR(36), SubscriptionId)), Status FROM dbo.Subscriptions WHERE CustomerId = ?",
        customer_id,
    ))


def test_before_the_migration_only_the_old_store_is_written(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    sub = store.add_subscription(customer, "basic")

    assert controller.get_state(db)[0] == BEFORE
    assert old_ids(db, customer) == {sub}
    assert new_rows(db, customer) == {}


def test_dual_writes_ramp_up_by_customer(db: str, store: SubscriptionStore) -> None:
    controller.start_dual_writes(db, 2)  # customer ids ending in 00 or 01 are in, so 1 is in and 2 is out
    inside = store.create_customer("in@example.com")
    outside = store.create_customer("out@example.com")
    assert (inside, outside) == (1, 2)

    in_sub = store.add_subscription(inside, "pro")
    out_sub = store.add_subscription(outside, "pro")

    assert new_rows(db, inside) == {in_sub: "active"}
    assert new_rows(db, outside) == {}
    assert old_ids(db, inside) == {in_sub} and old_ids(db, outside) == {out_sub}


def test_first_dual_write_copies_the_customer_lazily(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    first = store.add_subscription(customer, "basic")
    controller.start_dual_writes(db, 100)

    second = store.add_subscription(customer, "team")

    assert new_rows(db, customer) == {first: "active", second: "active"}
    assert store.stats.get("lazy_copies") == 1


def test_cancel_updates_both_stores(db: str, store: SubscriptionStore) -> None:
    controller.start_dual_writes(db, 100)
    customer = store.create_customer("a@example.com")
    sub = store.add_subscription(customer, "basic")

    store.cancel_subscription(customer, sub)

    assert new_rows(db, customer) == {sub: "canceled"}
    assert store.list_subscriptions(customer) == [Subscription(sub, "basic", "canceled")]
    with pytest.raises(SubscriptionNotFound):
        store.cancel_subscription(customer, sub)


def test_backfill_copies_everything_once(db: str, store: SubscriptionStore) -> None:
    customers = [store.create_customer(f"{i}@example.com") for i in range(5)]
    subs = [store.add_subscription(c, "basic") for c in customers for _ in range(2)]

    with pytest.raises(controller.MigrationNotReady):
        controller.backfill(db)  # dual writes aren't at 100% yet

    controller.start_dual_writes(db, 100)
    result = controller.backfill(db, batch_size=2)

    assert result == controller.BatchResult(batches=3, rows=len(subs))
    assert controller.unmigrated_customers(db) == 0
    assert controller.store_differences(db) == 0
    assert controller.backfill(db, batch_size=2) == controller.BatchResult(0, 0)


def test_reads_cannot_move_until_the_stores_agree(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    store.add_subscription(customer, "basic")
    controller.start_dual_writes(db, 100)

    with pytest.raises(controller.MigrationNotReady):
        controller.start_reading_new(db)

    controller.backfill(db)
    controller.start_reading_new(db)
    assert controller.get_state(db)[0] == READ_NEW


def test_experiment_keeps_the_old_answer_and_records_a_mismatch(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    sub = store.add_subscription(customer, "basic")
    controller.start_dual_writes(db, 100)
    controller.backfill(db)
    controller.start_experiment(db)
    execute(db, "UPDATE dbo.Subscriptions SET Status = 'canceled'")

    assert store.list_subscriptions(customer) == [Subscription(sub, "basic", "active")]
    assert controller.experiment_mismatches(db) == 1
    with pytest.raises(controller.MigrationNotReady):
        controller.start_reading_new(db)


def test_experiment_candidate_failure_does_not_break_the_read(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    sub = store.add_subscription(customer, "basic")
    controller.start_dual_writes(db, 100)
    controller.backfill(db)
    controller.start_experiment(db)

    def broken(*_: object) -> list[Subscription]:
        raise RuntimeError("new store is down")

    store._read_new = broken  # type: ignore[method-assign]

    assert store.list_subscriptions(customer) == [Subscription(sub, "basic", "active")]
    assert store.stats.get("experiment_errors") == 1


def test_old_read_path_is_a_hard_error_once_new_is_the_source_of_truth(db: str, store: SubscriptionStore) -> None:
    customer = store.create_customer("a@example.com")
    sub = store.add_subscription(customer, "basic")
    controller.start_dual_writes(db, 100)
    controller.backfill(db)
    controller.start_reading_new(db)
    controller.start_writing_new(db)

    conn = connect(db)
    try:
        with pytest.raises(OldReadPathRetired):
            SubscriptionStore._read_old(conn.cursor(), MigrationState(WRITE_NEW, 100, False), customer)
    finally:
        conn.close()

    store.cancel_subscription(customer, sub)  # validated against the new store, mirrored to the old
    assert new_rows(db, customer) == {sub: "canceled"}
    assert controller.store_differences(db) == 0


def test_old_column_is_emptied_then_dropped(db: str, store: SubscriptionStore) -> None:
    customers = [store.create_customer(f"{i}@example.com") for i in range(3)]
    for c in customers:
        store.add_subscription(c, "pro")
    controller.start_dual_writes(db, 100)
    controller.backfill(db)
    controller.start_reading_new(db)
    controller.start_writing_new(db)
    controller.start_removing_old(db)

    store.list_subscriptions(customers[0])  # lazily empties this customer's old data
    assert controller.old_rows_left(db) == 2
    with pytest.raises(controller.MigrationNotReady):
        controller.finish(db)

    assert controller.empty_old_store(db, batch_size=1) == controller.BatchResult(2, 2)
    controller.finish(db)

    columns = query(db, "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Customers')")
    assert ("SubscriptionsJson",) not in columns

    new_customer = store.create_customer("after@example.com")
    sub = store.add_subscription(new_customer, "team")
    assert store.list_subscriptions(new_customer) == [Subscription(sub, "team", "active")]


def test_full_migration_under_load_loses_nothing(db: str) -> None:
    report = run_migration(db, workers=4, step_seconds=0.5, seed_customers=30, batch_size=25, log=lambda _: None)

    assert report.error_samples == []
    assert report.ok, report
    assert report.counts["reads"] > 0 and report.counts["experiment_matches"] > 0
