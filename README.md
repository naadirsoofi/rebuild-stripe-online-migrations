# rebuild-stripe-online-migrations

A small rebuild of Stripe's [Online migrations at scale](https://stripe.com/blog/online-migrations) (Jacqueline Xu, 2017), on SQL Server.

Stripe moved hundreds of millions of subscriptions out of an array on the Customer object and into their own table, without taking anything down. They did it in four phases. This repo does the same move on a laptop: subscriptions start as a JSON array inside each `Customers` row and end up in a `Subscriptions` table, while eight threads keep creating customers, adding and canceling subscriptions, and reading them the whole time.

## The phases

| Phase | Writes go to | Reads come from | What happens |
|---|---|---|---|
| 0. Before | old | old | JSON array on the customer row |
| 1. Dual write | old, plus new for a growing share of customers (10%, 50%, 100%) | old | First dual write copies that customer over (lazy copy). Then a batched backfill copies everyone else. Then a read experiment compares old and new on every read and keeps the old answer. |
| 2. Read new | old and new | new | Only allowed once nobody is left to copy, the stores match row for row, and the experiment saw zero mismatches. |
| 3. Write new | new first, old kept as an archive | new | Reading the old column now raises a hard error, like Stripe's `Opus::Error.hard`. |
| 4. Remove old | new | new | Every read empties that customer's old JSON, and a batched job empties the rest. |
| 5. Done | new | new | The old column is dropped, with the load still running. |

## Results from one run

8 worker threads, 3 seconds per step, about 35 seconds total, on SQL Server 2022 Developer:

```
Requests:  23771 creates, 79515 adds, 23390 cancels, 87039 reads
Errors: 0   Wrong reads: 0   Deadlock retries: 0   Lazy copies: 6204
Experiment: 7812 matches, 0 mismatches
End state: 23771 customers, 79515 subscriptions, 0 lost or wrong
```

"Wrong reads" means a read that didn't match what that worker knew it had written. Each worker owns its customers and keeps its own record, so every read in every phase gets checked exactly, and the final table is compared against those records too.

## How it maps to SQL Server

- **The old store is a JSON column.** `OPENJSON` turns it into rows (`dbo.OldSubscriptionRows`), `JSON_MODIFY` appends to it, and `FOR JSON PATH` rebuilds it on a cancel. No read-modify-write in Python.
- **The phase switch is a lock.** Every request reads `dbo.MigrationState` with `REPEATABLEREAD`, which holds a shared lock until it commits. Changing the phase needs an exclusive lock, so it waits for requests already in flight. No request ever runs half in one phase and half in the next.
- **Copying is per customer and locked.** The lazy copy and the backfill both take `UPDLOCK` on the customer row first, so a write and the backfill never copy the same customer at the same time.
- **Verification uses a snapshot.** Stripe ran their checks on database snapshots in Hadoop. Here the "do the stores match" check runs under `SNAPSHOT` isolation (see below for why that matters).
- **Dropping the column is metadata only,** so phase 5 doesn't need a maintenance window.

## What I learned

- **My first consistency check lied.** Under load, the check that the two stores match kept finding differences, even though every read and the final tally were correct. Under `READ COMMITTED`, the scan could pass a customer, then see that customer's dual write land in the new table, and report a row that only existed on one side. Running the check under `SNAPSHOT` isolation fixed it: one point in time, no false alarms, and no blocking the writers.
- **The ramp has to be by customer, not by request.** If a random 10% of requests dual write, a customer can be copied and then miss later writes. Bucketing by `CustomerId % 100` means once a customer is in, they stay in as the percent goes up.
- **The backfill has to wait for 100% dual writes.** Otherwise a customer gets copied and then drifts. The controller refuses to backfill before that.

## Where this is easier than Stripe's

Both stores live in one database, so a dual write is one transaction and can't half succeed. With two separate stores you need something like an outbox table or change data capture to keep them in step, and the comparison and backfill matter a lot more. At real scale I'd also run the backfill off a replica or snapshot, sample the read experiment instead of running it on every read, and throttle the batches against replica lag.

## Run it

Needs Python 3.11+, the ODBC Driver 18 for SQL Server, and SQL Server 2016 or later.

```
pip install -e .[test]
python demo.py
python -m pytest
```

It connects to `localhost` with Windows authentication by default. To use another server, set `REBUILD_SQL_CONN` to an ODBC connection string without a database, for example:

```
REBUILD_SQL_CONN="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;PWD=<your password>;TrustServerCertificate=yes"
```

`demo.py` creates (and replaces) a database called `RebuildOnlineMigrations`, and the tests use `RebuildOnlineMigrations_Test`.

## Layout

- `sql/schema.sql`: tables, the JSON function, the differences view, the backfill and cleanup procedures
- `migration/store.py`: every read and write, following the current phase
- `migration/experiment.py`: a tiny version of GitHub's Scientist
- `migration/controller.py`: the operator's steps, each with its own checks
- `migration/load.py`: the worker threads and the full run
- `tests/`: one test per behavior, plus the full run under load
