"""Run the full migration under load and print what happened."""
from __future__ import annotations

import argparse
import sys

from migration.load import run_migration


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="RebuildOnlineMigrations")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--step-seconds", type=float, default=3.0)
    parser.add_argument("--seed-customers", type=int, default=250, help="customers each worker creates first")
    args = parser.parse_args()

    report = run_migration(args.database, args.workers, args.step_seconds, args.seed_customers)
    c = report.counts
    print()
    print(f"Requests:  {c.get('creates', 0)} creates, {c.get('adds', 0)} adds, "
          f"{c.get('cancels', 0)} cancels, {c.get('reads', 0)} reads")
    print(f"Errors: {c.get('errors', 0)}   Wrong reads: {c.get('wrong_reads', 0)}   "
          f"Deadlock retries: {c.get('deadlock_retries', 0)}   Lazy copies: {c.get('lazy_copies', 0)}")
    print(f"Experiment: {c.get('experiment_matches', 0)} matches, {c.get('experiment_mismatches', 0)} mismatches")
    print(f"End state: {report.customers} customers, {report.subscriptions} subscriptions, "
          f"{report.lost_or_wrong} lost or wrong")
    for sample in report.error_samples:
        print(f"  {sample}")
    print("PASS" if report.ok else "FAIL")
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
