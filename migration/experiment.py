"""A tiny version of GitHub's Scientist: run the old and new code side by side, keep the old answer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class ExperimentResult(Generic[T]):
    control: T
    candidate: T | None
    matched: bool
    candidate_error: Exception | None = None


def run_experiment(
    control: Callable[[], T],
    candidate: Callable[[], T],
    same: Callable[[T, T], bool],
) -> ExperimentResult[T]:
    """Run both paths. The caller always gets the control's answer; the candidate can't break it."""
    control_value = control()
    try:
        candidate_value = candidate()
    except Exception as exc:  # noqa: BLE001 - any candidate failure is just a mismatch
        return ExperimentResult(control_value, None, False, exc)
    return ExperimentResult(control_value, candidate_value, same(control_value, candidate_value))
