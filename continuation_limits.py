"""Explicit wall-time allowances for the unchanged finite continuation workload."""
from __future__ import annotations
import os


def allowance(name: str, default: int, maximum: int = 600) -> int:
    raw = os.environ.get(name, str(default))
    if not raw.isdecimal():
        raise ValueError(f'{name} must be an integer number of seconds')
    value = int(raw)
    if not default <= value <= maximum:
        raise ValueError(f'{name} must be between {default} and {maximum} seconds')
    return value


BATCH_SECONDS = allowance('P016_BATCH_SECONDS', 300)
SUITE_SECONDS = allowance('P016_SUITE_SECONDS', 420)
if SUITE_SECONDS <= BATCH_SECONDS:
    raise ValueError('The suite allowance must exceed the batch allowance')
