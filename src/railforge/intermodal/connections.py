from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class Move:
    id: str
    terminal: str
    arrival: datetime
    departure: datetime


@dataclass(frozen=True)
class InterchangeWindow:
    arrival_at: datetime
    unload_start: datetime
    unload_end: datetime
    load_start: datetime
    load_end: datetime
    cutoff_at: datetime
    feasible: bool
    slack: timedelta


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def window(inbound: Move, outbound: Move, unload: timedelta,
           load: timedelta, cutoff_before: timedelta) -> InterchangeWindow:
    if unload < timedelta(0) or load < timedelta(0) or cutoff_before < timedelta(0):
        raise ValueError("interchange durations cannot be negative")

    arrival = _utc(inbound.arrival)
    unload_end = arrival + unload
    load_end = unload_end + load
    cutoff = _utc(outbound.departure) - cutoff_before
    return InterchangeWindow(
        arrival_at=arrival,
        unload_start=arrival,
        unload_end=unload_end,
        load_start=unload_end,
        load_end=load_end,
        cutoff_at=cutoff,
        feasible=load_end <= cutoff,
        slack=cutoff - load_end,
    )


def feasible(inbound: Move, outbound: Move, unload: timedelta,
             load: timedelta, cutoff_before: timedelta) -> bool:
    if inbound.terminal != outbound.terminal:
        return False
    return window(inbound, outbound, unload, load, cutoff_before).feasible


def choose(inbound, candidates, unload, load, cutoff_before):
    xs = [x for x in candidates if feasible(inbound, x, unload, load, cutoff_before)]
    return min(xs, key=lambda x: (x.departure.astimezone(timezone.utc), x.id), default=None)
