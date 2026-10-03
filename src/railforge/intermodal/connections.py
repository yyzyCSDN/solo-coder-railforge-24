from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone, date

from railforge.terminal import chassis_pool


@dataclass(frozen=True)
class Move:
    id: str
    terminal: str
    arrival: datetime
    departure: datetime


def feasible(inbound: Move, outbound: Move, unload: timedelta, load: timedelta,
             cutoff_before: timedelta) -> bool:
    """Road service is reachable only when the container is lifted onto the
    chassis *and* ready before the road cutoff.

    The anchor is ``inbound.arrival`` (train physically at the terminal). Gate
    arrival is not discharge completion: both the unload and the load lift must
    still fit in the window.
    """
    if inbound.terminal != outbound.terminal:
        return False
    ready = inbound.arrival.astimezone(timezone.utc) + unload + load
    cutoff = outbound.departure.astimezone(timezone.utc) - cutoff_before
    return ready <= cutoff


def choose(inbound, candidates, unload, load, cutoff_before):
    xs = [x for x in candidates if feasible(inbound, x, unload, load, cutoff_before)]
    return min(xs, key=lambda x: (x.departure.astimezone(timezone.utc), x.id), default=None)
