from __future__ import annotations

from dataclasses import asdict, is_dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from typing import Mapping

from railforge.intermodal.connections import InterchangeWindow, window
from railforge.inventory.wagon_pool import Wagon, allocate


INTERMODAL_CHAIN_TOPIC = "feature.intermodal.chain"


def _canonical(value):
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return {"__datetime__": value.astimezone(timezone.utc).isoformat()}
    if isinstance(value, date):
        return {"__date__": value.isoformat()}
    if isinstance(value, timedelta):
        return {"__timedelta_seconds__": value.total_seconds()}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, frozenset):
        value = set(value)
    if isinstance(value, set):
        return [_canonical(v) for v in sorted(value, key=lambda x: json.dumps(_canonical(x), sort_keys=True, default=str))]
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    return value


def request_fingerprint(request: dict) -> str:
    body = json.dumps(_canonical(request), sort_keys=True, separators=(",", ":"), default=str)
    return sha256(body.encode("utf-8")).hexdigest()


def reserved_wagon_ids(stream) -> set[str]:
    ids: set[str] = set()
    for event in stream.snapshot():
        if event.topic != INTERMODAL_CHAIN_TOPIC or event.payload.get("status") != "committed":
            continue
        result = event.payload.get("result", {})
        for wagon in result.get("wagons", []):
            if isinstance(wagon, Mapping) and wagon.get("id") is not None:
                ids.add(str(wagon["id"]))
    return ids


def available_wagon_pool(rows: list[Wagon], stream) -> list[Wagon]:
    used = reserved_wagon_ids(stream)
    return [replace(wagon, reserved=True) if wagon.id in used else wagon for wagon in rows]


def book_chain(inbound, candidates, unload, load, cutoff,
               wagons, wagon_type, location, on, count, required_days, forbidden, stream):
    from railforge.intermodal.connections import choose

    move = choose(inbound, candidates, unload, load, cutoff)
    if move is None:
        raise ValueError("no intermodal connection")

    interchange_window: InterchangeWindow = window(inbound, move, unload, load, cutoff)
    if not interchange_window.feasible:
        raise ValueError("intermodal cutoff missed")

    pool = available_wagon_pool(wagons, stream)
    selected = allocate(pool, wagon_type, location, on, count, required_days, forbidden)
    return {
        "move": move,
        "window": interchange_window,
        "wagons": selected,
        "vehicle_pool_reservation": {
            "type": wagon_type,
            "location": location,
            "on": on,
            "quantity": count,
            "wagon_ids": tuple(wagon.id for wagon in selected),
        },
    }
