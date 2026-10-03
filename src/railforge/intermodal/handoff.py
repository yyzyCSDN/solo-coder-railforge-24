"""Replayable rail-to-road interchange handoff workflow.

The handoff is modelled as one ordered, deterministic step list:

    arrival -> unload -> load -> cutoff -> chassis_pool_reservation

Every step is pure: it either extends a :class:`TransferPlan` or raises.  No
step mutates shared state, so a recorded plan can be rebuilt byte-for-byte from
the request parameters alone (:func:`replay`).  Publication of the chassis
reservation is the control plane's job (see
``RailForgeWorkflowControl.arrange_intermodal_handoff``) and happens only after
the cutoff step has passed -- a failed cutoff or an empty vehicle pool never
publishes an appointment.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from railforge.intermodal.connections import Move
from railforge.terminal.chassis_pool import ChassisStock, apply, reserve


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class HandoffRequest:
    request_id: str
    inbound: Move
    outbound: Move
    unload: timedelta
    load: timedelta
    cutoff_before: timedelta
    chassis_quantity: float
    chassis_on: date | None = None


@dataclass(frozen=True)
class TransferPlan:
    """Result of one successful handoff run.

    ``timeline`` records every workflow step in order. ``status`` is either
    ``committed`` (all steps passed) or ``rejected`` (a step failed). Only
    committed plans carry a chassis reservation.
    """

    request_id: str
    status: str
    terminal: str
    timeline: tuple[dict[str, Any], ...]
    arrival_at: datetime | None = None
    unload_complete_at: datetime | None = None
    load_complete_at: datetime | None = None
    cutoff_at: datetime | None = None
    slack_seconds: float | None = None
    chassis_reservation: tuple[tuple[str, float], ...] = ()
    error_step: str | None = None
    error: str | None = None


def _fail(request: HandoffRequest, step: str, message: str,
          timeline: list[dict[str, Any]]) -> TransferPlan:
    timeline.append({"step": step, "status": "rejected", "detail": message})
    return TransferPlan(
        request_id=request.request_id,
        status="rejected",
        terminal=request.inbound.terminal,
        timeline=tuple(timeline),
        error_step=step,
        error=message,
    )


def _chassis_date(request: HandoffRequest) -> date:
    return request.chassis_on or _utc(request.inbound.arrival).date()


def timeline(request: HandoffRequest) -> tuple[dict[str, Any], ...]:
    """Deterministic ordered timeline of the pure domain steps."""
    arrival = _utc(request.inbound.arrival)
    unload_done = arrival + request.unload
    load_done = unload_done + request.load
    cutoff = _utc(request.outbound.departure) - request.cutoff_before
    return (
        {"step": "arrival", "status": "ok", "at": arrival},
        {"step": "unload", "status": "ok", "at": unload_done,
         "duration_seconds": request.unload.total_seconds()},
        {"step": "load", "status": "ok", "at": load_done,
         "duration_seconds": request.load.total_seconds()},
        {"step": "cutoff", "status": "ok", "at": cutoff,
         "ready_at": load_done, "slack_seconds": (cutoff - load_done).total_seconds()},
    )


def run_transfer(request: HandoffRequest,
                 chassis_rows: Iterable[ChassisStock]) -> TransferPlan:
    """Evaluate the whole handoff without publishing anything.

    Raises ``ValueError`` on the first failing step so callers cannot mistake a
    rejection for a reservation; the rejected :class:`TransferPlan` is attached
    as ``error.plan`` for diagnostics.
    """
    steps: list[dict[str, Any]] = []

    # Step 1 — arrival: the rail move must actually end where the road move starts.
    arrival = _utc(request.inbound.arrival)
    if request.inbound.terminal != request.outbound.terminal:
        plan = _fail(request, "arrival", "terminal mismatch", steps)
        raise _HandoffRejected(plan)
    steps.append({"step": "arrival", "status": "ok", "at": arrival})

    # Step 2 — unload: gate arrival is NOT discharge completion; the unload lift
    # is worked from the physical arrival instant.
    unload_done = arrival + request.unload
    if request.unload < timedelta(0):
        plan = _fail(request, "unload", "negative unload duration", steps)
        raise _HandoffRejected(plan)
    steps.append({"step": "unload", "status": "ok", "at": unload_done,
                  "duration_seconds": request.unload.total_seconds()})

    # Step 3 — load: lifting onto the road chassis.
    if request.load < timedelta(0):
        plan = _fail(request, "load", "negative load duration", steps)
        raise _HandoffRejected(plan)
    load_done = unload_done + request.load
    steps.append({"step": "load", "status": "ok", "at": load_done,
                  "duration_seconds": request.load.total_seconds()})

    # Step 4 — cutoff: loaded-and-ready must beat the road service cutoff.
    cutoff = _utc(request.outbound.departure) - request.cutoff_before
    slack = (cutoff - load_done).total_seconds()
    if load_done > cutoff:
        detail = f"ready {load_done.isoformat()} after road cutoff {cutoff.isoformat()}"
        steps.append({"step": "cutoff", "status": "rejected", "at": cutoff,
                      "ready_at": load_done, "slack_seconds": slack, "detail": detail})
        plan = TransferPlan(
            request_id=request.request_id,
            status="rejected",
            terminal=request.inbound.terminal,
            timeline=tuple(steps),
            arrival_at=arrival,
            unload_complete_at=unload_done,
            load_complete_at=load_done,
            cutoff_at=cutoff,
            slack_seconds=slack,
            error_step="cutoff",
            error=detail,
        )
        raise _HandoffRejected(plan)
    steps.append({"step": "cutoff", "status": "ok", "at": cutoff,
                  "ready_at": load_done, "slack_seconds": slack})

    # Step 5 — vehicle (chassis) pool reservation. Checked last, published last.
    on = _chassis_date(request)
    try:
        reservation = reserve(list(chassis_rows), request.inbound.terminal, on,
                              request.chassis_quantity)
    except ValueError as exc:
        plan = _fail(request, "chassis_pool", str(exc), steps)
        raise _HandoffRejected(plan) from exc
    steps.append({"step": "chassis_pool", "status": "ok", "on": on,
                  "reservation": tuple(reservation)})

    return TransferPlan(
        request_id=request.request_id,
        status="committed",
        terminal=request.inbound.terminal,
        timeline=tuple(steps),
        arrival_at=arrival,
        unload_complete_at=unload_done,
        load_complete_at=load_done,
        cutoff_at=cutoff,
        slack_seconds=slack,
        chassis_reservation=tuple(reservation),
    )


def apply_transfer(plan: TransferPlan,
                   chassis_rows: Iterable[ChassisStock]) -> list[ChassisStock]:
    """Return the pool rows after consuming a committed plan's reservation."""
    if plan.status != "committed":
        raise ValueError("cannot apply a non-committed plan")
    return apply(list(chassis_rows), list(plan.chassis_reservation))


class _HandoffRejected(ValueError):
    def __init__(self, plan: TransferPlan):
        super().__init__(plan.error or "handoff rejected")
        self.plan = plan


def request_payload(request: HandoffRequest) -> dict[str, Any]:
    """JSON-safe snapshot of the request, embedded in the published event so a
    replay needs nothing but the stream."""
    return {
        "request_id": request.request_id,
        "terminal": request.inbound.terminal,
        "inbound_move": request.inbound.id,
        "outbound_move": request.outbound.id,
        "inbound_arrival": _utc(request.inbound.arrival).isoformat(),
        "outbound_departure": _utc(request.outbound.departure).isoformat(),
        "unload_seconds": request.unload.total_seconds(),
        "load_seconds": request.load.total_seconds(),
        "cutoff_before_seconds": request.cutoff_before.total_seconds(),
        "chassis_quantity": request.chassis_quantity,
        "chassis_on": _chassis_date(request).isoformat(),
    }


def request_from_payload(data: Mapping[str, Any]) -> HandoffRequest:
    inbound = Move(id=data["inbound_move"], terminal=data["terminal"],
                   arrival=datetime.fromisoformat(data["inbound_arrival"]),
                   departure=datetime.fromisoformat(data["inbound_arrival"]))
    outbound = Move(id=data["outbound_move"], terminal=data["terminal"],
                    arrival=datetime.fromisoformat(data["outbound_departure"]),
                    departure=datetime.fromisoformat(data["outbound_departure"]))
    return HandoffRequest(
        request_id=data["request_id"],
        inbound=inbound,
        outbound=outbound,
        unload=timedelta(seconds=float(data["unload_seconds"])),
        load=timedelta(seconds=float(data["load_seconds"])),
        cutoff_before=timedelta(seconds=float(data["cutoff_before_seconds"])),
        chassis_quantity=float(data["chassis_quantity"]),
        chassis_on=date.fromisoformat(data["chassis_on"]),
    )


def replay(events: Iterable[Mapping[str, Any]]) -> list[TransferPlan]:
    """Rebuild committed handoff plans from the hash-chained event stream.

    Each ``intermodal.handoff.reserved`` event is re-evaluated from its embedded
    request snapshot; the replayed timeline/result must match what was recorded.
    The function is read-only and deterministic.
    """
    plans: list[TransferPlan] = []
    topic_handoff = "intermodal.handoff.reserved"
    for event in events:
        payload = event["payload"] if "payload" in event else event
        topic = event.get("topic") or payload.get("topic")
        if topic is not None and topic != topic_handoff:
            continue
        if "request" not in payload:
            continue
        request = request_from_payload(payload["request"])
        result = payload["result"]
        recorded = result.get("plan", result)
        # Recompute the pure timeline (arrival..cutoff) from the request alone;
        # the reservation allocation was recorded at commit time.
        steps = timeline(request)
        reservation = tuple(
            (lot, qty) for lot, qty in recorded.get("chassis_reservation", [])
        )
        steps = steps + ({"step": "chassis_pool", "status": "ok",
                          "on": request_payload(request)["chassis_on"],
                          "reservation": reservation},)
        plan = TransferPlan(
            request_id=request.request_id,
            status=recorded.get("status", "committed"),
            terminal=request.inbound.terminal,
            timeline=steps,
            arrival_at=request.inbound.arrival,
            unload_complete_at=steps[1]["at"],
            load_complete_at=steps[2]["at"],
            cutoff_at=steps[3]["at"],
            slack_seconds=steps[3]["slack_seconds"],
            chassis_reservation=reservation,
        )
        # Guard against a tampered or replayed-out-of-order stream.
        if recorded.get("load_complete_at") != _utc(plan.load_complete_at).isoformat():
            raise ValueError("handoff replay mismatch")
        plans.append(plan)
    return plans
