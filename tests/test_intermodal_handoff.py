import unittest
from datetime import datetime, timezone, timedelta

from railforge.api import RailForgeService
from railforge.intermodal.connections import Move, feasible, choose
from railforge.intermodal.handoff import (
    HandoffRequest,
    run_transfer,
    replay,
    request_payload,
    request_from_payload,
)
from railforge.terminal.chassis_pool import ChassisStock
from railforge.workflows.control import RailForgeWorkflowControl

U = timezone.utc


def move(mid, terminal, arrival_h, departure_h):
    return Move(
        id=mid,
        terminal=terminal,
        arrival=datetime(2026, 5, 1, arrival_h, tzinfo=U),
        departure=datetime(2026, 5, 1, departure_h, tzinfo=U),
    )


class FeasibleRegressionTests(unittest.TestCase):
    """The window is anchored at physical arrival; gate/entry time is not
    discharge completion."""

    def test_window_uses_arrival_not_departure(self):
        inbound = move("RAIL-1", "Duisburg", 8, 6)   # left origin 06:00, at terminal 08:00
        outbound = move("ROAD-9", "Duisburg", 10, 12)
        unload = timedelta(hours=2)
        load = timedelta(hours=1)
        cutoff_before = timedelta(hours=1)
        # ready = 08:00 + 2h + 1h = 11:00; cutoff = 12:00 - 1h = 11:00 -> exactly feasible
        self.assertTrue(feasible(inbound, outbound, unload, load, cutoff_before))

    def test_insufficient_window_rejected(self):
        inbound = move("RAIL-1", "Duisburg", 9, 6)
        outbound = move("ROAD-9", "Duisburg", 10, 12)
        # ready 12:00 > cutoff 11:00
        self.assertFalse(
            feasible(inbound, outbound, timedelta(hours=2), timedelta(hours=1),
                     timedelta(hours=1))
        )

    def test_old_bug_would_have_passed(self):
        # With the buggy anchor (inbound.departure = 06:00) ready would be 09:00
        # and this impossible connection would look feasible.
        inbound = move("RAIL-1", "Duisburg", 10, 6)
        outbound = move("ROAD-9", "Duisburg", 10, 12)
        self.assertFalse(
            feasible(inbound, outbound, timedelta(hours=2), timedelta(hours=1),
                     timedelta(hours=1))
        )

    def test_terminal_mismatch(self):
        self.assertFalse(
            feasible(move("a", "X", 8, 6), move("b", "Y", 18, 20),
                     timedelta(), timedelta(), timedelta())
        )

    def test_choose_picks_earliest_feasible(self):
        inbound = move("R", "T", 8, 6)
        candidates = [move("late", "T", 18, 20), move("tight", "T", 10, 11),
                      move("ok", "T", 12, 13)]
        # tight: ready 11:00 vs cutoff 10:00 no; ok: ready 11:00 <= 12:00 yes
        picked = choose(inbound, candidates, timedelta(hours=2),
                        timedelta(hours=1), timedelta(hours=1))
        self.assertEqual(picked.id, "ok")


class HandoffWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.pool = [
            ChassisStock(lot="L1", location="Duisburg", quantity=3.0,
                         available_on=datetime(2026, 4, 30, tzinfo=U).date()),
            ChassisStock(lot="L2", location="Duisburg", quantity=2.0,
                         available_on=datetime(2026, 4, 30, tzinfo=U).date()),
        ]
        self.inbound = move("RAIL-1", "Duisburg", 8, 6)
        self.outbound = move("ROAD-9", "Duisburg", 10, 13)
        self.at = datetime(2026, 5, 1, 7, tzinfo=U)

    def _request(self, request_id="REQ-1", qty=2.0):
        return HandoffRequest(
            request_id=request_id,
            inbound=self.inbound,
            outbound=self.outbound,
            unload=timedelta(hours=2),
            load=timedelta(hours=1),
            cutoff_before=timedelta(hours=1),
            chassis_quantity=qty,
        )

    def test_happy_path_publishes_and_consumes_pool(self):
        receipt = self.control.arrange_intermodal_handoff(
            "OP-1", self._request(), self.at, initial_pool=self.pool
        )
        plan = receipt.result["plan"]
        self.assertEqual(plan.status, "committed")
        self.assertEqual(plan.arrival_at.hour, 8)
        self.assertEqual(plan.unload_complete_at.hour, 10)
        self.assertEqual(plan.load_complete_at.hour, 11)
        self.assertEqual(plan.cutoff_at.hour, 12)
        steps = [s["step"] for s in plan.timeline]
        self.assertEqual(steps, ["arrival", "unload", "load", "cutoff", "chassis_pool"])
        remaining = self.control.store.read(
            "terminal.chassis-pool:Duisburg"
        ).value
        self.assertEqual(sum(r.quantity for r in remaining), 3.0)  # 5 - 2
        self.assertEqual(self.control.stream.head, 1)

    def test_cutoff_failure_publishes_nothing(self):
        bad = HandoffRequest(
            request_id="REQ-BAD",
            inbound=move("RAIL-X", "Duisburg", 10, 6),
            outbound=self.outbound,
            unload=timedelta(hours=2),
            load=timedelta(hours=2),
            cutoff_before=timedelta(hours=1),
            chassis_quantity=1.0,
        )
        with self.assertRaises(ValueError):
            self.control.arrange_intermodal_handoff(
                "OP-BAD", bad, self.at, initial_pool=self.pool
            )
        # No event, no audit, no pool write.
        self.assertEqual(self.control.stream.head, 0)
        self.assertTrue(self.control.trail.verify())
        self.assertEqual(
            list(self.control.trail.records), []
        )
        self.assertIsNone(self.control.store.read("terminal.chassis-pool:Duisburg"))
        self.assertNotIn("OP-BAD", self.control.receipts)

    def test_insufficient_pool_publishes_nothing(self):
        with self.assertRaises(ValueError):
            self.control.arrange_intermodal_handoff(
                "OP-BIG", self._request("REQ-BIG", qty=99.0), self.at,
                initial_pool=self.pool
            )
        self.assertEqual(self.control.stream.head, 0)
        self.assertIsNone(self.control.store.read("terminal.chassis-pool:Duisburg"))

    def test_duplicate_operation_is_idempotent(self):
        request = self._request()
        first = self.control.arrange_intermodal_handoff(
            "OP-1", request, self.at, initial_pool=self.pool
        )
        second = self.control.arrange_intermodal_handoff(
            "OP-1", request, self.at, initial_pool=self.pool
        )
        self.assertIs(first, second)
        self.assertEqual(self.control.stream.head, 1)
        remaining = self.control.store.read(
            "terminal.chassis-pool:Duisburg"
        ).value
        self.assertEqual(sum(r.quantity for r in remaining), 3.0)  # consumed once

    def test_duplicate_domain_request_with_new_operation_id_returns_same_receipt(self):
        first = self.control.arrange_intermodal_handoff(
            "OP-1", self._request("REQ-1"), self.at, initial_pool=self.pool
        )
        # Client retried and minted a new operation id but same request id.
        second = self.control.arrange_intermodal_handoff(
            "OP-1-RETRY", self._request("REQ-1"), self.at, initial_pool=self.pool
        )
        self.assertIs(first, second)
        self.assertEqual(self.control.stream.head, 1)

    def test_same_operation_id_different_workflow_rejected(self):
        self.control.arrange_intermodal_handoff(
            "OP-1", self._request("REQ-1"), self.at, initial_pool=self.pool
        )
        with self.assertRaises(ValueError):
            self.control.arrange_intermodal_handoff(
                "OP-1", self._request("REQ-2"), self.at, initial_pool=self.pool
            )

    def test_pool_cas_concurrent_conflict_publishes_once(self):
        # Two controls sharing one physical store race the first reservation.
        # The loser's CAS must fail before it publishes, so exactly one event
        # exists and the pool is consumed exactly once.
        import threading
        from railforge.storage.ops_store import VersionedStore
        from railforge.events.stream import EventStream
        from railforge.audit.trail import Trail

        class RacedStore(VersionedStore):
            """Release writes only after both racers observed the same v=0."""

            def __init__(self):
                super().__init__()
                self._reads = threading.Barrier(2)
                self._writes = threading.Barrier(2)

            def read(self, key):
                v = super().read(key)
                if key == "terminal.chassis-pool:Duisburg" and v is None:
                    self._reads.wait(timeout=5)
                return v

            def write(self, key, value, expected_version=None):
                if key == "terminal.chassis-pool:Duisburg":
                    self._writes.wait(timeout=5)
                return super().write(key, value, expected_version)

        store = RacedStore()
        c1 = RailForgeWorkflowControl(EventStream(), Trail(), store)
        c2 = RailForgeWorkflowControl(EventStream(), Trail(), store)
        outcomes = {}

        def worker(control, op, req_id, qty, key):
            try:
                control.arrange_intermodal_handoff(
                    op, self._request(req_id, qty), self.at, initial_pool=self.pool
                )
                outcomes[key] = "ok"
            except RuntimeError:
                outcomes[key] = "version-conflict"
            except Exception as exc:  # pragma: no cover - surface unexpected
                outcomes[key] = f"error:{exc!r}"

        t1 = threading.Thread(target=worker, args=(c1, "OP-1", "REQ-1", 2.0, "a"))
        t2 = threading.Thread(target=worker, args=(c2, "OP-2", "REQ-2", 2.0, "b"))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(sorted(outcomes.values()), ["ok", "version-conflict"])
        winner = c1 if outcomes["a"] == "ok" else c2
        self.assertEqual(winner.stream.head, 1)
        loser = c2 if winner is c1 else c1
        self.assertEqual(loser.stream.head, 0)
        self.assertEqual(len(loser.trail.records), 0)
        # Pool consumed by exactly one reservation.
        remaining = store.read("terminal.chassis-pool:Duisburg").value
        self.assertEqual(sum(r.quantity for r in remaining), 3.0)

    def test_replay_rebuilds_plan_from_stream(self):
        self.control.arrange_intermodal_handoff(
            "OP-1", self._request("REQ-1"), self.at, initial_pool=self.pool
        )
        self.control.arrange_intermodal_handoff(
            "OP-2", self._request("REQ-2", qty=1.0),
            datetime(2026, 5, 1, 8, tzinfo=U),
        )
        events = [
            {"topic": e.topic, "payload": e.payload}
            for e in self.control.stream.snapshot()
        ]
        plans = replay(events)
        self.assertEqual([p.request_id for p in plans], ["REQ-1", "REQ-2"])
        self.assertEqual(plans[0].load_complete_at.hour, 11)
        self.assertEqual(plans[0].chassis_reservation[0][0], "L1")
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())

    def test_payload_roundtrip_and_pure_domain(self):
        request = self._request()
        restored = request_from_payload(request_payload(request))
        self.assertEqual(restored.request_id, request.request_id)
        plan = run_transfer(request, self.pool)
        self.assertEqual(plan.status, "committed")
        # Pure evaluation must not mutate the supplied pool.
        self.assertEqual(sum(r.quantity for r in self.pool), 5.0)

    def test_service_facade_and_health(self):
        service = RailForgeService(self.control)
        receipt = service.arrange_handoff(
            "OP-1", self._request(), self.at, initial_pool=self.pool
        )
        self.assertEqual(receipt.subject, "REQ-1")
        replayed = service.replay_handoffs()
        self.assertEqual(len(replayed), 1)
        health = service.health()
        self.assertEqual(health["status"], "ok")
        self.assertIn("intermodal.handoff", health["capabilities"])


if __name__ == "__main__":
    unittest.main()
