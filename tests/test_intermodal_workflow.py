from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import unittest

from railforge.api import RailForgeService
from railforge.intermodal.connections import Move
from railforge.inventory.wagon_pool import Wagon
from railforge.workflows.control import RailForgeWorkflowControl


U = timezone.utc
AT = datetime(2026, 1, 1, 9, tzinfo=U)


class IntermodalWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.service = RailForgeService(self.control)
        self.inbound = Move("RAIL-IN", "DALLAS", datetime(2026, 1, 1, 10, tzinfo=U),
                            datetime(2026, 1, 1, 11, tzinfo=U))
        self.road = Move("ROAD-OUT", "DALLAS", datetime(2026, 1, 1, 10, tzinfo=U),
                         datetime(2026, 1, 1, 11, 15, tzinfo=U))
        self.wagons = [
            Wagon("W1", "BOX", "DALLAS", date(2026, 2, 1)),
            Wagon("W2", "BOX", "DALLAS", date(2026, 2, 1)),
        ]

    def book(self, operation_id="REQ-1", road=None, count=1):
        road = road or self.road
        return self.service.features.book_intermodal_chain(
            operation_id, self.inbound, [road], timedelta(minutes=30),
            timedelta(minutes=30), timedelta(0), self.wagons, "BOX",
            "DALLAS", date(2026, 1, 2), count, 0, frozenset(), AT,
        )

    def test_arrival_feeds_full_unload_and_load_window(self):
        receipt = self.book(count=1)
        window = receipt.result["window"]
        self.assertEqual(window.unload_start, datetime(2026, 1, 1, 10, tzinfo=U))
        self.assertEqual(window.unload_end, datetime(2026, 1, 1, 10, 30, tzinfo=U))
        self.assertEqual(window.load_end, datetime(2026, 1, 1, 11, tzinfo=U))
        self.assertEqual(window.cutoff_at, datetime(2026, 1, 1, 11, 15, tzinfo=U))
        self.assertTrue(window.feasible)
        self.assertEqual(receipt.result["vehicle_pool_reservation"]["wagon_ids"], ("W1",))
        self.assertEqual(receipt.event.payload["status"], "committed")
        self.assertEqual(receipt.event.payload["control_path"], "atomic")
        self.assertTrue(self.control.stream.verify_chain())

    def test_duplicate_request_is_idempotent(self):
        first = self.book()
        duplicate = self.book()
        self.assertIs(first, duplicate)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(len(self.control.trail._rows), 1)

    def test_same_operation_id_with_changed_request_is_rejected(self):
        self.book()
        with self.assertRaisesRegex(ValueError, "different request"):
            self.book(count=2)
        self.assertEqual(self.control.stream.head, 1)

    def test_failed_cutoff_or_pool_does_not_publish(self):
        tight = Move("ROAD-TIGHT", "DALLAS", datetime(2026, 1, 1, 10, tzinfo=U),
                     datetime(2026, 1, 1, 10, 45, tzinfo=U))
        with self.assertRaisesRegex(ValueError, "no intermodal connection"):
            self.book("FAILED-CUTOFF", tight)
        self.assertIsNone(self.control.store.read("FAILED-CUTOFF"))
        self.assertEqual(self.control.stream.head, 0)

        self.book(count=1)
        with self.assertRaisesRegex(ValueError, "insufficient wagons"):
            self.service.features.book_intermodal_chain(
                "FAILED-POOL", self.inbound, [self.road], timedelta(minutes=30),
                timedelta(minutes=30), timedelta(0), self.wagons[:1], "BOX",
                "DALLAS", date(2026, 1, 2), 1, 0, frozenset(), AT,
            )
        self.assertEqual(self.control.stream.head, 1)
        self.assertIsNone(self.control.store.read("FAILED-POOL"))

    def test_workflow_state_is_replayable(self):
        receipt = self.book()
        replayed_service = RailForgeService(RailForgeWorkflowControl(stream=self.control.stream,
                                                                     trail=self.control.trail,
                                                                     store=self.control.store))
        replayed = replayed_service.workflow().replay({"feature.intermodal.chain"})
        self.assertEqual(replayed["REQ-1"].event, receipt.event)

        duplicate = replayed_service.features.book_intermodal_chain(
            "REQ-1", self.inbound, [self.road], timedelta(minutes=30),
            timedelta(minutes=30), timedelta(0), self.wagons, "BOX",
            "DALLAS", date(2026, 1, 2), 1, 0, frozenset(), AT,
        )
        self.assertEqual(duplicate.event, receipt.event)

        with self.assertRaisesRegex(ValueError, "insufficient wagons"):
            replayed_service.features.book_intermodal_chain(
                "REQ-2", self.inbound, [self.road], timedelta(minutes=30),
                timedelta(minutes=30), timedelta(0), self.wagons[:1], "BOX",
                "DALLAS", date(2026, 1, 2), 1, 0, frozenset(), AT,
            )
        self.assertEqual(self.control.stream.head, 1)


if __name__ == "__main__":
    unittest.main()
