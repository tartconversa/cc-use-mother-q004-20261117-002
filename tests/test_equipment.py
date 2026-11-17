import threading

from cycleflow.demo import END, START
from tests.common import ServiceCase


DAY = "2026-01-12"


def ev(number, kind, at, payload, source="test"):
    return {"source": source, "external_id": str(number), "kind": kind,
            "occurred_at": at, "payload": payload}


def at(clock):
    return f"{DAY}T{clock}Z"


class EquipmentCase(ServiceCase):
    def setUp(self):
        super().setUp()
        self.seed_design()
        self.seed_lots()
        self.seed_plan()
        self.seed_job("J1", quantity=2, number=5, minute=4)
        self.seed_job("J2", quantity=2, number=6, minute=5)
        self.register()

    def register(self, equipment_id="E1", number=40, clock="07:00:00", **extra):
        payload = {"equipment_id": equipment_id, "station": "test-rig"}
        payload.update(extra)
        return self.service.ingest(ev(number, "equipment.register", at(clock), payload))

    def enqueue(self, number, job, clock, equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.enqueue", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job}))

    def withdraw(self, number, job, clock, equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.withdraw", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job}))

    def acquire(self, number, job, clock, equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.acquire", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job}))

    def release(self, number, job, clock, equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.release", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job}))

    def down(self, number, clock, reason="maintenance", equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.down", at(clock),
                                      {"equipment_id": equipment_id, "reason": reason}))

    def up(self, number, clock, equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.up", at(clock),
                                      {"equipment_id": equipment_id}))

    def reserve(self, number, job, start, end, clock="08:00:00", equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.reserve", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job,
                                       "slot_start": at(start), "slot_end": at(end)}))

    def rebook(self, number, job, start, end, clock="08:30:00", equipment_id="E1"):
        return self.service.ingest(ev(number, "equipment.rebook", at(clock),
                                      {"equipment_id": equipment_id, "job_id": job,
                                       "slot_start": at(start), "slot_end": at(end)}))


class EquipmentRuleTests(EquipmentCase):
    def test_register_and_detail_state(self):
        detail = self.service.get_equipment("E1")
        self.assertEqual(detail["state"], "available")
        self.assertIsNone(detail["occupied_by"])
        duplicate = self.register(number=41)
        self.assertEqual(duplicate["state"], "rejected")
        self.assertEqual(duplicate["error_code"], "duplicate_equipment")

    def test_missing_equipment_waits_then_retries(self):
        outcome = self.enqueue(50, "J1", "10:00:00", equipment_id="GHOST")
        self.assertEqual(outcome["state"], "waiting")
        self.assertEqual(outcome["error_code"], "missing_dependency")
        self.register("GHOST", number=51)
        retried = self.service.retry(outcome["id"])
        self.assertEqual(retried["state"], "accepted")

    def test_missing_job_waits(self):
        outcome = self.enqueue(50, "GHOST", "10:00:00")
        self.assertEqual(outcome["state"], "waiting")
        self.assertEqual(outcome["error_code"], "missing_dependency")

    def test_missing_payload_field_rejected(self):
        outcome = self.service.ingest(ev(50, "equipment.acquire", at("10:00:00"),
                                         {"equipment_id": "E1"}))
        self.assertEqual(outcome["state"], "rejected")
        self.assertEqual(outcome["error_code"], "missing_field")

    def test_acquire_closes_queue_episode(self):
        self.enqueue(50, "J1", "10:00:00")
        self.acquire(51, "J1", "10:05:00")
        self.release(52, "J1", "10:20:00")
        detail = self.service.get_equipment("E1")
        self.assertEqual(detail["state"], "available")
        self.assertEqual(detail["queue"][0]["close_kind"], "acquired")
        wait = self.service.job_wait("J1")
        self.assertEqual(wait["by_cause"]["idle_device"], 300)
        self.assertEqual(wait["equipment_wait_seconds"], 300)
        self.assertEqual(wait["occupancy"][0]["seconds"], 900)

    def test_double_acquire_rejected_until_release(self):
        self.assertEqual(self.acquire(50, "J1", "10:00:00")["state"], "accepted")
        busy = self.acquire(51, "J2", "10:10:00")
        self.assertEqual(busy["state"], "rejected")
        self.assertEqual(busy["error_code"], "equipment_busy")
        self.release(52, "J1", "10:30:00")
        self.assertEqual(self.acquire(53, "J2", "10:30:00")["state"], "accepted")

    def test_out_of_order_acquire_cannot_overlap(self):
        self.acquire(50, "J1", "10:00:00")
        self.release(51, "J1", "10:30:00")
        late = self.acquire(52, "J2", "09:50:00")
        self.assertEqual(late["state"], "rejected")
        self.assertEqual(late["error_code"], "equipment_busy")
        with self.service.store.read() as db:
            count = db.execute("SELECT COUNT(*) FROM equipment_occupancy").fetchone()[0]
        self.assertEqual(count, 1)
        self.assertTrue(self.service.audit("robot-arm")["ok"])

    def test_release_and_withdraw_require_open_records(self):
        self.assertEqual(self.release(50, "J1", "10:00:00")["error_code"], "not_occupied")
        self.assertEqual(self.withdraw(51, "J1", "10:00:00")["error_code"], "not_queued")

    def test_withdraw_closes_episode_without_occupancy(self):
        self.enqueue(50, "J1", "10:00:00")
        self.assertEqual(self.withdraw(51, "J1", "10:15:00")["state"], "accepted")
        self.assertEqual(self.acquire(52, "J1", "10:20:00")["state"], "accepted")
        wait = self.service.job_wait("J1")
        self.assertEqual(wait["episodes"][0]["end_kind"], "withdrawn")
        self.assertEqual(wait["by_cause"]["idle_device"], 900)

    def test_down_up_pairing_rules(self):
        self.assertEqual(self.up(50, "10:00:00")["error_code"], "not_down")
        self.assertEqual(self.down(51, "10:00:00")["state"], "accepted")
        again = self.down(52, "10:10:00")
        self.assertEqual(again["error_code"], "already_down")
        self.assertEqual(self.up(53, "10:40:00")["state"], "accepted")

    def test_acquire_while_down_rejected(self):
        self.down(50, "10:00:00")
        blocked = self.acquire(51, "J1", "10:10:00")
        self.assertEqual(blocked["error_code"], "equipment_down")
        self.up(52, "10:40:00")
        self.assertEqual(self.acquire(53, "J1", "10:40:00")["state"], "accepted")

    def test_terminal_job_cannot_queue_after_completion(self):
        self.complete_stages("J1", base=10)
        self.ingest(30, "test.record", 30, {"test_id": "T1", "job_id": "J1",
                                            "plan_id": "P1", "passed": 2, "failed": 0})
        outcome = self.enqueue(50, "J1", "12:00:00")
        self.assertEqual(outcome["error_code"], "terminal_job")

    def test_reservation_rules(self):
        self.assertEqual(self.reserve(50, "J1", "10:00:00", "11:00:00")["state"], "accepted")
        clash = self.reserve(51, "J2", "10:30:00", "11:30:00")
        self.assertEqual(clash["error_code"], "slot_conflict")
        self.assertEqual(self.reserve(52, "J2", "11:00:00", "12:00:00")["state"], "accepted")
        duplicate = self.reserve(53, "J1", "13:00:00", "14:00:00")
        self.assertEqual(duplicate["error_code"], "reservation_exists")
        bad_slot = self.service.ingest(ev(54, "equipment.reserve", at("08:05:00"),
                                          {"equipment_id": "E1", "job_id": "J2",
                                           "slot_start": at("15:00:00"), "slot_end": at("14:00:00")}))
        self.assertEqual(bad_slot["error_code"], "invalid_slot")
        self.register("E2", number=55)
        no_res = self.rebook(56, "J1", "13:00:00", "14:00:00", equipment_id="E2")
        self.assertEqual(no_res["error_code"], "no_reservation")
        moved = self.rebook(57, "J1", "12:00:00", "13:00:00")
        self.assertEqual(moved["state"], "accepted")
        detail = self.service.get_equipment("E1")
        states = {row["state"] for row in detail["reservations"]}
        self.assertIn("superseded", states)
        self.assertIn("active", states)


class AttributionTests(EquipmentCase):
    def test_contention_attribution_names_blocker(self):
        self.acquire(50, "J1", "10:00:00")
        self.release(51, "J1", "10:30:00")
        self.enqueue(52, "J2", "10:05:00")
        self.acquire(53, "J2", "10:30:00")
        self.release(54, "J2", "10:45:00")
        wait = self.service.job_wait("J2")
        self.assertEqual(wait["by_cause"]["contention"], 1500)
        self.assertEqual(wait["blockers"], ["J1"])
        self.assertEqual(wait["by_cause"]["downtime"], 0)
        self.assertEqual(self.service.job_wait("J1")["equipment_wait_seconds"], 0)

    def test_downtime_attribution_stays_equipment_level(self):
        self.down(50, "10:00:00")
        self.up(51, "10:40:00")
        self.enqueue(52, "J1", "10:10:00")
        self.acquire(53, "J1", "10:40:00")
        self.release(54, "J1", "10:50:00")
        wait = self.service.job_wait("J1")
        self.assertEqual(wait["by_cause"]["downtime"], 1800)
        report = self.service.equipment_report("robot-arm", START, END)
        device = report["devices"][0]
        self.assertEqual(device["down_seconds"], 2400)
        self.assertEqual(device["wait_by_cause"]["downtime"], 1800)
        self.assertIn("never allocated", report["note"])

    def test_reservation_change_attribution(self):
        self.reserve(50, "J1", "10:00:00", "11:00:00")
        self.rebook(51, "J1", "12:00:00", "13:00:00")
        self.enqueue(52, "J1", "09:30:00")
        self.acquire(53, "J1", "12:00:00")
        self.release(54, "J1", "12:30:00")
        wait = self.service.job_wait("J1")
        self.assertEqual(wait["by_cause"]["reservation_change"], 7200)
        self.assertEqual(wait["by_cause"]["idle_device"], 1800)
        self.assertEqual(wait["by_cause"]["contention"], 0)

    def test_contention_beats_reservation_change_when_device_busy(self):
        self.reserve(50, "J1", "10:00:00", "11:00:00")
        self.rebook(51, "J1", "12:00:00", "13:00:00")
        self.acquire(52, "J2", "09:30:00")
        self.release(53, "J2", "12:00:00")
        self.enqueue(54, "J1", "10:00:00")
        self.acquire(55, "J1", "12:00:00")
        self.release(56, "J1", "12:10:00")
        wait = self.service.job_wait("J1")
        self.assertEqual(wait["by_cause"]["contention"], 7200)
        self.assertEqual(wait["by_cause"]["reservation_change"], 0)
        self.assertEqual(wait["blockers"], ["J2"])

    def test_open_episode_clamped_by_window_and_flagged(self):
        self.enqueue(50, "J1", "10:00:00")
        report = self.service.equipment_report("robot-arm", f"{DAY}T00:00:00Z", f"{DAY}T23:59:59Z")
        job = next(j for j in report["jobs"] if j["job_id"] == "J1")
        self.assertEqual(job["by_cause"]["idle_device"], 14 * 3600 - 1)
        self.assertEqual(job["episodes"][0]["end_kind"], "open")
        self.assertIn({"code": "open_queue", "subject": "E1/J1"}, report["anomalies"])
        wait = self.service.job_wait("J1", upto=at("12:00:00"))
        self.assertEqual(wait["by_cause"]["idle_device"], 7200)

    def test_report_breakdowns_and_delivery_flag(self):
        self.acquire(50, "J1", "10:00:00")
        self.release(51, "J1", "10:30:00")
        self.enqueue(52, "J2", "10:05:00")
        self.acquire(53, "J2", "10:30:00")
        self.release(54, "J2", "10:45:00")
        report = self.service.equipment_report("robot-arm", START, END)
        self.assertEqual(report["totals"]["by_cause"]["contention"], 1500)
        self.assertEqual(report["totals"]["jobs_with_equipment_wait"], 1)
        revision = report["revisions"][0]
        self.assertEqual(revision["revision"], 1)
        self.assertEqual(revision["by_cause"]["contention"], 1500)
        self.assertEqual(set(revision["job_ids"]), {"J1", "J2"})
        device = report["devices"][0]
        self.assertEqual(device["occupied_seconds"], 2700)
        self.assertEqual(device["contention_blockers"], ["J1"])
        j2 = next(j for j in report["jobs"] if j["job_id"] == "J2")
        self.assertTrue(j2["blocking_delivery"])
        self.assertEqual(j2["blockers"], ["J1"])
        self.assertEqual(report["weeks"][0]["by_cause"]["contention"], 1500)
        self.assertEqual(report["anomalies"], [])

    def test_cross_week_wait_splits_at_iso_boundary(self):
        self.service.ingest(ev(60, "design.release", "2026-01-01T00:00:00Z",
                               {"design_id": "RX", "product": "gantry",
                                "revision": 1, "parts": {"motor": 1}}))
        self.service.ingest(ev(61, "lot.receive", "2026-01-01T00:05:00Z",
                               {"lot_id": "MX", "part": "motor", "supplier": "m", "quantity": 4}))
        self.service.ingest(ev(62, "job.open", "2026-01-04T08:00:00Z",
                               {"job_id": "JX", "design_id": "RX", "quantity": 1}))
        self.service.ingest(ev(63, "equipment.register", "2026-01-01T00:10:00Z",
                               {"equipment_id": "EX", "station": "rig"}))
        self.service.ingest(ev(64, "equipment.enqueue", "2026-01-04T23:00:00Z",
                               {"equipment_id": "EX", "job_id": "JX"}))
        self.service.ingest(ev(65, "equipment.acquire", "2026-01-05T01:00:00Z",
                               {"equipment_id": "EX", "job_id": "JX"}))
        self.service.ingest(ev(66, "equipment.release", "2026-01-05T02:00:00Z",
                               {"equipment_id": "EX", "job_id": "JX"}))
        report = self.service.equipment_report("gantry", "2026-01-01T00:00:00Z",
                                               "2026-02-01T00:00:00Z")
        weeks = {w["week"]: w for w in report["weeks"]}
        self.assertEqual(weeks["2026-W01"]["by_cause"]["idle_device"], 3600)
        self.assertEqual(weeks["2026-W02"]["by_cause"]["idle_device"], 3600)
        self.assertEqual(report["totals"]["attributed_wait_seconds"], 7200)


class EquipmentIntegrationTests(ServiceCase):
    def test_existing_facts_untouched_by_equipment_events(self):
        before = self.full_scenario()["jobs"][0]
        for item in (ev(13, "equipment.register", at("08:00:00"),
                        {"equipment_id": "RIG", "station": "integration"}),
                     ev(14, "equipment.enqueue", at("09:16:00"),
                        {"equipment_id": "RIG", "job_id": "J1"}),
                     ev(15, "equipment.acquire", at("09:21:00"),
                        {"equipment_id": "RIG", "job_id": "J1"}),
                     ev(16, "equipment.release", at("09:27:00"),
                        {"equipment_id": "RIG", "job_id": "J1"})):
            self.assertEqual(self.service.ingest(item)["state"], "accepted")
        report = self.service.report("robot-arm", START, END)
        after = report["jobs"][0]
        self.assertEqual(after["stages"], before["stages"])
        self.assertEqual(after["tests"], before["tests"])
        self.assertEqual(after["wait_seconds"], before["wait_seconds"])
        self.assertEqual(after["cycle_seconds"], before["cycle_seconds"])
        self.assertTrue(report["quality"]["publishable"])
        frozen = self.service.publish("jan", "robot-arm", START, END, report["fingerprint"])
        self.assertFalse(frozen["repeated"])
        self.assertTrue(self.service.audit("robot-arm")["ok"])

    def test_concurrent_acquire_has_single_winner(self):
        self.seed_design()
        self.seed_lots()
        self.seed_job("J1", quantity=1, number=5, minute=4)
        self.seed_job("J2", quantity=1, number=6, minute=5)
        self.service.ingest(ev(7, "equipment.register", at("07:00:00"),
                               {"equipment_id": "E1", "station": "rig"}))
        barrier = threading.Barrier(3)
        outcomes = {}

        def worker(job, number):
            barrier.wait()
            outcomes[job] = self.service.ingest(
                ev(number, "equipment.acquire", at("10:00:00"),
                   {"equipment_id": "E1", "job_id": job}))

        threads = [threading.Thread(target=worker, args=("J1", 50)),
                   threading.Thread(target=worker, args=("J2", 51))]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        states = sorted(outcomes[job]["state"] for job in ("J1", "J2"))
        self.assertEqual(states, ["accepted", "rejected"])
        loser = next(job for job in ("J1", "J2") if outcomes[job]["state"] == "rejected")
        self.assertEqual(outcomes[loser]["error_code"], "equipment_busy")
        with self.service.store.read() as db:
            count = db.execute("SELECT COUNT(*) FROM equipment_occupancy").fetchone()[0]
        self.assertEqual(count, 1)
        self.assertTrue(self.service.audit("robot-arm")["ok"])

    def test_audit_flags_injected_occupancy_overlap(self):
        self.seed_design()
        self.seed_lots()
        self.seed_job("J1", quantity=1, number=5, minute=4)
        self.seed_job("J2", quantity=1, number=6, minute=5)
        self.service.ingest(ev(7, "equipment.register", at("07:00:00"),
                               {"equipment_id": "E1", "station": "rig"}))
        self.service.ingest(ev(8, "equipment.acquire", at("10:00:00"),
                               {"equipment_id": "E1", "job_id": "J1"}))
        self.service.ingest(ev(9, "equipment.release", at("10:30:00"),
                               {"equipment_id": "E1", "job_id": "J1"}))
        self.assertTrue(self.service.audit("robot-arm")["ok"])
        with self.service.store.transaction() as db:
            db.execute(
                "INSERT INTO equipment_occupancy(equipment_id,job_id,acquired_at,released_at,"
                "acquire_event_id) VALUES('E1','J2',?,?,8)",
                (at("10:15:00"), at("10:45:00")))
        audit = self.service.audit("robot-arm")
        self.assertFalse(audit["ok"])
        self.assertIn("occupancy_overlap", {f["code"] for f in audit["findings"]})
