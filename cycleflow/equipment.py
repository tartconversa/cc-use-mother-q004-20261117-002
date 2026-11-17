from .contracts import DomainError, MissingDependency, duration_seconds, keys, required_text, timestamp
from .execution import ExecutionRules


TERMINAL = ("completed", "failed", "scrapped")


class EquipmentRules:
    """Shared equipment occupancy, queueing, reservations and downtime.

    Occupancy and downtime are half-open occurred_at intervals. Every write is
    validated against the intervals already accepted, never against arrival
    order, so out-of-order events cannot double-book a device: an acquire is
    rejected whenever any existing occupancy ends after its occurred_at (or is
    still open), which keeps accepted intervals pairwise disjoint per device.
    """

    def __init__(self, db):
        self.db = db
        self.jobs = ExecutionRules(db)

    def get(self, equipment_id):
        row = self.db.execute("SELECT * FROM equipment WHERE equipment_id=?", (equipment_id,)).fetchone()
        if row is None:
            raise MissingDependency("equipment", equipment_id)
        return dict(row)

    def _registered(self, equipment_id, occurred_at):
        equipment = self.get(equipment_id)
        if occurred_at < equipment["registered_at"]:
            raise DomainError("time_order", "event precedes equipment registration")
        return equipment

    def _job(self, job_id, occurred_at, *, opened_check=True, terminal_check=True):
        job = self.jobs.get(job_id)
        if opened_check and occurred_at < job["opened_at"]:
            raise DomainError("time_order", "equipment event precedes job open")
        if terminal_check and job["status"] in TERMINAL and job["completed_at"] \
                and occurred_at >= job["completed_at"]:
            raise DomainError("terminal_job", "terminal job cannot queue for equipment", 409)
        return job

    def _open_queue(self, equipment_id, job_id):
        return self.db.execute(
            "SELECT * FROM equipment_queue WHERE equipment_id=? AND job_id=? AND closed_at IS NULL",
            (equipment_id, job_id)).fetchone()

    def register(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "station"}, {"equipment_id", "station", "line"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        station = required_text(payload["station"], "station")
        line = payload.get("line")
        if line is not None:
            line = required_text(line, "line")
        if self.db.execute("SELECT 1 FROM equipment WHERE equipment_id=?", (equipment_id,)).fetchone():
            raise DomainError("duplicate_equipment", "equipment already registered", 409)
        self.db.execute("INSERT INTO equipment(equipment_id,station,line,registered_at) VALUES(?,?,?,?)",
                        (equipment_id, station, line, occurred_at))
        return {"equipment_id": equipment_id, "station": station, "line": line}

    def enqueue(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at)
        if self._open_queue(equipment_id, job_id) is not None:
            raise DomainError("already_queued", "job already waits for this equipment", 409)
        self.db.execute(
            "INSERT INTO equipment_queue(equipment_id,job_id,enqueued_at,enqueue_event_id) VALUES(?,?,?,?)",
            (equipment_id, job_id, occurred_at, event_id))
        return {"equipment_id": equipment_id, "job_id": job_id, "enqueued_at": occurred_at}

    def withdraw(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at, opened_check=False, terminal_check=False)
        episode = self._open_queue(equipment_id, job_id)
        if episode is None:
            raise DomainError("not_queued", "job is not waiting for this equipment", 409)
        duration_seconds(episode["enqueued_at"], occurred_at)
        self.db.execute(
            "UPDATE equipment_queue SET closed_at=?,close_kind='withdrawn',close_event_id=? WHERE id=?",
            (occurred_at, event_id, episode["id"]))
        return {"equipment_id": equipment_id, "job_id": job_id, "withdrawn_at": occurred_at}

    def acquire(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at)
        busy = self.db.execute(
            "SELECT job_id FROM equipment_occupancy WHERE equipment_id=?"
            " AND (released_at IS NULL OR released_at>?) ORDER BY acquired_at LIMIT 1",
            (equipment_id, occurred_at)).fetchone()
        if busy is not None:
            raise DomainError("equipment_busy", f"equipment occupied by {busy['job_id']} at that time", 409)
        down = self.db.execute(
            "SELECT 1 FROM equipment_downtime WHERE equipment_id=?"
            " AND down_at<=? AND (up_at IS NULL OR up_at>?) LIMIT 1",
            (equipment_id, occurred_at, occurred_at)).fetchone()
        if down is not None:
            raise DomainError("equipment_down", "equipment is down at acquire time", 409)
        self.db.execute(
            "INSERT INTO equipment_occupancy(equipment_id,job_id,acquired_at,acquire_event_id) VALUES(?,?,?,?)",
            (equipment_id, job_id, occurred_at, event_id))
        episode = self._open_queue(equipment_id, job_id)
        if episode is not None and episode["enqueued_at"] <= occurred_at:
            self.db.execute(
                "UPDATE equipment_queue SET closed_at=?,close_kind='acquired',close_event_id=? WHERE id=?",
                (occurred_at, event_id, episode["id"]))
        return {"equipment_id": equipment_id, "job_id": job_id, "acquired_at": occurred_at}

    def release(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at, opened_check=False, terminal_check=False)
        row = self.db.execute(
            "SELECT * FROM equipment_occupancy WHERE equipment_id=? AND job_id=? AND released_at IS NULL",
            (equipment_id, job_id)).fetchone()
        if row is None:
            raise DomainError("not_occupied", "job does not hold this equipment", 409)
        seconds = duration_seconds(row["acquired_at"], occurred_at)
        self.db.execute(
            "UPDATE equipment_occupancy SET released_at=?,release_event_id=? WHERE id=?",
            (occurred_at, event_id, row["id"]))
        return {"equipment_id": equipment_id, "job_id": job_id, "released_at": occurred_at,
                "occupied_seconds": seconds}

    def down(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "reason"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        reason = required_text(payload["reason"], "reason")
        self._registered(equipment_id, occurred_at)
        clash = self.db.execute(
            "SELECT 1 FROM equipment_downtime WHERE equipment_id=? AND (up_at IS NULL OR up_at>?) LIMIT 1",
            (equipment_id, occurred_at)).fetchone()
        if clash is not None:
            raise DomainError("already_down", "equipment is already down at that time", 409)
        self.db.execute(
            "INSERT INTO equipment_downtime(equipment_id,reason,down_at,down_event_id) VALUES(?,?,?,?)",
            (equipment_id, reason, occurred_at, event_id))
        return {"equipment_id": equipment_id, "down_at": occurred_at, "reason": reason}

    def up(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        self._registered(equipment_id, occurred_at)
        row = self.db.execute(
            "SELECT * FROM equipment_downtime WHERE equipment_id=? AND up_at IS NULL",
            (equipment_id,)).fetchone()
        if row is None:
            raise DomainError("not_down", "equipment is not down", 409)
        seconds = duration_seconds(row["down_at"], occurred_at)
        self.db.execute("UPDATE equipment_downtime SET up_at=?,up_event_id=? WHERE id=?",
                        (occurred_at, event_id, row["id"]))
        return {"equipment_id": equipment_id, "up_at": occurred_at, "down_seconds": seconds}

    def _slot(self, payload):
        slot_start = timestamp(payload["slot_start"], "slot_start")
        slot_end = timestamp(payload["slot_end"], "slot_end")
        if slot_start >= slot_end:
            raise DomainError("invalid_slot", "slot_start must precede slot_end")
        return slot_start, slot_end

    def _slot_conflict(self, equipment_id, slot_start, slot_end, exclude_id=None):
        sql = ("SELECT job_id FROM equipment_reservations WHERE equipment_id=? AND state='active'"
               " AND slot_start<? AND slot_end>?")
        args = [equipment_id, slot_end, slot_start]
        if exclude_id is not None:
            sql += " AND id!=?"
            args.append(exclude_id)
        return self.db.execute(sql + " ORDER BY slot_start LIMIT 1", args).fetchone()

    def reserve(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id", "slot_start", "slot_end"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at, opened_check=False)
        slot_start, slot_end = self._slot(payload)
        if self.db.execute(
                "SELECT 1 FROM equipment_reservations WHERE equipment_id=? AND job_id=? AND state='active'",
                (equipment_id, job_id)).fetchone():
            raise DomainError("reservation_exists", "job already holds a reservation; use rebook", 409)
        clash = self._slot_conflict(equipment_id, slot_start, slot_end)
        if clash is not None:
            raise DomainError("slot_conflict", f"slot overlaps a reservation by {clash['job_id']}", 409)
        self.db.execute(
            "INSERT INTO equipment_reservations(equipment_id,job_id,slot_start,slot_end,state,created_at,created_event_id)"
            " VALUES(?,?,?,?,'active',?,?)",
            (equipment_id, job_id, slot_start, slot_end, occurred_at, event_id))
        return {"equipment_id": equipment_id, "job_id": job_id,
                "slot_start": slot_start, "slot_end": slot_end}

    def rebook(self, payload, occurred_at, event_id):
        keys(payload, {"equipment_id", "job_id", "slot_start", "slot_end"})
        equipment_id = required_text(payload["equipment_id"], "equipment_id")
        job_id = required_text(payload["job_id"], "job_id")
        self._registered(equipment_id, occurred_at)
        self._job(job_id, occurred_at, opened_check=False)
        slot_start, slot_end = self._slot(payload)
        row = self.db.execute(
            "SELECT * FROM equipment_reservations WHERE equipment_id=? AND job_id=? AND state='active'",
            (equipment_id, job_id)).fetchone()
        if row is None:
            raise DomainError("no_reservation", "job holds no active reservation to move", 409)
        if occurred_at < row["created_at"]:
            raise DomainError("time_order", "rebook precedes the reservation it moves")
        clash = self._slot_conflict(equipment_id, slot_start, slot_end, exclude_id=row["id"])
        if clash is not None:
            raise DomainError("slot_conflict", f"slot overlaps a reservation by {clash['job_id']}", 409)
        self.db.execute(
            "UPDATE equipment_reservations SET state='superseded',superseded_at=?,superseded_event_id=? WHERE id=?",
            (occurred_at, event_id, row["id"]))
        self.db.execute(
            "INSERT INTO equipment_reservations(equipment_id,job_id,slot_start,slot_end,"
            "previous_slot_start,previous_slot_end,state,created_at,created_event_id)"
            " VALUES(?,?,?,?,?,?,'active',?,?)",
            (equipment_id, job_id, slot_start, slot_end,
             row["slot_start"], row["slot_end"], occurred_at, event_id))
        return {"equipment_id": equipment_id, "job_id": job_id, "slot_start": slot_start,
                "slot_end": slot_end, "previous_slot_start": row["slot_start"],
                "previous_slot_end": row["slot_end"]}

    def dispatch(self, kind, payload, occurred_at, event_id):
        if kind == "equipment.register":
            return self.register(payload, occurred_at, event_id)
        if kind == "equipment.enqueue":
            return self.enqueue(payload, occurred_at, event_id)
        if kind == "equipment.withdraw":
            return self.withdraw(payload, occurred_at, event_id)
        if kind == "equipment.acquire":
            return self.acquire(payload, occurred_at, event_id)
        if kind == "equipment.release":
            return self.release(payload, occurred_at, event_id)
        if kind == "equipment.down":
            return self.down(payload, occurred_at, event_id)
        if kind == "equipment.up":
            return self.up(payload, occurred_at, event_id)
        if kind == "equipment.reserve":
            return self.reserve(payload, occurred_at, event_id)
        if kind == "equipment.rebook":
            return self.rebook(payload, occurred_at, event_id)
        raise DomainError("unknown_kind", f"unsupported equipment event {kind}")
