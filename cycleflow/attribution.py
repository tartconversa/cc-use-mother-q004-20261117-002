from collections import defaultdict
from datetime import datetime, timedelta, timezone

from .analytics import window
from .contracts import MissingDependency, duration_seconds, required_text, timestamp


CAUSES = ("downtime", "contention", "reservation_change", "idle_device")


def _parse(value):
    return datetime.fromisoformat(value)


def _iso(moment):
    return moment.isoformat(timespec="seconds")


def _week_splits(start, end):
    """Split a half-open interval into (ISO week label, seconds) parts."""
    cursor = start
    while cursor < end:
        monday = cursor.date() - timedelta(days=cursor.isoweekday() - 1)
        boundary = datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc) + timedelta(days=7)
        seg_end = min(boundary, end)
        yield cursor.strftime("%G-W%V"), int((seg_end - cursor).total_seconds())
        cursor = seg_end


def _overlap_seconds(start, end, intervals):
    total = 0
    for low, high in intervals:
        lo, hi = max(start, low), min(end, high)
        if hi > lo:
            total += int((hi - lo).total_seconds())
    return total


def _classify_segments(t0, t1, downs, occupancies, delays):
    """Classify [t0, t1) into cause segments.

    Priority: downtime (device-level, never a line's fault) > contention
    (another job holds the device) > reservation_change (the job's own slot
    was moved later) > idle_device (device free, unexplained by equipment).
    """
    points = {t0, t1}
    for low, high, _ in (*downs, *occupancies, *delays):
        if low < t1 and high > t0:
            points.add(max(low, t0))
            points.add(min(high, t1))
    ordered = sorted(points)
    segments = []
    for a, b in zip(ordered, ordered[1:]):
        cause = "idle_device"
        blocker = None
        if any(low < b and high > a for low, high, _ in downs):
            cause = "downtime"
        else:
            holders = [meta for low, high, meta in occupancies if low < b and high > a]
            if holders:
                cause = "contention"
                blocker = holders[0]
            elif any(low < b and high > a for low, high, _ in delays):
                cause = "reservation_change"
        segments.append((a, b, cause, blocker))
    return segments


def _serialize_episode(episode):
    by_cause = dict.fromkeys(CAUSES, 0)
    blockers = set()
    segments = []
    for start, end, cause, blocker in episode["segments"]:
        seconds = int((end - start).total_seconds())
        by_cause[cause] += seconds
        if blocker is not None:
            blockers.add(blocker)
        segments.append({"start": _iso(start), "end": _iso(end), "cause": cause,
                         "blocker": blocker, "seconds": seconds})
    return {"equipment_id": episode["equipment_id"], "enqueued_at": episode["enqueued_at"],
            "ended_at": episode["ended_at"], "end_kind": episode["end_kind"],
            "enqueue_event_id": episode["enqueue_event_id"],
            "close_event_id": episode["close_event_id"],
            "seconds": sum(by_cause.values()), "by_cause": by_cause,
            "blockers": sorted(blockers), "segments": segments}


def _serialize_wait(internal):
    episodes = []
    totals = dict.fromkeys(CAUSES, 0)
    blockers = set()
    for episode in internal["episodes"]:
        out = _serialize_episode(episode)
        for cause, seconds in out["by_cause"].items():
            totals[cause] += seconds
        blockers.update(out["blockers"])
        episodes.append(out)
    return {"episodes": episodes, "by_cause": totals,
            "equipment_wait_seconds": sum(totals.values()), "blockers": sorted(blockers)}


def _stage_wait(job, steps):
    if not steps:
        return None
    wait = duration_seconds(job["opened_at"], steps[0]["started_at"])
    for previous, current in zip(steps, steps[1:]):
        if previous["completed_at"]:
            wait += duration_seconds(previous["completed_at"], current["started_at"])
    return wait


class EquipmentAnalytics:
    """Attributes prototype waiting to equipment downtime, reservation moves
    and cross-job contention, without touching stage or test facts."""

    def __init__(self, store):
        self.store = store

    def _load(self, db):
        return {
            "equipment": {row["equipment_id"]: dict(row) for row in db.execute(
                "SELECT * FROM equipment ORDER BY equipment_id")},
            "occupancy": [dict(row) for row in db.execute(
                "SELECT * FROM equipment_occupancy ORDER BY acquired_at,id")],
            "queue": [dict(row) for row in db.execute(
                "SELECT * FROM equipment_queue ORDER BY enqueued_at,id")],
            "downtime": [dict(row) for row in db.execute(
                "SELECT * FROM equipment_downtime ORDER BY down_at,id")],
            "reservations": [dict(row) for row in db.execute(
                "SELECT * FROM equipment_reservations ORDER BY created_at,id")],
            "steps": [dict(row) for row in db.execute(
                "SELECT job_id,stage,started_at,completed_at FROM job_steps ORDER BY started_at,stage")],
            "jobs": {row["job_id"]: dict(row) for row in db.execute(
                "SELECT j.job_id,j.status,j.opened_at,j.completed_at,j.parent_job,"
                "d.product,d.revision FROM jobs j JOIN designs d ON d.design_id=j.design_id")},
        }

    def _job_wait_data(self, data, job_id, clamp):
        episodes = []
        for row in data["queue"]:
            if row["job_id"] != job_id:
                continue
            t0 = _parse(row["enqueued_at"])
            if t0 >= clamp:
                continue
            t1 = _parse(row["closed_at"]) if row["closed_at"] else clamp
            t1 = min(t1, clamp)
            if t1 <= t0:
                continue
            equipment_id = row["equipment_id"]
            downs = [(_parse(d["down_at"]), _parse(d["up_at"]) if d["up_at"] else clamp, None)
                     for d in data["downtime"] if d["equipment_id"] == equipment_id]
            occupancies = [(_parse(o["acquired_at"]),
                            _parse(o["released_at"]) if o["released_at"] else clamp, o["job_id"])
                           for o in data["occupancy"]
                           if o["equipment_id"] == equipment_id and o["job_id"] != job_id]
            delays = [(_parse(r["previous_slot_start"]), _parse(r["slot_start"]), None)
                      for r in data["reservations"]
                      if r["equipment_id"] == equipment_id and r["job_id"] == job_id
                      and r["previous_slot_start"] and r["slot_start"] > r["previous_slot_start"]]
            episodes.append({"equipment_id": equipment_id,
                             "enqueued_at": row["enqueued_at"],
                             "ended_at": row["closed_at"],
                             "end_kind": row["close_kind"] or "open",
                             "enqueue_event_id": row["enqueue_event_id"],
                             "close_event_id": row["close_event_id"],
                             "segments": _classify_segments(t0, t1, downs, occupancies, delays)})
        occupancy = []
        for row in data["occupancy"]:
            if row["job_id"] != job_id:
                continue
            start = _parse(row["acquired_at"])
            if start >= clamp:
                continue
            end = _parse(row["released_at"]) if row["released_at"] else clamp
            end = min(end, clamp)
            down_intervals = [(_parse(d["down_at"]), _parse(d["up_at"]) if d["up_at"] else clamp)
                              for d in data["downtime"] if d["equipment_id"] == row["equipment_id"]]
            occupancy.append({"equipment_id": row["equipment_id"],
                              "acquired_at": row["acquired_at"], "released_at": row["released_at"],
                              "open": row["released_at"] is None,
                              "seconds": int((end - start).total_seconds()),
                              "downtime_overlap_seconds": _overlap_seconds(start, end, down_intervals),
                              "acquire_event_id": row["acquire_event_id"],
                              "release_event_id": row["release_event_id"]})
        reservations = []
        for row in data["reservations"]:
            if row["job_id"] != job_id:
                continue
            reservations.append({"equipment_id": row["equipment_id"],
                                 "slot_start": row["slot_start"], "slot_end": row["slot_end"],
                                 "previous_slot_start": row["previous_slot_start"],
                                 "previous_slot_end": row["previous_slot_end"],
                                 "state": row["state"], "created_at": row["created_at"],
                                 "superseded_at": row["superseded_at"],
                                 "created_event_id": row["created_event_id"],
                                 "superseded_event_id": row["superseded_event_id"]})
        return {"episodes": episodes, "occupancy": occupancy, "reservations": reservations}

    def job_wait(self, job_id, upto=None):
        job_id = required_text(job_id, "job_id")
        if upto is not None:
            upto = timestamp(upto, "upto")
        with self.store.read() as db:
            data = self._load(db)
            job = data["jobs"].get(job_id)
            if job is None:
                raise MissingDependency("job", job_id)
            clamp = _parse(upto) if upto else _parse(
                db.execute("SELECT MAX(occurred_at) FROM events").fetchone()[0])
            internal = self._job_wait_data(data, job_id, clamp)
            wait = _serialize_wait(internal)
            steps = [s for s in data["steps"] if s["job_id"] == job_id]
            return {"job_id": job_id, "status": job["status"], "product": job["product"],
                    "revision": job["revision"], "upto": _iso(clamp),
                    "stage_wait_seconds": _stage_wait(job, steps),
                    "equipment_wait_seconds": wait["equipment_wait_seconds"],
                    "by_cause": wait["by_cause"], "blockers": wait["blockers"],
                    "episodes": wait["episodes"], "occupancy": internal["occupancy"],
                    "reservations": internal["reservations"]}

    def equipment_report(self, product, start, end):
        product = required_text(product, "product")
        start, end = window(start, end)
        clamp = _parse(end)
        window_start = _parse(start)
        with self.store.read() as db:
            data = self._load(db)
            jobs = sorted((j for j in data["jobs"].values()
                           if j["product"] == product and start <= j["opened_at"] < end),
                          key=lambda j: (j["opened_at"], j["job_id"]))
            totals = dict.fromkeys(CAUSES, 0)
            weeks = defaultdict(lambda: {"by_cause": dict.fromkeys(CAUSES, 0),
                                         "total_seconds": 0, "job_ids": set()})
            revisions = defaultdict(lambda: {"by_cause": dict.fromkeys(CAUSES, 0),
                                             "total_seconds": 0, "job_ids": set()})
            device_wait = defaultdict(lambda: dict.fromkeys(CAUSES, 0))
            device_blockers = defaultdict(set)
            used_devices = set()
            job_details = []
            for job in jobs:
                job_id = job["job_id"]
                internal = self._job_wait_data(data, job_id, clamp)
                wait = _serialize_wait(internal)
                steps = [s for s in data["steps"] if s["job_id"] == job_id]
                revision_group = revisions[job["revision"]]
                revision_group["job_ids"].add(job_id)
                for cause, seconds in wait["by_cause"].items():
                    totals[cause] += seconds
                    revision_group["by_cause"][cause] += seconds
                    revision_group["total_seconds"] += seconds
                for episode in internal["episodes"]:
                    equipment_id = episode["equipment_id"]
                    used_devices.add(equipment_id)
                    for seg_start, seg_end, cause, blocker in episode["segments"]:
                        device_wait[equipment_id][cause] += int((seg_end - seg_start).total_seconds())
                        if blocker is not None:
                            device_blockers[equipment_id].add(blocker)
                        for week, week_seconds in _week_splits(seg_start, seg_end):
                            group = weeks[week]
                            group["by_cause"][cause] += week_seconds
                            group["total_seconds"] += week_seconds
                            group["job_ids"].add(job_id)
                for entry in internal["occupancy"]:
                    used_devices.add(entry["equipment_id"])
                for entry in internal["reservations"]:
                    used_devices.add(entry["equipment_id"])
                job_details.append({
                    "job_id": job_id, "revision": job["revision"], "status": job["status"],
                    "opened_at": job["opened_at"], "completed_at": job["completed_at"],
                    "parent_job": job["parent_job"],
                    "stage_wait_seconds": _stage_wait(job, steps),
                    "equipment_wait_seconds": wait["equipment_wait_seconds"],
                    "by_cause": wait["by_cause"], "blockers": wait["blockers"],
                    "blocking_delivery": job["status"] in ("open", "in_progress", "awaiting_test")
                                         and wait["equipment_wait_seconds"] > 0,
                    "episodes": wait["episodes"], "occupancy": internal["occupancy"],
                    "reservations": internal["reservations"]})
            devices = []
            anomalies = []
            for equipment_id in sorted(used_devices):
                info = data["equipment"][equipment_id]
                occupancy_rows = [o for o in data["occupancy"] if o["equipment_id"] == equipment_id]
                downtime_rows = [d for d in data["downtime"] if d["equipment_id"] == equipment_id]
                occupied = _overlap_seconds(window_start, clamp, [
                    (_parse(o["acquired_at"]), _parse(o["released_at"]) if o["released_at"] else clamp)
                    for o in occupancy_rows])
                down_intervals = [(_parse(d["down_at"]), _parse(d["up_at"]) if d["up_at"] else clamp)
                                  for d in downtime_rows]
                open_occupancy = any(o["released_at"] is None for o in occupancy_rows)
                open_downtime = any(d["up_at"] is None for d in downtime_rows)
                if open_occupancy:
                    anomalies.append({"code": "open_occupancy", "subject": equipment_id})
                if open_downtime:
                    anomalies.append({"code": "open_downtime", "subject": equipment_id})
                devices.append({
                    "equipment_id": equipment_id, "station": info["station"], "line": info["line"],
                    "occupied_seconds": occupied,
                    "down_seconds": _overlap_seconds(window_start, clamp, down_intervals),
                    "down_episodes": sum(1 for low, high in down_intervals
                                         if low < clamp and high > window_start),
                    "wait_by_cause": device_wait[equipment_id],
                    "contention_blockers": sorted(device_blockers[equipment_id]),
                    "open_occupancy": open_occupancy, "open_downtime": open_downtime})
            for row in data["queue"]:
                if row["closed_at"] is None and row["equipment_id"] in used_devices:
                    anomalies.append({"code": "open_queue",
                                      "subject": f"{row['equipment_id']}/{row['job_id']}"})
            anomalies.sort(key=lambda item: (item["code"], item["subject"]))
            head = db.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
            return {"product": product, "window_start": start, "window_end": end,
                    "totals": {"attributed_wait_seconds": sum(totals.values()), "by_cause": totals,
                               "jobs_with_equipment_wait": sum(
                                   1 for d in job_details if d["equipment_wait_seconds"] > 0),
                               "job_count": len(job_details)},
                    "weeks": [{"week": week, "by_cause": group["by_cause"],
                               "total_seconds": group["total_seconds"],
                               "job_ids": sorted(group["job_ids"])}
                              for week, group in sorted(weeks.items())],
                    "revisions": [{"revision": revision, "by_cause": group["by_cause"],
                                   "total_seconds": group["total_seconds"],
                                   "job_ids": sorted(group["job_ids"])}
                                  for revision, group in sorted(revisions.items())],
                    "devices": devices, "jobs": job_details, "anomalies": anomalies,
                    "note": "devices[].down_seconds is equipment-level and never allocated to a "
                            "line; by_cause values are job-level wait attributions",
                    "source_head": head}

    def equipment_detail(self, equipment_id):
        equipment_id = required_text(equipment_id, "equipment_id")
        with self.store.read() as db:
            row = db.execute("SELECT * FROM equipment WHERE equipment_id=?", (equipment_id,)).fetchone()
            if row is None:
                raise MissingDependency("equipment", equipment_id)
            detail = dict(row)
            occupancy = [dict(r) for r in db.execute(
                "SELECT * FROM equipment_occupancy WHERE equipment_id=? ORDER BY acquired_at,id",
                (equipment_id,))]
            queue = [dict(r) for r in db.execute(
                "SELECT * FROM equipment_queue WHERE equipment_id=? ORDER BY enqueued_at,id",
                (equipment_id,))]
            downtime = [dict(r) for r in db.execute(
                "SELECT * FROM equipment_downtime WHERE equipment_id=? ORDER BY down_at,id",
                (equipment_id,))]
            reservations = [dict(r) for r in db.execute(
                "SELECT * FROM equipment_reservations WHERE equipment_id=? ORDER BY created_at,id",
                (equipment_id,))]
            current = next((o for o in occupancy if o["released_at"] is None), None)
            down = next((d for d in downtime if d["up_at"] is None), None)
            detail["state"] = "down" if down else ("occupied" if current else "available")
            detail["occupied_by"] = current["job_id"] if current else None
            detail["occupancy"] = occupancy
            detail["queue"] = queue
            detail["downtime"] = downtime
            detail["reservations"] = reservations
            return detail
