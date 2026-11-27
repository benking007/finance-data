"""Compare verified upstream partitions and replay only approved differences."""

from collections import defaultdict
from datetime import datetime
from hashlib import sha256

from sqlalchemy.exc import SQLAlchemyError

from .business_tables import BusinessFieldError, business_tables, project_fields
from .ingest import IngestCancelled, IngestError, Partition, RequestError, fetch_validated_partition, run_ingestion
from .storage import json_text, normalize_date


AUDIT_SCOPE = "按各表的近期增量窗口走查，快照表核对当前快照。结果按已核验分片统计。"
AUDIT_ACTIVE = {"starting", "running", "repairing", "stopping"}


def _changes_digest(profile, differences):
    table = business_tables[profile.get("id") or profile["api_name"]]
    optional = set(profile.get("request", {}).get("optional_response_fields", ()))
    content = sorted(json_text((kind, sorted(optional.difference(row)), project_fields(table, row)))
                     for kind, rows in differences.items() for row in rows)
    return sha256(json_text(content).encode()).hexdigest()


class _Audit:
    def __init__(self, store, profiles, targets, visible, notify):
        self.store = store
        self.notify = notify
        self.results = {key: {"dataset_id": key, "status": "queued", "start": None, "end": None,
                              "missing": 0, "changed": 0, "source_rows": 0,
                              "checked_partitions": 0, "failed_partitions": 0, "message": None}
                        for key in visible}
        self.plan = []
        self._codes = {key: profiles[key].get("code_field") for key in targets}
        self._fields = defaultdict(set)
        self._dated = set()
        self._verified = defaultdict(set)
        self._values = defaultdict(set)
        for key in targets:
            request = profiles[key]["request"]
            for selector in ("code_source", "cap_code_source", "date_code_source", "date_cap_code_source"):
                source = request.get(selector)
                if source in self._codes:
                    field = request.get(f"{selector}_field") or self._codes[source]
                    if field:
                        self._fields[source].add(field)
                    if selector.startswith("date_"):
                        self._dated.add(source)
            for source in request.get("cap_code_sources", ()):
                if self._codes.get(source):
                    self._fields[source].add(self._codes[source])

    def has_source(self, source):
        return source in self._fields

    def has_partition(self, source, partition_id):
        return partition_id in self._verified[source]

    def instrument_values(self, source, field, *, event_date=None):
        return sorted(self._values[source, field or self._codes[source], event_date and event_date.isoformat()])

    def observe(self, profile, partition_id, partition, rows, *, status, source_count):
        dataset_id = profile.get("id") or profile["api_name"]
        # A completed split parent has no response of its own.
        if status == "complete" and not rows and not source_count:
            self._verified[dataset_id].add(partition_id)
            return {"row_count": 0}
        result = self.results.get(dataset_id)
        if result is not None:
            diff = self.store.compare_business_rows(profile, rows)
            counts = {kind: len(diff[kind]) for kind in ("missing", "changed")}
            if any(counts.values()):
                self.plan.append({"dataset_id": dataset_id, "partition_id": partition_id,
                                  "params": partition.params, "code": partition.code, "api_name": partition.api_name,
                                  "start": partition.start.isoformat() if partition.start else None,
                                  "end": partition.end.isoformat() if partition.end else None,
                                  "source_digest": _changes_digest(profile, {"source": rows}),
                                  "digest": _changes_digest(profile, diff)})
            if partition.start is not None:
                start, end = partition.start.date().isoformat(), partition.end.date().isoformat()
                result["start"] = min(result["start"] or start, start)
                result["end"] = max(result["end"] or end, end)
            for kind, count in counts.items():
                result[kind] += count
            result["source_rows"] += source_count
            result["checked_partitions"] += 1
        if self.has_source(dataset_id):
            for row in rows:
                day = normalize_date(row.get(profile.get("date_field"))) if dataset_id in self._dated else None
                for field in self._fields[dataset_id]:
                    value = row.get(field)
                    if value not in (None, ""):
                        self._values[dataset_id, field, None].add(str(value))
                        if day:
                            self._values[dataset_id, field, day].add(str(value))
            self._verified[dataset_id].add(partition_id)
        return {"row_count": len(rows)}

    def progress(self, event):
        result = self.results.get(event["dataset_id"])
        if result is None:
            return
        result["status"] = event["status"]
        result["failed_partitions"] = event["failed"]
        result["message"] = event["message"]
        self.notify(result.copy())


def compare_category(store, profiles, targets, visible, *, client, cancelled, notify):
    audit = _Audit(store, profiles, targets, visible, notify)
    report = run_ingestion(store, profiles, targets, client=client, mode="incremental",
                           cancelled=cancelled, progress=audit.progress, auditor=audit)
    if report["cancelled"]:
        return "cancelled", []
    statuses = {item["status"] for item in audit.results.values()}
    if statuses == {"complete"}:
        return "complete", audit.plan
    return ("partial" if any(item["checked_partitions"] for item in audit.results.values()) else "failed"), audit.plan


def repair_category(store, profiles, plan, *, client, cancelled, notify):
    progress = {"written": 0, "skipped": 0, "failed": 0, "total": len(plan), "processed": 0, "message": None}
    blocked = set()
    for item in plan:
        if cancelled():
            break
        dataset_id = item["dataset_id"]
        if dataset_id in blocked:
            progress["processed"] += 1
            progress["failed"] += 1
            continue
        profile = profiles[dataset_id]
        if not profile.get("offline_enabled", True):
            progress["skipped"] += 1
            progress["processed"] += 1
            progress["message"] = f"{dataset_id} · 已停止离线采集"
            notify(progress.copy())
            continue
        partition = Partition(item["params"], datetime.fromisoformat(item["start"]) if item["start"] else None,
                              datetime.fromisoformat(item["end"]) if item["end"] else None,
                              item["code"], item["api_name"])
        try:
            fetched = fetch_validated_partition(client, profile, partition, cancelled)
            if fetched.verification_error:
                raise fetched.verification_error
            if fetched.capped:
                raise IngestError("该分片已无法验证完整性，请重新走查")
            differences = store.compare_business_rows(profile, fetched.rows)
            if not any(differences.values()):
                if _changes_digest(profile, {"source": fetched.rows}) != item["source_digest"]:
                    raise IngestError("分片的上游记录已变化，请重新走查")
                progress["skipped"] += 1
            elif _changes_digest(profile, differences) != item["digest"]:
                raise IngestError("差异已变化，请重新走查")
            elif not cancelled():
                applied = store.apply_business_rows(profile, [*differences["missing"], *differences["changed"]])
                progress["written"] += applied["inserted"] + applied["updated"]
            else:
                break
        except IngestCancelled:
            break
        except (IngestError, BusinessFieldError, SQLAlchemyError) as exc:
            detail = "目标数据库操作失败" if isinstance(exc, SQLAlchemyError) else str(exc)
            progress["message"] = f"{dataset_id} · {detail}"
            progress["failed"] += 1
            if isinstance(exc, RequestError):
                if exc.invalid_token or exc.network_unavailable:
                    progress["processed"] += 1
                    break
                if exc.stop_dataset:
                    blocked.add(dataset_id)
        progress["processed"] += 1
        notify(progress.copy())
    return progress
