"""Private job control, resumable job history, and incremental scheduling."""

from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, time as clock_time, timedelta, timezone
import logging
import os
import re
from threading import Event, RLock, Thread
from time import monotonic
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

from .audit import AUDIT_ACTIVE, AUDIT_SCOPE, compare_category, repair_category
from .catalog import catalog_snapshot, load_profiles
from .ingest import SNAPSHOT_STRATEGIES, RateLimiter, TushareClient, category_worker_count, request_dependencies, run_ingestion
from .storage import timestamp
from .targets import DatabaseTargets

LOCAL_ZONE = ZoneInfo("Asia/Shanghai")
ACTIVE = {"starting", "running", "stopping"}
COMPLETE_DATASET = {"complete", "done", "empty", "skipped"}
INACTIVE_MODES = {"derived", "account", "account_read", "account_write", "write", "private", "file", "file_delivery", "realtime", "live", "pending", "unsupported"}
PREFERRED_ORDER = ("trade_cal", "stock_basic", "stock_basic_history", "index_basic", "fund_basic", "fut_basic", "opt_basic", "hk_basic", "us_basic", "factor_list")
LOGGER = logging.getLogger(__name__)
DEFAULT_SCHEDULE = {
    "enabled": False, "time": "21:30", "last_started_at": None, "last_status": None,
    "last_message": None, "last_target": None, "enabled_since": None, "datasets": None,
}
CATEGORY_OPERATION = "category_incremental"
FULL_JOB_OPERATION = "latest_full_history"
DATA_AUDIT_OPERATION = "latest_data_audit"


def runnable(profile, mode=None):
    if mode == "incremental" and profile.get("archived_source"):
        return False
    strategy = profile.get("request", {}).get("strategy")
    return bool(profile.get("api_name") and strategy in SNAPSHOT_STRATEGIES | {"date", "window", "code_window"}
                and profile.get("mode") not in INACTIVE_MODES)


def dataset_order(dataset_ids):
    order = {dataset_id: i for i, dataset_id in enumerate(PREFERRED_ORDER)}
    return sorted(dataset_ids, key=lambda dataset_id: (order.get(dataset_id, len(order)), dataset_id))


def _date_input(value):
    if not value:
        return None
    try:
        if re.fullmatch(r"[0-9]{8}", value):
            return datetime.strptime(value, "%Y%m%d").date()
        return date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError("日期需使用 YYYY-MM-DD 格式") from exc


class JobManager:
    def __init__(self, database_url, *, client_factory=None, runner=None):
        self.profiles = load_profiles()
        limiter = RateLimiter.from_environment()
        self._client_factory = client_factory or (lambda token: TushareClient(token, limiter=limiter))
        self._runner = runner or run_ingestion
        self._runner_options = {"category_workers": category_worker_count()} if runner is None else {}
        self._lock = RLock()
        self._stop = Event()
        self._wake = Event()
        self._shutdown = Event()
        self._worker = None
        self._audit_stop = Event()
        self._audit_worker = None
        self._category_datasets = {category: [] for category in catalog_snapshot(self.profiles)["categories"]}
        for dataset_id in dataset_order(self.profiles):
            profile = self.profiles[dataset_id]
            if runnable(profile, "incremental"):
                self._category_datasets.setdefault(profile.get("category", "其他"), []).append(dataset_id)
        self._category_datasets = {category: datasets for category, datasets in self._category_datasets.items() if datasets}
        self._targets = DatabaseTargets(database_url)
        try:
            saved_token = self._targets.saved_token()
        except Exception:
            self._targets.close()
            raise
        self._token_saved = saved_token is not None
        self._token = os.environ.get("TUSHARE_TOKEN", "").strip() or saved_token or ""
        try:
            self.__dict__.update(self._read_workspace(self._targets.store))
            self._recover_latest_run()
        except SQLAlchemyError:
            if self._targets.kind != "mysql":
                self._targets.close()
                raise
            self._targets.store.close()
            self._targets.store = None
            self._targets.status = "unavailable"
            self.__dict__.update(self._read_workspace(None))
        self._scheduler = Thread(target=self._watch_schedule, name="finance-incremental-scheduler", daemon=True)
        self._scheduler.start()

    @staticmethod
    def _read_workspace(store):
        schedule = DEFAULT_SCHEDULE.copy()
        category_plans = {"plans": None, "runs": {}}
        full_job = None
        history = []
        audit = {}
        if store is not None:
            schedule = store.read_operation("schedule", schedule)
            category_plans = store.read_operation(CATEGORY_OPERATION, category_plans)
            full_job = store.read_operation(FULL_JOB_OPERATION)
            history = store.read_operation("job_history", history)
            audit = store.read_operation(DATA_AUDIT_OPERATION, {}) or {}
        return {"store": store, "_job": None, "_datasets": {}, "_stats": [], "_stats_at": float("-inf"),
                "_schedule": schedule, "_category_plans": category_plans, "_full_job": full_job,
                "_history": history, "_target_reachable": store is not None,
                "_data_audit": audit.get("report"), "_audit_plan": audit.get("plan", [])}

    def query_store(self):
        with self._lock:
            if self.store is None:
                raise RuntimeError("请先连接并初始化目标数据库")
            return self.store

    def target_state(self):
        with self._lock:
            state = self._targets.state()
            if state["status"] == "ready" and not self._target_reachable:
                state["status"] = "unavailable"
                state["error"] = "目标数据库连接中断，请检查连接并在目标数据库卡片重新连接。"
            return state

    def initialize_target(self, *, kind, mysql=None):
        with self._lock:
            if self._audit_active():
                raise RuntimeError("请先停止当前走查或补齐再切换数据库")
            if self._job and self._job["status"] in ACTIVE:
                raise RuntimeError("请先停止当前采集任务再切换数据库")
            store = None
            previous = None
            try:
                store, settings, encrypted = self._targets.prepare(kind, mysql)
                if store is not self.store:
                    workspace = self._read_workspace(store)
                    previous = {name: getattr(self, name) for name in workspace}
                    self.__dict__.update(workspace)
                    self._recover_latest_run()
                retired = self._targets.activate(kind, store, settings, encrypted)
            except Exception as error:
                if previous is not None:
                    self.__dict__.update(previous)
                if store is not None and store is not self.store and store is not self._targets.local:
                    store.close()
                if isinstance(error, (SQLAlchemyError, OSError)):
                    raise RuntimeError("无法完成目标数据库初始化或保存，请检查数据库连接和权限") from None
                raise
            self._target_reachable = True
            self._stats_at = float("-inf")
            self._wake.set()
            if retired is not None and retired is not store and retired is not self._targets.local:
                try:
                    retired.close()
                except SQLAlchemyError:
                    LOGGER.warning("Former collection database pool could not be closed")
            return self.target_state()

    def close(self):
        with self._lock:
            self._shutdown.set()
            self._stop.set()
            self._audit_stop.set()
            self._wake.set()
            workers = [worker for worker in (self._worker, self._audit_worker) if worker is not None]
        self._scheduler.join(timeout=2)
        for worker in workers:
            if worker.is_alive():
                worker.join(timeout=3)
        if not any(worker.is_alive() for worker in workers):
            self._targets.close()

    def set_token(self, token, *, remember=False):
        token = token.strip()
        if not token or len(token) > 512 or any(ch.isspace() or not ch.isprintable() for ch in token):
            raise ValueError("请输入有效的 Tushare Token")
        with self._lock:
            if self._audit_active():
                raise RuntimeError("请先停止当前走查或补齐")
            if self._job and self._job["status"] in ACTIVE:
                raise RuntimeError("请先停止当前采集任务")
            self._remember_token(token if remember else None)
            self._token = token
            self._wake.set()
        return {"has_token": True}

    def clear_token(self):
        with self._lock:
            if self._audit_active():
                raise RuntimeError("请先停止当前走查或补齐")
            if self._job and self._job["status"] in ACTIVE:
                raise RuntimeError("请先停止当前采集任务")
            self._remember_token(None)
            self._token = ""
        return {"has_token": False}

    def _remember_token(self, token):
        try:
            self._targets.save_token(token)
        except (OSError, SQLAlchemyError):
            raise RuntimeError("无法保存凭证，请检查本地配置库及文件权限") from None
        self._token_saved = token is not None

    def _selected(self, datasets, mode=None):
        available = {key for key, profile in self.profiles.items() if runnable(profile, mode)}
        if datasets is None:
            defaults = available
            if mode == "sample":
                smoke = {"trade_cal", "stock_basic", "daily", "daily_basic", "adj_factor", "income", "stk_mins", "index_daily",
                         "fund_nav", "hk_daily", "us_daily", "cn_cpi", "anns_d", "news", "factor_value"}
                defaults = available & smoke or available
            result = defaults
        else:
            invalid = sorted(set(datasets) - available)
            if invalid:
                raise ValueError(f"以下数据集暂不可自动采集：{', '.join(invalid[:10])}")
            result = set(datasets)
        if not result:
            raise ValueError("请选择至少一个可采集的数据集")
        return dataset_order(result)

    def start_job(self, *, mode, datasets=None, start=None, end=None):
        if mode not in {"sample", "history", "incremental"}:
            raise ValueError("未知采集模式")
        first, last = _date_input(start), _date_input(end)
        if first and last and first > last:
            raise ValueError("开始日期必须早于或等于结束日期")
        with self._lock:
            is_full = mode == "history" and datasets is None and first is None and last is None
            return self._launch(mode, self._selected(datasets, mode), first, last, "manual", is_full=is_full)

    def start_category_job(self, *, category):
        with self._lock:
            scope = self._category_scope(category)
            if not scope:
                raise ValueError("该分类计划中的数据集已变更，请重新保存计划")
            return self._launch_categories({category: scope}, "category_manual")

    def _with_dependencies(self, visible):
        datasets = list(visible)
        included = set(datasets)
        for dataset_id in datasets:
            request = self.profiles[dataset_id]["request"]
            for dependency in request_dependencies(request):
                profile = self.profiles.get(dependency, {})
                if dependency not in included and runnable(profile, "incremental"):
                    included.add(dependency)
                    datasets.append(dependency)
        return dataset_order(datasets)

    def _launch_categories(self, scopes, trigger, *, scheduled_categories=None):
        visible = list(dict.fromkeys(dataset_id for scope in scopes.values() for dataset_id in scope))
        category = next(iter(scopes)) if len(scopes) == 1 else None
        scheduled_target = scheduled_categories.get(category) if scheduled_categories and category else None
        return self._launch("incremental", self._with_dependencies(visible), None, None, trigger,
                            category=category, scheduled_target=scheduled_target,
                            scheduled_categories=scheduled_categories, display_scope=visible)

    def _launch(self, mode, datasets, first, last, trigger, *, category=None, scheduled_target=None,
                scheduled_categories=None, is_full=False, display_scope=None):
        self.query_store()
        if self._shutdown.is_set():
            raise RuntimeError("采集服务正在关闭")
        if not self._token:
            raise ValueError("请先设置 Tushare Token")
        if self._repair_active():
            raise RuntimeError("当前数据补齐结束后可启动采集")
        if self._job and self._job["status"] in ACTIVE:
            raise RuntimeError("已有采集任务正在运行")
        visible = set(datasets if display_scope is None else display_scope)
        entries = {key: {"dataset_id": key, "category": self.profiles[key].get("category", "其他"), "dependency": key not in visible,
                         "status": "queued", "planned": None, "completed": 0, "skipped": 0, "failed": 0, "rows": 0}
                   for key in datasets}
        job = {"id": uuid4().hex, "mode": mode, "status": "starting", "trigger": trigger,
               "started_at": timestamp(), "finished_at": None, "message": None,
               "current_dataset": None, "datasets_total": len(visible), "start": first.isoformat() if first else None,
               "end": last.isoformat() if last else None, "category": category,
               "scheduled_target": scheduled_target, "scheduled_categories": scheduled_categories or {}, "is_full": is_full}
        history = [job.copy(), *self._history[:59]]
        self.store.write_operation("job_history", history)
        self._stop.clear()
        self._job, self._datasets, self._history = job, entries, history
        self._worker = Thread(target=self._run, args=(mode, datasets, first, last, self._token), name="finance-ingestion", daemon=True)
        try:
            self._worker.start()
        except RuntimeError:
            self._job.update(status="failed", finished_at=timestamp(), message="无法启动采集进程。")
            self._history[0] = self._summary()
            self.store.write_operation("job_history", self._history)
            raise
        return deepcopy(self._job)

    def stop_job(self):
        with self._lock:
            if not self._job or self._job["status"] not in ACTIVE:
                raise RuntimeError("当前没有正在运行的采集任务")
            self._stop.set()
            self._job["status"] = "stopping"
            return deepcopy(self._job)

    def _audit_active(self):
        return bool(self._data_audit and self._data_audit["status"] in AUDIT_ACTIVE)

    def _repair_active(self):
        return self._audit_active() and self._data_audit.get("repair") is not None

    def _audit_state(self):
        if self._data_audit is None:
            return None
        eligible = self._data_audit["status"] in {"complete", "partial"} and bool(self._audit_plan)
        return deepcopy(self._data_audit | {"can_repair": eligible})

    def _save_data_audit(self, report, plan):
        self.store.write_operation(DATA_AUDIT_OPERATION, {"report": report, "plan": plan})
        self._data_audit, self._audit_plan = report, plan

    def start_data_audit(self, *, category):
        with self._lock:
            visible = self._category_datasets.get(category)
            if not visible:
                raise ValueError("该分类暂无可走查的数据表")
            results = [{"dataset_id": dataset_id, "status": "queued", "start": None, "end": None,
                        "checked_partitions": 0, "failed_partitions": 0, "source_rows": 0,
                        "missing": 0, "changed": 0, "message": None} for dataset_id in visible]
            report = {"id": uuid4().hex, "category": category, "status": "starting", "scope": AUDIT_SCOPE,
                      "started_at": timestamp(), "finished_at": None, "datasets_total": len(visible),
                      "datasets_checked": 0, "source_rows": 0, "missing": 0, "changed": 0,
                      "results": results, "message": None}
            return self._launch_data_audit(report, [], self._with_dependencies(visible), tuple(visible))

    def start_data_repair(self, audit_id):
        with self._lock:
            if self._data_audit is None or self._data_audit["id"] != audit_id:
                raise RuntimeError("当前目标数据库中没有该次走查，请重新走查")
            if self._job and self._job["status"] in ACTIVE:
                raise RuntimeError("请等待当前采集任务结束再启动补齐")
            if not self._audit_state()["can_repair"]:
                raise RuntimeError("当前走查没有待补齐的已核对差异")
            report = self._data_audit | {"status": "repairing", "message": None, "finished_at": None,
                                        "repair": {"total": len(self._audit_plan), "processed": 0,
                                                   "written": 0, "skipped": 0, "failed": 0, "message": None}}
            return self._launch_data_audit(report, self._audit_plan)

    def _launch_data_audit(self, report, plan, targets=(), visible=()):
        store = self.query_store()
        if not self._token:
            raise ValueError("请先设置 Tushare Token")
        if self._shutdown.is_set():
            raise RuntimeError("采集服务正在关闭")
        if self._audit_active():
            raise RuntimeError("已有走查或补齐任务正在运行")
        try:
            self._save_data_audit(report, plan)
        except (OSError, SQLAlchemyError):
            raise RuntimeError("无法保存走查状态，请检查目标数据库连接") from None
        self._audit_stop.clear()
        worker = Thread(target=self._run_data_audit, args=(store, self._token, targets, visible, plan),
                        name="finance-data-audit", daemon=True)
        self._audit_worker = worker
        try:
            worker.start()
        except RuntimeError:
            failed = report | {"status": "failed", "finished_at": timestamp(), "message": "无法启动走查或补齐。"}
            self._data_audit, self._audit_plan = failed, []
            try:
                self._save_data_audit(failed, [])
            except (OSError, SQLAlchemyError):
                LOGGER.warning("Could not save failed data audit start")
            raise RuntimeError(failed["message"]) from None
        return self._audit_state()

    def stop_data_audit(self, audit_id):
        with self._lock:
            if not self._audit_active() or self._data_audit["id"] != audit_id:
                raise RuntimeError("该走查或补齐任务当前未运行")
            self._audit_stop.set()
            self._data_audit["status"] = "stopping"
            return self._audit_state()

    def _audit_progress(self, progress):
        with self._lock:
            if progress.get("message") and self._token:
                progress["message"] = str(progress["message"]).replace(self._token, "[已隐藏]")[:260]
            if self._repair_active():
                self._data_audit["repair"] = progress
                return
            results = self._data_audit["results"]
            for index, previous in enumerate(results):
                if previous["dataset_id"] == progress["dataset_id"]:
                    results[index] = progress
                    for key in ("source_rows", "missing", "changed"):
                        self._data_audit[key] += progress[key] - previous[key]
                    break
            self._data_audit["datasets_checked"] = sum(item["checked_partitions"] > 0 and
                                                       item["status"] not in {"queued", "running"} for item in results)

    def _run_data_audit(self, store, token, targets, visible, plan):
        repairing = bool(plan)
        status, message = "failed", "数据补齐失败。" if repairing else "数据走查失败。"
        approved = []
        client = None
        with self._lock:
            if not repairing and not self._audit_stop.is_set():
                self._data_audit["status"] = "running"
        try:
            client = self._client_factory(token)
            options = {"client": client, "cancelled": self._audit_stop.is_set, "notify": self._audit_progress}
            if repairing:
                result = repair_category(store, self.profiles, plan, **options)
                self._audit_progress(result)
                if result["failed"]:
                    status, message = "partial", "部分分片未写入，请重新走查后再补齐。"
                else:
                    status, message = "repaired", "已完成本次核对差异的补齐。"
            else:
                status, approved = compare_category(store, self.profiles, targets, visible, **options)
                message = {"complete": "已完成走查，可核对结果后决定是否补齐。",
                           "partial": "部分分片无法核验，可查看明细并补齐已核对的差异。",
                           "failed": "未能核验数据，请查看各表的走查结果。"}.get(status, message)
        except Exception as error:
            LOGGER.warning("Data audit worker failed (%s)", type(error).__name__)
        finally:
            try:
                if client is not None and hasattr(client, "close"):
                    client.close()
            except Exception as error:
                LOGGER.warning("Audit client shutdown failed (%s)", type(error).__name__)
            with self._lock:
                if self._audit_stop.is_set():
                    status = "interrupted" if self._shutdown.is_set() else "cancelled"
                    message = "任务已中止，请重新走查。已有补齐写入会保留。" if repairing else "走查已中止，请重新走查。"
                report = self._data_audit | {"status": status, "message": message, "finished_at": timestamp()}
                for item in report["results"]:
                    if item["status"] in {"queued", "running"}:
                        item["status"] = "unstarted" if item["status"] == "queued" else status
                plan = approved if status in {"complete", "partial"} else []
                try:
                    self._save_data_audit(report, plan)
                except (OSError, SQLAlchemyError):
                    report.update(status="failed", message="无法保存走查状态，请检查目标数据库连接后重新走查。")
                    self._data_audit, self._audit_plan = report, []
                self._stats_at = float("-inf")
                self._wake.set()

    def _progress(self, progress):
        dataset_id = progress.get("dataset_id")
        with self._lock:
            entry = self._datasets.get(dataset_id)
            if entry is None:
                return
            old_status = entry["status"]
            for key in ("planned", "completed", "skipped", "failed", "rows", "message", "metrics"):
                if progress.get(key) is not None:
                    value = progress[key]
                    entry[key] = str(value).replace(self._token, "[已隐藏]")[:260] if key == "message" and self._token else value
            kind = progress.get("event")
            if kind == "dataset_started":
                entry["status"] = "running"
                self._job["current_dataset"] = dataset_id
            elif kind == "dataset_finished":
                entry["status"] = progress.get("status", old_status)
            elif kind == "partition_progress" and progress.get("partition_status") == "blocked":
                entry["status"] = "blocked"

    def _summary(self):
        return self._job.copy()

    def _recover_latest_run(self):
        if self._audit_active():
            report = self._data_audit | {"status": "interrupted", "finished_at": timestamp(),
                                        "message": "服务已中断，请重新走查。"}
            for item in report["results"]:
                if item["status"] in {"queued", "running"}:
                    item["status"] = "unstarted" if item["status"] == "queued" else "interrupted"
            self._save_data_audit(report, [])
        if not self._history:
            return
        latest = self._history[0]
        if latest["status"] in ACTIVE:
            latest.update(status="interrupted", finished_at=timestamp(), message="服务中断。重新启动同一范围可续跑。")
            self.store.write_operation("job_history", self._history)
            if latest.get("trigger") == "schedule":
                self._schedule.update(last_status="interrupted", last_message="服务中断，已提交的分片保留。", last_target=None)
                self.store.write_operation("schedule", self._schedule)
        self._persist_job_views(latest)

    def _save_category_plans(self, data):
        self.store.write_operation(CATEGORY_OPERATION, data)
        self._category_plans = data

    def _persist_job_views(self, job):
        if job.get("is_full") and (self._full_job or {}).get("id") != job["id"]:
            state = job | {"categories": job.get("categories", []), "datasets": job.get("datasets", [])}
            self.store.write_operation(FULL_JOB_OPERATION, state)
            self._full_job = state
        scheduled = job.get("scheduled_categories") or {}
        category = job.get("category")
        targets = scheduled or ({category: job.get("scheduled_target")} if category is not None else {})
        pending = {name: target for name, target in targets.items()
                   if self._category_plans["runs"].get(name, {}).get("id") != job["id"]}
        if not pending:
            return
        data = deepcopy(self._category_plans)
        groups = {group["name"]: group for group in job.get("categories", [])}
        for name, scheduled_target in pending.items():
            progress = groups.get(name)
            status, message = job["status"], job["message"]
            if scheduled and progress and (status not in {"cancelled", "interrupted"} or progress["status"] == "complete"):
                status = progress["status"]
                message = {
                    "complete": "分类采集完成。", "partial": "分类采集已结束，部分数据集有缺口。",
                    "failed": "分类采集未能完成，请查看数据集进度。", "blocked": "分类采集受阻，请查看数据集进度。",
                }.get(status, message)
            target = data["runs"].get(name, {}).get("last_target")
            if scheduled_target is not None and (job["status"] != "interrupted" or status == "complete"):
                target = scheduled_target
            data["runs"][name] = {"id": job["id"], "last_started_at": job["started_at"],
                                  "last_finished_at": job["finished_at"], "last_status": status,
                                  "last_message": message, "last_trigger": job["trigger"],
                                  "last_target": target, "progress": progress}
        self._save_category_plans(data)

    def _run(self, mode, datasets, first, last, token):
        final_status, message = "failed", "采集任务未完成"
        with self._lock:
            if not self._stop.is_set():
                self._job["status"] = "running"
        client = None
        try:
            client = self._client_factory(token)
            result = self._runner(self.store, self.profiles, datasets, client=client, mode=mode, start=first, end=last,
                                  progress=self._progress, cancelled=self._stop.is_set, today=datetime.now(LOCAL_ZONE).date(),
                                  **self._runner_options)
            with self._lock:
                statuses = {entry["status"] for entry in self._datasets.values() if not entry["dependency"]}
            if self._shutdown.is_set() and self._stop.is_set():
                final_status, message = "interrupted", "服务中断，已提交的分片保留。"
            elif self._stop.is_set():
                final_status, message = "cancelled", "任务已暂停，可使用同一范围续跑。"
            elif statuses <= COMPLETE_DATASET:
                final_status, message = "complete", "任务完成。"
            elif statuses & (COMPLETE_DATASET | {"partial"}):
                final_status, message = "partial", "任务已结束，部分数据集有缺口。"
            else:
                final_status, message = "failed", "采集未能完成，请查看数据集进度。"
            if (not self._job.get("category") and not self._job.get("scheduled_categories")
                    and isinstance(result, dict) and result.get("status") in {"complete", "partial", "failed", "cancelled"}
                    and not self._stop.is_set()):
                outcomes = {"complete": "任务完成。", "partial": "任务已结束，部分数据集有缺口。",
                            "failed": "采集未能完成，请查看数据集进度。", "cancelled": "任务已暂停，可使用同一范围续跑。"}
                final_status = result["status"]
                message = outcomes[final_status]
        except Exception as exc:
            message = f"任务发生错误（{type(exc).__name__}）。"
        finally:
            try:
                if client is not None and hasattr(client, "close"):
                    client.close()
            finally:
                with self._lock:
                    for entry in self._datasets.values():
                        if entry["status"] in {"queued", "running"}:
                            entry["status"] = "unstarted" if entry["status"] == "queued" else final_status
                    self._job.update(status=final_status, message=message, finished_at=timestamp(), current_dataset=None)
                    self._stats_at = float("-inf")
                    job_state = self._job_state()
                    self._history[0] = self._summary()
                    if self._job.get("is_full") or self._job.get("category") or self._job.get("scheduled_categories"):
                        self._history[0]["categories"] = job_state["categories"]
                    self.store.write_operation("job_history", self._history)
                    self._persist_job_views(job_state)
                    if self._job["trigger"] == "schedule":
                        self._schedule.update(last_status=final_status, last_message=message)
                        if final_status == "interrupted":
                            self._schedule["last_target"] = None
                        self.store.write_operation("schedule", self._schedule)
                    self._wake.set()

    def set_schedule(self, *, enabled, time, datasets=None):
        self._validate_schedule(enabled, time)
        with self._lock:
            self.query_store()
            selected = self._selected(datasets, "incremental") if datasets is not None else self._schedule.get("datasets")
            plan = self._configured_plan(self._schedule, enabled, time) | {"datasets": selected}
            self.store.write_operation("schedule", plan)
            self._schedule = plan
            if self._category_plans["plans"] is not None:
                self._save_category_plans(self._category_plans | {"plans": self._inherited_category_plans()})
            self._wake.set()
            return self._schedule_state(datetime.now(LOCAL_ZONE), self._schedule)

    @staticmethod
    def _validate_schedule(enabled, time):
        if not isinstance(enabled, bool) or not isinstance(time, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", time):
            raise ValueError("请输入 00:00 至 23:59 的计划时间")

    @staticmethod
    def _configured_plan(plan, enabled, time):
        since = plan.get("enabled_since")
        if enabled and (not plan["enabled"] or time != plan["time"]):
            since = timestamp()
        return plan | {"enabled": enabled, "time": time, "enabled_since": since}

    def _inherited_category_plans(self):
        selected = self._schedule.get("datasets")
        scoped = set(selected) if selected is not None else None
        plans = {}
        for category, datasets in self._category_datasets.items():
            inherited = [dataset for dataset in datasets if scoped is None or dataset in scoped]
            plans[category] = DEFAULT_SCHEDULE.copy()
            if inherited:
                plans[category].update(self._schedule, datasets=inherited if scoped is not None else None)
        return plans

    def _plans_for_display(self):
        inherited = self._inherited_category_plans()
        if self._category_plans["plans"] is None:
            return inherited
        saved = self._category_plans["plans"]
        return {category: saved.get(category, DEFAULT_SCHEDULE.copy()) for category in inherited}

    def _category_scope(self, category, plan=None):
        if category not in self._category_datasets:
            raise ValueError("该分类暂无可自动采集的数据集")
        if plan is None:
            plan = self._plans_for_display()[category]
        allowed = self._category_datasets[category]
        selected = plan.get("datasets")
        if selected is None:
            return allowed.copy()
        return [dataset for dataset in allowed if dataset in selected]

    def set_category_schedule(self, *, category, enabled, time):
        self._validate_schedule(enabled, time)
        with self._lock:
            self.query_store()
            scope = self._category_scope(category)
            plans = self._plans_for_display()
            if not scope:
                plans[category] = plans[category] | {"datasets": None}
            plans[category] = self._configured_plan(plans[category], enabled, time)
            self._save_category_plans(self._category_plans | {"plans": plans})
            self._wake.set()
            return self._category_state(category, plans[category], datetime.now(LOCAL_ZONE), self._job_state())

    def _target(self, now, plan):
        return datetime.combine(now.date(), clock_time.fromisoformat(plan["time"]), LOCAL_ZONE)

    def _due(self, now, target, plan):
        since = plan.get("enabled_since")
        last_target = plan.get("last_target") or ""
        return (now >= target and last_target < target.date().isoformat()
                and (not since or datetime.fromisoformat(since.replace("Z", "+00:00")) <= target))

    def _due_category_target(self, now, plan):
        target = self._target(now, plan)
        if now < target and plan.get("enabled_since"):
            target -= timedelta(days=1)
        return target if plan["enabled"] and self._due(now, target, plan) else None

    def _schedule_state(self, now, plan):
        result = deepcopy(plan)
        target = self._target(now, plan)
        due = self._due(now, target, plan)
        next_run = target + timedelta(days=int(now >= target and not due))
        result.update(timezone=LOCAL_ZONE.key, next_run=next_run.isoformat() if result["enabled"] else None)
        if result["enabled"] and due and not self._token:
            result.update(last_status="waiting_token", last_message="当前到期任务等待设置 Tushare Token。")
        return result

    def _effective_category_plan(self, category, plan):
        run = self._category_plans["runs"].get(category, {})
        targets = [value for value in (plan.get("last_target"), run.get("last_target")) if value is not None]
        return plan | {"last_target": max(targets, default=None)}

    def _category_state(self, category, plan, now, job):
        run = self._category_plans["runs"].get(category, {})
        effective = self._effective_category_plan(category, plan)
        state = self._schedule_state(now, effective) | {
            "category": category, "datasets_total": len(self._category_scope(category, plan)),
            "last_finished_at": run.get("last_finished_at"), "last_trigger": run.get("last_trigger"),
            "progress": run.get("progress"), "queued": False,
        }
        if run:
            state.update({field: run[field] for field in ("last_started_at", "last_status", "last_message")})
        target = self._due_category_target(now, effective)
        active = job is not None and job["status"] in ACTIVE
        scheduled = job.get("scheduled_categories", {}) if active else {}
        owns_job = active and (job.get("category") == category or category in scheduled)
        if target is not None:
            state.update(next_run=target.isoformat(), queued=bool(self._token and not owns_job))
            if not self._token:
                state.update(last_status="waiting_token", last_message="当前到期任务等待设置 Tushare Token。")
        if owns_job:
            progress = next((group for group in job["categories"] if group["name"] == category), None)
            status = job["status"]
            if progress and status == "running":
                status = progress["status"]
            state.update(last_started_at=job["started_at"], last_finished_at=None, last_status=status,
                         last_message=job["message"], last_trigger=job["trigger"], progress=progress)
            dependency = job.get("current_dependency") if job.get("category") == category else None
            if dependency and job["status"] != "stopping":
                state["last_message"] = f"正在同步采集依赖：{dependency}。"
            if category in scheduled or job.get("scheduled_target"):
                next_run = self._target(now, effective)
                if next_run <= now:
                    next_run += timedelta(days=1)
                state["next_run"] = next_run.isoformat() if plan["enabled"] else None
        return state

    def _watch_schedule(self):
        while not self._shutdown.is_set():
            try:
                self._check_schedule()
            except Exception as exc:
                LOGGER.warning("Incremental schedule check failed (%s)", type(exc).__name__)
            self._wake.wait(10)
            self._wake.clear()

    def _check_schedule(self):
        with self._lock:
            if (self._shutdown.is_set() or self.store is None or not self._token or self._repair_active()
                    or (self._job and self._job["status"] in ACTIVE)):
                return
            now = datetime.now(LOCAL_ZONE)
            if self._category_plans["plans"] is not None:
                scopes = {}
                scheduled = {}
                for category, plan in self._plans_for_display().items():
                    target = self._due_category_target(now, self._effective_category_plan(category, plan))
                    scope = self._category_scope(category, plan)
                    if target is not None and scope:
                        scopes[category] = scope
                        scheduled[category] = target.date().isoformat()
                if scopes:
                    self._launch_categories(scopes, "category_schedule", scheduled_categories=scheduled)
                return
            target = self._target(now, self._schedule)
            if not self._schedule["enabled"] or not self._due(now, target, self._schedule):
                return
            datasets = self._selected(self._schedule.get("datasets"), "incremental")
            self._launch("incremental", datasets, None, None, "schedule")
            self._schedule.update(last_target=target.date().isoformat(), last_started_at=timestamp(), last_status="running", last_message=None)
            self.store.write_operation("schedule", self._schedule)

    def _job_state(self):
        if self._job is None:
            return None
        categories = defaultdict(list)
        visible = [item for item in self._datasets.values() if not item["dependency"]]
        for item in visible:
            categories[item["category"]].append(item)
        summary = []
        active = self._job["status"] in ACTIVE
        for name, entries in categories.items():
            finished = sum(entry["status"] in COMPLETE_DATASET for entry in entries)
            planned = [entry["planned"] for entry in entries]
            states = {entry["status"] for entry in entries}
            current_dataset = next((entry["dataset_id"] for entry in entries if entry["status"] == "running"), None)
            if states <= COMPLETE_DATASET:
                status = "complete"
            elif active and states & {"queued", "running"}:
                status = "running" if current_dataset else "queued"
            elif self._job["status"] in {"cancelled", "interrupted"} and states <= {"cancelled", "interrupted", "unstarted"}:
                status = self._job["status"]
            elif states <= {"blocked", "unstarted"}:
                status = "blocked"
            elif finished or "partial" in states:
                status = "partial"
            else:
                status = "failed"
            waiting_for = set()
            if status == "queued":
                for entry in entries:
                    if entry["status"] != "queued":
                        continue
                    request = self.profiles[entry["dataset_id"]].get("request", {})
                    waiting_for.update(dependency for dependency in request_dependencies(request)
                                       if dependency in self._datasets and self._datasets[dependency]["status"] in {"queued", "running"})
            summary.append({"name": name, "datasets_total": len(entries), "datasets_finished": finished,
                            "partitions_done": sum(entry["completed"] + entry["skipped"] for entry in entries),
                            "partitions_failed": sum(entry["failed"] for entry in entries), "rows": sum(entry["rows"] for entry in entries),
                            "partitions_total": sum(planned) if all(number is not None for number in planned) else None,
                            "status": status, "current_dataset": current_dataset,
                            "waiting_for": sorted(waiting_for)})
        current = self._job["current_dataset"]
        if not current or self._datasets[current]["status"] != "running":
            current = next((entry["dataset_id"] for entry in self._datasets.values() if entry["status"] == "running"), None)
        dependency = current if current and self._datasets[current]["dependency"] else None
        return deepcopy(self._job | {"categories": summary, "datasets": visible,
                                     "current_dataset": current, "current_dependency": dependency})

    def state(self):
        with self._lock:
            if self.store is not None and monotonic() - self._stats_at >= 10:
                try:
                    self._stats = self.store.list_stats()
                    self._target_reachable = True
                except SQLAlchemyError:
                    self._stats = []
                    self._target_reachable = False
                self._stats_at = monotonic()
            now = datetime.now(LOCAL_ZONE)
            job = self._job_state()
            full_job = job if job and job.get("is_full") else deepcopy(self._full_job)
            category_schedules = [self._category_state(category, plan, now, job) for category, plan in self._plans_for_display().items()]
            return {"has_token": bool(self._token), "token_saved": self._token_saved, "job": job, "full_job": full_job,
                    "schedule": self._schedule_state(now, self._schedule), "category_schedules": category_schedules,
                    "history": deepcopy(self._history), "database": deepcopy(self._stats), "target": self.target_state(),
                    "data_audit": self._audit_state(), "data_audit_categories": list(self._category_datasets)}
