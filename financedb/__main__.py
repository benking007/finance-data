"""Local service and database utilities for python -m financedb."""

import argparse
import json
import os
from pathlib import Path
import sys

from .storage import DEFAULT_DATABASE_URL, SCHEMA_VERSION, Store


def main():
    parser = argparse.ArgumentParser(prog="financedb")
    parser.add_argument("--database-url", default=os.environ.get("FINANCEDB_DATABASE_URL", DEFAULT_DATABASE_URL))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Initialize or verify the configured database")
    commands.add_parser("catalog", help="Print the official document snapshot")
    commands.add_parser("status", help="Print database coverage")
    commands.add_parser("demo", help="Load synthetic data into a separate SQLite database")
    runs = commands.add_parser("ingest", help="Run one authenticated ingestion job")
    runs.add_argument("mode", choices=["sample", "history", "incremental"])
    runs.add_argument("--dataset", action="append", dest="datasets")
    runs.add_argument("--start")
    runs.add_argument("--end")
    runs.add_argument("--code", action="append", dest="codes")
    runs.add_argument("--max-partitions", type=int)
    tick = commands.add_parser("import-tick", help="Import authorized futures Tick CSV files")
    tick.add_argument("files", nargs="+", type=Path)
    web = commands.add_parser("serve", help="Run the local dashboard and versioned API")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", default=8765, type=int)
    transfer = commands.add_parser("migrate", help="Copy and verify a business-table database into an empty target")
    transfer.add_argument("--target-url", default=os.getenv("FINANCEDB_TARGET_DATABASE_URL"))
    publish = commands.add_parser("build-release", help="Copy approved dataset fields into an empty release database")
    publish.add_argument("--target-url", default=os.getenv("FINANCEDB_PUBLISH_DATABASE_URL"))
    publish.add_argument("--fields", required=True, type=Path)
    args = parser.parse_args()

    try:
        if args.command == "serve":
            import uvicorn
            from .web import create_app
            if os.getenv("FINANCEDB_WEB_MODE", "local") == "local" and args.host not in {"127.0.0.1", "::1", "localhost"}:
                parser.error("本地管理服务只绑定回环地址")
            uvicorn.run(create_app(args.database_url), host=args.host, port=args.port, workers=1)
            return 0
        if args.command == "catalog":
            from .catalog import catalog_snapshot
            result = catalog_snapshot()
        elif args.command in {"migrate", "build-release"}:
            from .transfer import build_release, migrate
            if not args.target_url:
                parser.error("请使用目标数据库环境变量或 --target-url")
            if args.command == "migrate":
                result = migrate(args.database_url, args.target_url)
            else:
                allowed = json.loads(args.fields.read_text(encoding="utf-8"))
                result = build_release(args.database_url, args.target_url, allowed)
        else:
            result = database_command(args, parser)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
        if args.command in {"ingest", "import-tick"} and isinstance(result, dict):
            reports = result.get("datasets", {})
            outcomes = reports.values() if isinstance(reports, dict) else reports
            if any(item.get("status") not in {"complete", "done"} for item in outcomes):
                return 2
        return 0
    except KeyboardInterrupt:
        print("任务已中断。已完成分片可用于续跑。", file=sys.stderr)
        return 130
    except (RuntimeError, ValueError, OSError) as exc:
        message = str(exc)
        token = os.getenv("TUSHARE_TOKEN")
        if token:
            message = message.replace(token, "[已隐藏]")
        print(message, file=sys.stderr)
        return 1


def database_command(args, parser):
    store = Store(args.database_url)
    try:
        store.initialize()
        if args.command == "init":
            return {"backend": store.backend, "schema_version": SCHEMA_VERSION}
        if args.command == "status":
            return {"datasets": store.list_stats(), "recent_partitions": store.list_partitions()}
        from .catalog import load_profiles
        profiles = load_profiles()
        if args.command == "demo":
            from .demo import seed_demo
            return seed_demo(store, profiles)
        if args.command == "import-tick":
            from .ingest import import_tick_csv
            return import_tick_csv(store, profiles["fut_tick"], args.files)
        from .ingest import TushareClient, run_ingestion
        from .ops import dataset_order, runnable
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            parser.error("请在本地安全环境变量或管理面板中设置 TUSHARE_TOKEN")
        available = [key for key, profile in profiles.items() if runnable(profile, args.mode)]
        defaults = [key for key in ("trade_cal", "stock_basic", "daily", "daily_basic", "adj_factor", "income", "stk_mins") if key in available]
        selected = dataset_order(args.datasets or (defaults if args.mode == "sample" else available))
        if set(selected) - set(available):
            parser.error("存在尚未配置的自动采集数据集，请先查看 catalog")
        with TushareClient(token) as client:
            return run_ingestion(store, profiles, selected, client=client, mode=args.mode,
                                 start=args.start, end=args.end, codes=args.codes, max_partitions=args.max_partitions)
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
