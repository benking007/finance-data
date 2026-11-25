# 金融数据库

使用 SQLite 本地验证 Tushare 数据回补、增量同步和离线查询。相同的采集程序和查询服务支持 MySQL。实施范围及验收依据见随项目保存的方案评审稿。

## 本地管理面板

在项目根目录运行。依赖 Python 3.11 或更新版本。

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m financedb serve
```

浏览器打开 `http://127.0.0.1:8765/#operations`，在「数据运营」输入 Tushare Token。点击「启动全量」即可回补所有已适配接口的历史，页面展示总进度和各分类阶段。再次执行会跳过已提交分片。真实采集库默认在 `data/finance.sqlite3`。

「目标数据库」默认使用本地 SQLite。选 MySQL 时填写主机、端口、数据库名、用户名和密码，并按服务器要求选择 TLS。点击「初始化并使用」后，面板先连接并初始化应用表，再将采集和站内查询切换到该库。如果目标库尚未创建，MySQL 账号需要拥有创建该命名数据库的权限。已有数据、采集进度和定时计划随各自数据库保存。切换目标时请等待正在运行的采集结束。

面板会保存目标类型和 MySQL 连接参数，密码保留在当前服务进程内。服务重启后，可以重新输入密码，或通过部署环境注入 `FINANCEDB_MYSQL_PASSWORD` 来恢复已保存的 MySQL 目标。连接恢复前暂停向该目标采集。

「分类定时增量」按官方数据分类设置启用状态和执行时间，默认时间为北京时间 21:30。每张卡也可手动启动增量，并显示进度、下次运行和上次结果。多个任务到期时依次执行，计划和运行记录保存在数据库。面板输入的 Token 随服务重启清除。需要无人值守时，从部署环境注入 `TUSHARE_TOKEN` 并保持服务运行。面板只接受本机访问，请以单个应用进程运行采集，避免同时运行另一份采集 CLI。上游请求仅由采集进程通过官方 HTTPS 接口发出。

「数据目录」按官方顺序显示 15 个顶级分类和 274 个子项，支持按名称或接口搜索。255 项已有 REST 采集配置，其中包含 4 项仍可查询历史的停更接口。公募基金 9 项均已接入。其余条目为实时、Tick 文件交付、SDK 派生或账户操作等类型。上游注明历史起点时按文档回补，未注明时按配置的早期探测下界请求并核对实际覆盖。真实权限和响应仍需用 Tushare 账号验证。

## SQLite 测试与命令行

演示库是独立的 SQLite 文件，包含生成的行情和统计样本。

```bash
.venv/bin/python -m financedb --database-url sqlite:///data/demo.sqlite3 demo
.venv/bin/python -m financedb --database-url sqlite:///data/demo.sqlite3 serve --port 8766
```

真实数据的命令行操作可在安全环境变量中注入 `TUSHARE_TOKEN` 后运行。目录、状态和 Tick 导入无需该 Token。

```bash
.venv/bin/python -m financedb catalog
.venv/bin/python -m financedb status
.venv/bin/python -m financedb ingest sample --dataset stock_basic --dataset daily
.venv/bin/python -m financedb ingest history --dataset daily --start 2025-01-01 --end 2025-12-31
.venv/bin/python -m financedb ingest incremental --dataset daily
.venv/bin/python -m financedb import-tick /path/to/authorized-tick.csv
```

数据库以数据集、业务键、证券代码和日期索引组织。源字段保留在 JSON 列，精确小数以十进制文本保存。财报等保留已观察到的修订。首次续跑要使用同一库和与原任务一致的参数。全量开始前，按各市场与数据频率对样本测量磁盘、请求和写入量。

## MySQL

需要用命令行直接管理 MySQL 时，将 `FINANCEDB_DATABASE_URL` 置为 `mysql+pymysql://USER:PASSWORD@HOST:3306/DATABASE?charset=utf8mb4`，运行 `.venv/bin/python -m financedb init` 或 `.venv/bin/alembic upgrade head`。命令行使用它自己的连接参数。SQLite 和 MySQL 使用相同逻辑表、字段映射和 API。

迁移时将 `FINANCEDB_TARGET_DATABASE_URL` 指向空库，保持 `FINANCEDB_DATABASE_URL` 指向源库，运行 `.venv/bin/python -m financedb migrate`。迁移读取一致性快照，核对行数和完整表摘要。切换时暂停采集并执行最后一次迁移，核对后修改查询和采集的连接配置。迁移失败可指向新的空库重跑。保留原库作为回退基础。

## 对外数据服务

真实 Tushare 数据的公开展示、查询、下载和下游使用范围以单独许可为准。公开模式只连接独立的发布库。将获授权的数据集和字段写成 JSON 文件，例如 `{"daily":["ts_code","trade_date","close"]}`。在配置独立的 `FINANCEDB_PUBLISH_DATABASE_URL` 后运行 `.venv/bin/python -m financedb build-release --fields approved-fields.json`，从当前私库创建只含这些字段的空发布库快照。

部署时注入以下配置。生产服务使用只读数据库账号，部署于 HTTPS 反向代理之后。

| 配置项 | 用途 |
| --- | --- |
| `FINANCEDB_WEB_MODE=published` | 启用发布模式 |
| `FINANCEDB_PUBLISH_DATABASE_URL` | 独立发布库的只读连接 |
| `FINANCEDB_PUBLISHED_DATASETS` | 允许发布的数据集 ID，逗号分隔 |
| `FINANCEDB_PUBLISHED_FIELDS_JSON` | 与发布库一致的字段授权 JSON |
| `FINANCEDB_API_KEYS_JSON` | 密钥策略，例如 `{"reader":{"key":"<独立长密钥>","datasets":["daily"],"csv":false,"requests_per_minute":60}}` |

启动 `.venv/bin/python -m financedb serve --host 0.0.0.0` 后，以 `X-API-Key` 请求头调用 `GET /v1/datasets`、`GET /v1/datasets/{id}`、`GET /v1/datasets/{id}/rows`。`/docs` 提供分类导航、接口参数、返回字段和请求示例，`/docs/openapi` 提供 Swagger UI，原始描述在 `/openapi.json`。发布端文档根据当前 API 密钥显示获授权的数据集与字段。定期生成新的发布快照并切换连接可更新对外数据。公开模式独立部署，管理面板和采集凭证留在本机管理实例。
