# 金融数据库

使用 SQLite 本地验证 Tushare 数据回补、增量同步和离线查询。相同的采集程序和查询服务支持 MySQL。实施范围及验收依据见随项目保存的方案评审稿。

## 运营后台

本地服务启动后，使用同一个站点打开[数据运营](http://127.0.0.1:8765/operations)、[数据目录](http://127.0.0.1:8765/)和 [API 文档](http://127.0.0.1:8765/docs)。首页和 API 文档的导航也提供「数据运营」入口。

在项目根目录运行。依赖 Python 3.11 或更新版本。

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m financedb serve
```

新站首次启动后打开 [注册](http://127.0.0.1:8765/register)，先创建管理员账号。第一个注册账号自动成为管理员，后续注册为普通账号。管理员登录后可以进入「数据运营」和「用户管理」。

在「数据运营」的「Tushare 连接」中输入 Tushare Token。点击「启动全量」即可回补所有已适配接口的历史，页面展示总进度和各分类阶段。任务停止或服务中断后，连接同一个目标库并选择「继续采集」，可跳过已提交分片。本地 SQLite 默认使用 `data/finance-v2.sqlite3`。

「目标数据库」默认使用本地 SQLite。选 MySQL 时填写主机、端口、新的空库名称、用户名和密码，并按服务器要求选择 TLS。点击「初始化并使用」后，面板会建好业务表，再将采集和站内查询切换到该库。如果目标库尚未创建，MySQL 账号需要拥有创建该命名数据库的权限。采集进度和定时计划随各自数据库保存。切换目标时请等待正在运行的采集结束。

MySQL 密码默认在服务端加密保存，重启后自动恢复连接。同一套连接参数留空密码即可复用已保存的密码。取消勾选「加密保存密码」并应用配置可清除已保存密码。部署环境中的 `FINANCEDB_MYSQL_PASSWORD` 在启动时优先。加密密钥位于本地 SQLite 文件旁的 `<数据库文件>.credentials.key`，权限为 `0600`。容器部署时需将本地 SQLite 文件及该密钥放在持久化卷中。

「分类定时增量」按官方数据分类设置启用状态和执行时间，默认时间为北京时间 21:30。每张卡也可手动启动增量，并显示进度、下次运行和上次结果。多个任务到期时依次执行，计划和运行记录保存在数据库。勾选「记住 Token」会将凭证加密保存，服务重启后可继续使用。也可在部署环境注入 `TUSHARE_TOKEN` 供服务启动时优先读取。请以单个应用进程运行采集，避免同时运行另一份采集 CLI。上游请求仅由采集进程通过官方 HTTPS 接口发出。

「数据目录」按官方顺序显示 15 个顶级分类和 274 个子项，支持按名称或接口搜索。255 项已有 REST 采集配置，其中包含 4 项仍可查询历史的停更接口。公募基金 9 项均已接入。其余条目为实时、Tick 文件交付、SDK 派生或账户操作等类型。上游注明历史起点时按文档回补，未注明时按配置的早期探测下界请求并核对实际覆盖。真实权限和响应仍需用 Tushare 账号验证。

## 账号与每日额度

用户可在 `/register` 注册、在 `/login` 登录，在「我的账户」查看今日已用额度和剩余额度。每个账号可生成一个有效 API Key。重置后旧密钥立即失效，新密钥在生成时完整显示。API 客户端通过 `X-API-Key` 请求头传递密钥，浏览器登录后可直接使用数据目录和 API 文档。

每个账号默认每日 3,000 次调用，登录会话和 API Key 共用同一个计数。北京时间每天 00:00 开始计算新一天，超额时返回 HTTP 429 和 `Retry-After`。管理员可在「用户管理」调整每个账号的每日额度。账户和额度保存在独立的 `data/accounts.sqlite3`，可用 `FINANCEDB_ACCOUNTS_PATH` 指定持久化路径。

部署在自有域名时，让服务继续监听 `127.0.0.1:8765`，在服务环境中设置 `FINANCEDB_SITE_ORIGIN=https://finance.example.com`，并用可信 HTTPS 反向代理把同域请求转发到该端口。代理须保留原始 `Host`。初次配置应先通过站点注册管理员，再开放给其他用户。

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

## 业务表结构

SQLite 和 MySQL 为 256 个可落库的数据集建立独立实体表，表名与数据集 ID 相同，例如 `daily`、`fund_nav` 和 `income`。普通接口与 VIP 接口接入同一数据集时共用实体表，例如 `income_vip` 写入 `income`。网站数据目录的详情可查看对应的业务表名。

业务字段是可在 SQL 中直接检索的列，按数据集配置索引。MySQL 的小数字段使用 `DECIMAL`，SQLite 使用精确十进制文本。原始返回值保存在各自表的 `_payload` 列，财报等保留已观测到的修订。

```sql
SELECT ts_code, trade_date, close
FROM daily
WHERE ts_code = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 20;
```

其余 5 张管理表记录结构版本、行数与覆盖、修订、分片检查点和任务状态。从旧共表版本切换时，请选择新空库初始化并重新采集。旧版结构不支持直接升级。

## MySQL

需要用命令行直接管理 MySQL 时，将 `FINANCEDB_DATABASE_URL` 置为 `mysql+pymysql://USER:PASSWORD@HOST:3306/DATABASE?charset=utf8mb4`，运行 `.venv/bin/python -m financedb init` 或 `.venv/bin/alembic upgrade head`。命令行使用它自己的连接参数。SQLite 和 MySQL 使用相同逻辑表、字段映射和 API。

在支持分表的同版本数据库之间复制时，将 `FINANCEDB_TARGET_DATABASE_URL` 指向空库，保持 `FINANCEDB_DATABASE_URL` 指向源库，运行 `.venv/bin/python -m financedb migrate`。复制读取一致性快照，核对行数和表摘要。切换时暂停采集并执行最后一次复制，核对后修改查询和采集的连接配置。复制失败可指向新的空库重跑。

## 对外数据服务

真实 Tushare 数据的公开展示、查询、下载和下游使用范围以单独许可为准。公开模式只连接独立的发布库。将获授权的数据集和字段写成 JSON 文件，例如 `{"daily":["ts_code","trade_date","close"]}`。在配置独立的 `FINANCEDB_PUBLISH_DATABASE_URL` 后运行 `.venv/bin/python -m financedb build-release --fields approved-fields.json`，从当前私库创建只含这些字段的空发布库快照。

部署时注入以下配置。生产服务使用只读数据库账号，部署于 HTTPS 反向代理之后。

| 配置项 | 用途 |
| --- | --- |
| `FINANCEDB_WEB_MODE=published` | 启用发布模式 |
| `FINANCEDB_PUBLISH_DATABASE_URL` | 独立发布库的只读连接 |
| `FINANCEDB_PUBLISHED_DATASETS` | 允许发布的数据集 ID，逗号分隔 |
| `FINANCEDB_PUBLISHED_FIELDS_JSON` | 与发布库一致的字段授权 JSON |
| `FINANCEDB_ACCOUNTS_PATH` | 账号与每日额度的独立数据库路径，默认 `data/accounts.sqlite3` |
| `FINANCEDB_SITE_ORIGIN` | 通过 HTTPS 反向代理提供服务时填写站点源，例如 `https://finance.example.com` |
| `FINANCEDB_API_KEYS_JSON` | 为已有集成提供的可选静态密钥策略，例如 `{"reader":{"key":"<独立长密钥>","datasets":["daily"],"csv":false,"requests_per_minute":60}}` |

启动 `.venv/bin/python -m financedb serve` 后，以 `X-API-Key` 请求头调用 `GET /v1/datasets`、`GET /v1/datasets/{id}`、`GET /v1/datasets/{id}/rows`。`/docs` 提供分类导航、接口参数、返回字段和请求示例，`/docs/openapi` 提供 Swagger UI，原始描述在 `/openapi.json`。发布模式也支持注册、登录、用户管理和每日额度。发布端只提供发布库授权的字段和数据集，采集运营后台随本地管理模式部署。定期生成新的发布快照并切换连接可更新对外数据。
