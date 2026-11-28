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

在「数据运营」的「Tushare 连接」中输入 Tushare Token。「全库离线」默认回补最近 1 个日历年，截止北京时间昨天。例如在 2026-11-27 开始，任务范围为 2025-11-27 至 2026-11-26。交易日历和当前基础名录作为参考数据同步，目标库中的历史记录继续保留。任务停止或服务中断后，在同一个目标库选择「继续采集」会恢复原日期范围并复用已提交分片。本地 SQLite 默认使用 `data/finance-v2.sqlite3`。

「单表历史回刷」按官方分类逐级选择数据表，同时显示中文名称和英文表名。选表后会显示当前目标库中最早已入库的日期。填写开始和结束日期后可运行单表任务，面板显示分片进度和写入结果。确有需要时，采集器会同步标的名录等依赖。以相同日期再次启动可以复用已提交分片。实际可取日期受各数据源的历史范围约束。

量化因子库的因子数据（`factor_value`）只回补最近 30 个自然日，截止北京时间昨天。单表回刷按与该窗口的交集执行，目标库原有的更早因子数据继续保留。

公告和新闻的近一年采集窗口继续适用于 `anns_d`、`idx_anns`、`news`、`major_news`、`cctv_news` 的增量、样本和单表历史回刷。闰日的上年边界为 2 月 28 日，完全早于窗口的手动回刷会提示范围没有交集。

「目标数据库」默认使用本地 SQLite。选 MySQL 时填写主机、端口、新的空库名称、用户名和密码，并按服务器要求选择 TLS。点击「初始化并使用」后，面板会建好业务表，再将采集和站内查询切换到该库。如果目标库尚未创建，MySQL 账号需要拥有创建该命名数据库的权限。采集进度和定时计划随各自数据库保存。切换目标时请等待正在运行的采集结束。

MySQL 密码默认在服务端加密保存，重启后自动恢复连接。同一套连接参数留空密码即可复用已保存的密码。取消勾选「加密保存密码」并应用配置可清除已保存密码。部署环境中的 `FINANCEDB_MYSQL_PASSWORD` 在启动时优先。加密密钥位于本地 SQLite 文件旁的 `<数据库文件>.credentials.key`，权限为 `0600`。容器部署时需将本地 SQLite 文件及该密钥放在持久化卷中。

「分类定时增量」按官方数据分类设置启用状态和执行时间，默认时间为北京时间 21:30。每张卡也可手动启动增量，并显示进度、下次运行和上次结果。一个增量批次可以和全库离线或单表历史回刷同时运行，各自显示进度并可单独停止。调度检查时已到期的分类合并为一批并行执行，各分类分别保存结果。已有增量批次运行时，新到期的计划在下批执行。勾选「记住 Token」会将凭证加密保存，服务重启后可继续使用。也可在部署环境注入 `TUSHARE_TOKEN` 供服务启动时优先读取。请以单个应用进程运行采集，避免同时运行另一份采集 CLI。上游请求仅由采集进程通过官方 HTTPS 接口发出。

管理员可在「数据走查」选择分类后点击「走查数据」。各表按当前的近期增量窗口或当前快照与 Tushare 比对，显示待新增行数、待更新行数及未核验分片。走查允许和采集同时运行，业务数据和采集检查点保持原状。普通采集空闲后，点击「启动补齐」才会重取并写入确认的差异。若重新取数时差异已变化，对应分片需重新走查。补齐仅写入新增或更新的业务记录，保留本地独有记录。

每个全量任务或增量批次默认最多并行调度 16 个分类，覆盖当前配置的全部可采集分类。每个分类一次执行一个就绪的数据集，同一任务内共用的标的名录等依赖统一调度。分类等候依赖时，同分类中其他已就绪的数据集仍可先执行。`FINANCEDB_INGEST_CATEGORY_WORKERS` 可将分类并发数设为 1 至 16。

每个数据集的独立分片默认最多并行发出 4 个请求，使用 `FINANCEDB_INGEST_WORKERS` 可调整为 1 至 16。分片按计划顺序入库，所有分类和分片共用账户和接口的频率预算。SQLite 写事务按顺序执行，MySQL 可并行写入不同业务表。限制最大分片数的命令行试跑按深度优先顺序逐个请求，以便精确记录尝试范围。

采集按接口配置处理单次返回上限。已确认的日期窗口、标的名录或枚举分类用于继续拆分，分页通过相邻页重叠记录核验。遇到上游忽略偏移、重复返回整页或子分片缺少已观察到的记录时，任务会保留待补齐的检查点。基金列表 `fund_basic` 已按场内外和存续状态拆分。如果触及 15,000 行且分页无法核验，保留可验证的基金记录并标记缺口。美股列表 `us_basic` 按官方从第 1 行开始分页，用整行识别上游记录以保留代码复用和多市场挂牌。 [基金列表](https://tushare.pro/document/2?doc_id=19) · [美股列表](https://tushare.pro/document/2?doc_id=252)

美股复权因子 `us_adjfactor` 单日达到 15,000 行时，先尝试核验相邻页。续页无法核验时，保留触顶样本，并使用已核验的美股和 ETF 名录补采样本里没出现的代码。已采到的代码和交易所组合照常入库，整日标记为待补齐，因为名录和触顶样本无法证明所有证券及挂牌记录均已覆盖。续跑会重试待补齐的日期并复用已完成的检查点。[美股复权因子](https://tushare.pro/document/2?doc_id=402) · [ETF 名录](https://tushare.pro/document/2?doc_id=504)

上市公司公告 `anns_d` 触及 2,000 行时优先尝试分页，使用相邻页重叠记录和新增记录核验偏移是否生效。不能核验分页时，使用 Tushare A 股名录及沪深交易所补充名录逐个代码补采：补充范围包括上交所在市和退市 B 股、深交所在市 B 股，以及深交所暂停、终止上市证券的原始代码。不能用只覆盖 A 股的 `stock_basic` 证明 B 股公告已经采全。[Tushare 股票名录](https://tushare.pro/document/2?doc_id=25) · [公告接口](https://tushare.pro/document/2?doc_id=176)

补充名录从交易所官方 HTTPS 接口读取，逐页核对页码、总数和唯一代码，全部通过后才保存为当日缓存。服务需要能够访问 `query.sse.com.cn` 和 `www.szse.cn`。缓存保存在当前目标库的 `operation_state`，同一北京时间日期的续跑可复用。遇到暂时性故障时会有限重试。若仍未成功，保存已验证的公告样本，等待冷却后由本次任务后续需要名录的分片再次尝试。错误包含交易所、页码和失败原因。交易所缓存不使用或传递 Tushare Token。[上交所列表](https://www.sse.com.cn/assortment/stock/list/share/) · [深交所列表](https://www.szse.cn/market/product/stock/list/index.html) · [深交所暂停、终止上市](https://www.szse.cn/market/stock/suspend/index.html)

升级后会重新核验旧版仅按 A 股拆分的整日检查点，已完成的股票子分片可继续复用。分页通过，或包含交易所补充名录的所有子分片均完成，才标记整日完整。仍不在完整名录中的代码、单个代码仍触顶、来源忽略股票筛选或子分片缺少已观察记录时，保留数据和 `partial` 检查点。已有历史及最近 1 年采集范围保持不变。

柜台债券报价 `bc_otcqt` 单日达到 2,000 行时，尝试核验续页。无法核验时保存已取得的报价，再按触顶样本里已知的银行和证券代码补采。整日仍保留 `partial` 检查点，等待分页通过核验。[柜台债券报价接口](https://tushare.pro/document/2?doc_id=322)

「数据目录」按官方顺序显示 15 个顶级分类和 274 个子项，支持按名称或接口搜索。248 项启用自动离线采集，其中包含 4 项仍可回补历史的停更接口。公募基金 9 项均已接入。其余条目包括停用离线的历史分钟、实时、Tick 文件交付、SDK 派生及账户操作。上游注明历史起点时按文档回补，未注明时按配置的早期探测下界请求并核对实际覆盖。真实权限和响应仍需用 Tushare 账号验证。

A 股、指数、ETF、期货、期权、港股及申万指数的历史分时和分钟 K 线已停用离线采集。全量、增量、走查和补齐会跳过这些数据集。接口文档和已入库记录仍可查询，期货 Tick 可单独导入 CSV。

历史起点优先采用上游公布的覆盖日期。交易或产品有明确的推出时间时，采用有出处的探测下界。期权及部分期货接口分别核对各交易所的推出时间。行情分片使用对应市场的交易日历，仅跳过已确认休市的日期，日历缺失和公告类日期仍保留。

大宗交易、中信行业指数、外汇、港股复权因子等支持日期范围的接口，会先核验区间查询能同时返回两个交易日的数据，并用有数据的区间验证实际探测跨度。历史早段按区间探测。空区间写入可续跑的检查点，出现记录或无法确认范围有效时，恢复原来的逐日或小窗口采集。已有完整检查点可复用，普通增量和限制分片数的命令行试跑沿用原分片方式。只有单月或单日筛选的接口仍按上游支持的粒度采集。

## 采集速度与诊断

默认账号预算为 200 次/分钟，单接口预算为 50 次/分钟。预算不根据 Token 自动识别。高积分账号可在服务的启动环境中配置额度；命令行采集和运营后台使用同一套配置。以下示例适用于已确认拥有相应频次的账号：

```bash
export FINANCEDB_TUSHARE_REQUESTS_PER_MINUTE=500
export FINANCEDB_TUSHARE_ENDPOINT_REQUESTS_PER_MINUTE=500
.venv/bin/python -m financedb serve
```

第一项为账号总预算，第二项为单接口的默认预算。实际还受接口配置中的较低额度约束，例如新闻 400 次/分钟、央行货币政策报告 200 次/分钟、技术因子接口 30 次/分钟及指数公告的每日额度。实际吞吐量还取决于上游响应和数据库写入速度。[官方频次表](https://tushare.pro/document/1?doc_id=290)

管理员登录后可查看同站点 `/ops/state`，每个 `job.datasets` 项的 `metrics` 包括：

| 指标 | 含义 |
| --- | --- |
| `requests`、`response_rows` | 实际上游调用数和响应行数，包含续页、失败请求及重试；响应行数可能包含重复 |
| `scope_requests` | 用于公告代码范围核验的交易所名录请求数，单独于 Tushare 调用数统计 |
| `http_seconds` | 各并发请求的 HTTP 读取和解析耗时之和 |
| `rate_wait_seconds`、`retry_wait_seconds` | 各并发请求的限流检查、等待和重试退避耗时之和 |
| `storage_seconds` | 分片检查、序列化、写库和事务提交的累计时间 |
| `empty_partitions`、`split_partitions` | 本次完成的空分片数、触顶拆分次数 |
| `elapsed_seconds` | 该数据集本次运行的总时间 |

完整全量任务结束后，这些指标随最近全量任务保存。命令行采集的 JSON 结果也包含相同指标。`rate_wait_seconds` 高时核对频次配置；`http_seconds` 高时核对服务器到 Tushare 的延迟及响应体积；`storage_seconds` 高时检查应用到 MySQL 的延迟、磁盘和慢查询。大量空分片需要核对历史探测起点。触及单次上限后的按标的拆分会显著增加请求数，续跑时已有完整分片可跳过。

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
.venv/bin/python -m financedb ingest history
.venv/bin/python -m financedb ingest history --dataset daily --start 2025-01-01 --end 2025-12-31
.venv/bin/python -m financedb ingest incremental --dataset daily
.venv/bin/python -m financedb import-tick /path/to/authorized-tick.csv
```

## 业务表结构

SQLite 和 MySQL 为 256 个可落库的数据集建立独立实体表，表名与数据集 ID 相同，例如 `daily`、`fund_nav` 和 `income`。普通接口与 VIP 接口接入同一数据集时共用实体表，例如 `income_vip` 写入 `income`。网站数据目录的详情可查看对应的业务表名。

业务字段是可在 SQL 中直接检索的列，按数据集配置索引。采集、查询和发布使用这些列，财报等保留已观测到的修订。上市公司公告以 URL、股票代码和公告日期作为业务键。同一键的标题、名称或发布时间等差异完整保存在 `dataset_revisions`，业务表按 `rec_time` 选择发布时间较晚的记录，同时间的差异按内容指纹稳定选取。缺少发布时间时使用公告日期，无法解析的发布时间保留失败检查点。MySQL 的普通小数字段使用 `DECIMAL`，SQLite 使用精确十进制文本，API 返回规范化的十进制字符串。若非空字段无法按其 SQL 类型准确落库，系统记录失败分片以便核对和重试。

`factor_value.factor_value` 在 SQLite 和 MySQL 中均以精确十进制文本保存，支持科学计数法。上游显式缺失或非有限的因子值保存为 SQL `NULL`。ETF 集合竞价仍请求 `float_share` 和 `turnover_rate`。上游省略这两列时，新记录中的对应值为空，已有记录中的对应字段保留原值。上游明确返回的 `null` 照常保存。

沪深 ETF 申赎清单明细保留同一 ETF 和交易日下内容不同的披露记录，全部业务字段相同的行合并。触顶拆分会核验上游样本中的每条可识别记录，失败分片可按原检查点续跑。[沪市申赎清单明细](https://tushare.pro/document/2?doc_id=535) · [深市申赎清单明细](https://tushare.pro/document/2?doc_id=536)

业务表按数据目录的字段清单建表。采集按已配置的业务键写入。有些接口没有稳定的业务键，这些接口按业务字段值去重。Tick 的文件摘要和行范围记录在导入检查点中，Tick 表保留内容相同的原始记录。API 的日期筛选、标的筛选和分页均基于业务字段。

从结构版本 2 升级时，在停止采集和服务、备份数据库后，对每个需要保留的业务库运行 `.venv/bin/python -m financedb init`，也可运行 `.venv/bin/alembic upgrade head`。务必为命令显式指定正在使用的业务库连接。升级逐表核对业务字段并删除旧系统列，其中期货 Tick 表还会删除文件摘要和原始行号。这会保留业务记录、已有修订和采集检查点。同一数据库完成升级后再启动服务。部署独立发布库时，可升级该库或重新生成发布快照。升级前生成的 API 分页游标需要从第一页重新获取。

```sql
SELECT ts_code, trade_date, close
FROM daily
WHERE ts_code = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 20;
```

其余 5 张管理表记录结构版本、行数与覆盖、修订、分片检查点和任务状态。从旧共表版本切换时，请选择新空库初始化并重新采集。旧版结构不支持直接升级。

## MySQL

需要用命令行直接管理 MySQL 时，将 `FINANCEDB_DATABASE_URL` 置为 `mysql+pymysql://USER:PASSWORD@HOST:3306/DATABASE?charset=utf8mb4`，运行 `.venv/bin/python -m financedb init` 或 `.venv/bin/alembic upgrade head`。命令行使用自己的连接参数，网页中保存的目标库设置不会代入。旧库升级需要 `ALTER` 和相关建表权限，MySQL 会逐表执行 DDL，若中断可使用同一连接续跑。业务表需允许无显式主键，服务器的强制主键或自动隐形主键配置会阻止升级。SQLite 和 MySQL 使用相同逻辑表、字段映射和 API。

已有 MySQL 的 `factor_value.factor_value` 为 `DECIMAL` 时，停止采集和服务后，用正在使用的 MySQL 连接执行一次 `financedb init`。此操作将该列改为 `LONGTEXT`，保留业务行和采集检查点。启动新版服务后，在原库上继续采集。SQLite 已使用文本列，可直接续跑。

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
