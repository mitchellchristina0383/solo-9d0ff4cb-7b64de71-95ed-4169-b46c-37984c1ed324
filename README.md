# 并发写入观测台（隔离级别沙箱）与库存主备读

给数据库工程师的一个小工具：在改隔离级别之前，先看看**同一组并发写入**在不同隔离级别下会留下什么数据。
另外带一个**库存入口**：更正写主库、拿回执，凭回执查时优先读备库，确认备库已回放这笔提交才返回备库结果。

两个会话（A、B）按可编辑的脚本交错执行 SQL。同一组脚本、同一份初始数据，分别在
**READ COMMITTED** 和 **SERIALIZABLE** 下各跑一轮，页面上并排展示：

- 每一步的执行时间线（完成 / 出错 / 被阻塞 / 解除阻塞）；
- 每个事务的结局（已提交 / 已回滚 / 40001 中止）；
- 两轮事务都结束后，核对查询查到的最终数据。

每步有两种推进方式：

| 模式 | 含义 |
| --- | --- |
| `complete`（默认） | 等这条语句执行完成再推进下一步 |
| `blocked` | 语句在后台执行，等数据库确认它**被另一会话阻塞**（`pg_blocking_pids()`）就推进下一步；语句仍在飞，持锁会话可以继续走完自己的 `COMMIT` |

每轮有超时（默认 5 秒）：超时立即结束本轮，取消卡住的语句并**回滚未提交的事务**。
重跑总是从“初始数据 SQL”重新开始，两轮之间、两次运行之间互不影响。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8080
```

`web` 依赖 `db-primary` 的健康检查，数据库就绪后页面即可用。端口默认 `127.0.0.1:8080`
（本工具不做任何鉴权，会原样执行你贴的 SQL，请勿暴露到公共网络）。

`docker compose up` 会起三个服务（首次启动备库要等 `pg_basebackup` 克隆完成）：

| 服务 | 作用 | 宿主端口 |
| --- | --- | --- |
| `db-primary` | PostgreSQL 16 主库：沙箱库 `sandbox` + 库存库 `inventory` | `127.0.0.1:5432` |
| `db-standby` | 流复制备库（热备只读），页面上的暂停/恢复回放操作它 | `127.0.0.1:5433` |
| `web` | Flask 页面与接口 | `127.0.0.1:8080` |

主库首次初始化时由 `docker/primary-init.sh` 建复制账号 `replicator` 和 `inventory` 库；
备库首次启动由 `docker/standby-entrypoint.sh` 用 `pg_basebackup -R` 克隆主库再以备库身份启动，
之后重启直接沿用数据卷。

## 库存入口：回执 + 主备一致读

页面 `/inventory`（首页顶部有入口）：

1. **更正余量（写主库）**：`UPDATE inventory` 与 `inventory_corrections` 流水在同一事务提交。
   提交成功后取主库 `pg_current_wal_insert_lsn()`（提交记录之后的 WAL 插入位置），
   连同商品、余量、更正记录 id、主库实例标识一起签成 **回执**（HMAC-SHA256）。
2. **凭回执查商品（先备库）**：解码并校验回执 → 连备库 → 等回放 →
   `pg_last_wal_replay_lsn() >= 回执 LSN` 才读库存。连接、等回放、取数**共用同一个时限**
   （页面可设，默认 1 秒，最大 30 秒）。
3. **兜底**：时限到期、备库连不上、目标不是备库、实例标识不符等任何一项不满足，
   都改读主库，页面明确标注 `结果来源：主库` 和未采用备库的原因；
   读到备库时标注 `结果来源：备库` 与等待耗时。

一致性判据的细节：

- 回执 LSN 用**插入位置**而不是 flush 位置。`synchronous_commit=off` 时 flush LSN 可能落后，
  两个不同更正会拿到同一个位点，备库据此可能读到旧值——本地实测复现过，已修正。
- 取数放在一个 `REPEATABLE READ READ ONLY` 事务里：事务内先读 `pg_last_wal_replay_lsn()`，
  再读库存与最近更正 id。只认“读之前已回放到位”，不靠页面展示的瞬时位点。
- 回执带主库 `system_identifier`：主库卷被重建后旧回执不可用于新实例的备库判断
  （实例标识不符 → 回落主库）。
- 页面上的“读到本次更正或之后的值”按**更正流水 id** 判断：结果快照里该商品最新一条更正
  `id >= 回执.correction_id`。回执之后又有别的更正时，读到更新的值同样满足要求。
- 回执签名只防篡改 WAL 位置（避免客户端把“读备库”骗到任意旧位点），不是身份认证；
  `INVENTORY_SECRET` 未设置时每次启动随机生成，重启后旧回执失效（Compose 里给了固定值）。

### 环境变量

| 变量 | 含义 |
| --- | --- |
| `INVENTORY_PRIMARY_HOST/_PORT/_DATABASE/_USER/_PASSWORD` | 主库连接（未设置时回落到 `PGHOST` 等） |
| `INVENTORY_STANDBY_HOST/_PORT/_DATABASE/_USER/_PASSWORD` | 备库连接；一个都没设则视为“未配置备库”，直接读主库 |
| `INVENTORY_SECRET` | 回执签名密钥 |

## 页面与脚本语法

页面上三段可编辑内容：

1. **初始数据 SQL**：每轮开始前在控制连接上执行（可多条语句），负责把库重置到相同起点；
2. **交错脚本**：见下；
3. **核对查询**：两轮事务都结束后，在新连接上执行并展示结果。

交错脚本由步骤组成，每个步骤一行头部 + 一段 SQL：

```sql
@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B blocked
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
COMMIT;
```

- 头部：`@A` 或 `@B`，模式可写 `complete` / `blocked`（也接受 `完成` / `阻塞`），省略时是 `complete`；
- 事务由引擎按本轮隔离级别自动 `BEGIN ISOLATION LEVEL ...`，**脚本里不要写 `BEGIN`**，
  用 `COMMIT` / `ROLLBACK` 结束事务；脚本结束仍未提交的事务会被引擎回滚并记在时间线里；
- 一个步骤里可以写多条语句（结果只展示最后一条的），但建议一步一条，时间线更清晰。

## 内置样例

页面顶部可一键载入：

1. **两人同时撤班（写偏斜）**：`alice`、`bob` 两人在岗，规则是“至少留一人在岗”。
   两人同时撤班：各自确认在岗人数 ≥ 2 才撤。READ COMMITTED 下两笔都成功，
   核对查询显示 `on_duty_count = 0`——**无人值守**；SERIALIZABLE 下后提交者收到
   `40001 could not serialize access`，事务中止，仍有一人在岗。
2. **行锁等待（blocked 模式）**：两个会话更新同一行。B 用 `blocked` 模式确认被 A
   阻塞即推进，让 A 先提交。READ COMMITTED 下 B 在 A 的结果上继续（余额 700）；
   SERIALIZABLE 下 B 解除阻塞时收到 40001（余额 900）。
3. **超时回滚（演示）**：B 用 `complete` 模式等 A 的行锁，而 A 的 `COMMIT` 排在 B 之后
   永远轮不到 → 本轮超时，引擎取消卡住的语句并回滚两个事务，余额保持 1000。
   超时后立刻重跑，仍从初始数据开始。

## 自测

栈起来之后，在容器里跑端到端自测（覆盖：写偏斜两种结局、锁等待的阻塞确认与解除、
超时回滚、超时后重跑、blocked 模式未阻塞分支、脚本解析器）：

```bash
docker compose up -d --build
docker compose exec web python tests/selftest.py
docker compose exec web python tests/inventory_selftest.py
```

`inventory_selftest.py` 覆盖：回执签发/篡改拒绝、LSN 单调递增、备库滞后时回落主库、
恢复后读到备库、暂停期间连续更正、备库断连回落、连接目标是主库时拒绝当备库读。
它通过环境变量取主备连接（容器里已由 Compose 注入）。

也可以对着任意可达的 PostgreSQL 跑：

```bash
pip install -r app/requirements.txt
PGHOST=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py
PYTHONPATH=app INVENTORY_PRIMARY_HOST=... INVENTORY_STANDBY_HOST=... \
  python tests/inventory_selftest.py
```

## 实现要点

- `app/engine.py`：执行引擎。每个会话一条连接（`autocommit=True`，事务边界完全由
  SQL 语句控制），语句在工作线程里执行；`blocked` 模式通过控制连接轮询
  `pg_blocking_pids()` 确认阻塞；超时用 `pg_cancel` 取消在飞语句；收尾先回滚
  “没有在飞语句”的事务（可能借此解除另一会话的阻塞），再等在飞语句结束。
- 事务结局以服务器命令标签为准：`COMMIT` 一个已失败的事务时 PostgreSQL 实际返回
  `ROLLBACK`，页面会如实显示为回滚；`40001` / `40P01` 记为“已中止”。
- `app/script_parser.py`：`@A complete` / `@B blocked` 头部解析，带行号报错。
- `app/main.py`：Flask 页面与接口。所有沙箱实验请求共用一个库，同一时刻只允许
  一轮实验在跑（其余请求收到 409）；`/api/inventory*` 是库存入口。
- `app/inventory.py`：主库更正与回执签发、备库等回放读、暂停/恢复回放、备库状态。
- `app/receipts.py`：回执编解码与签名校验。
- `tests/selftest.py`、`tests/inventory_selftest.py`：上面的端到端自测。

## 限制与说明

- 只支持两个会话；`blocked` 确认是 50ms 轮询，时间线精度有限；
- 引擎**不会**自动重试 40001（重试策略属于应用层，可在脚本里自行表达）；
- 单步可含多条语句，但只展示最后一条的结果；
- 初始数据 SQL 在 `postgres` 超级用户下执行，能力不设限——这是本地沙箱，不是多租户服务；
- 数据不持久化：沙箱库每轮重置；库存库在主备数据卷里，`docker compose down -v` 即清空；
- 主备是演示级配置：无鉴权、复制槽未固定（`wal_keep_size` 兜底）、无故障切换；
  备库故障时需要手动恢复（`docker compose restart db-standby` 或重建数据卷重新克隆）。
- 回执只保证“读到这次更正或之后”，不提供跨商品的全局快照一致性；
  备库结果通过回放位点判断，不作同步复制的强一致承诺。
