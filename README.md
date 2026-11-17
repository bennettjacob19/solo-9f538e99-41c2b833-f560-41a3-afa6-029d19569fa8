# TCP 延迟代理（慢链路“上传结束收不到回执”复现工具）

带网页的 TCP 延迟代理：可配置目标地址与上下行延迟，网页上直接上传文件试传，
实时显示每条连接的回执、双向字节数与缓冲峰值。用于在本地复现慢链路下
“上传结束却收不到回执”的问题。

另提供**按连接抓取转发轨迹**：开启抓取后，新建连接会逐块记录代理实际转发的
上下行字节（含相对时间）、EOF 与半关闭顺序、异常结束原因，并可按连接下载
JSONL 轨迹。抓取与转发隔离，抓取出问题不会影响上传与回执。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

* 网页 / API：http://localhost:8000
* TCP 延迟代理监听：`localhost:7000`（可直接对接设备或客户端）
* 示例接收端：容器网络内 `receiver:9000`（先发欢迎行，读到 EOF 后回
  `RECEIPT bytes=<n> sha256=<hex>`）

网页上可修改监听地址/端口、目标地址、上下行延迟（毫秒），保存后对**之后建立
的连接**生效；已建立的连接沿用建立时的配置。修改监听地址/端口会重启监听器。

## 抓取转发轨迹

网页“抓取开关”打开后，**之后新建**的连接开始记录；已建立的连接沿用建立时的
设置（开关状态在 accept 时快照，与配置快照一致）。轨迹内容包括：

* `start`：连接 id、客户端地址、目标、上下行延迟、限制值；
* `data`：代理**实际写出**的每个数据块（`dir` 为 `up`/`down`，`b64` 为原始
  字节，`t` 为相对连接建立的秒数）；
* `event`：`eof_received`、`half_close_forwarded`（及方向）、异常；
* `end`：结束记录，含 `complete`、状态、原因、累计字节、半关闭顺序。

每条连接可在网页上下载，或 `GET /api/connections/{id}/trace`。

**隔离与完整性保证：**

* 转发协程只把记录放入内存队列，**从不等待磁盘**；上传与回执不受抓取影响。
* 每连接待写抓取数据上限 **64 KiB**（另有 ≤ 4 KiB 控制记录缓冲）；磁盘变慢
  导致缓冲满时停止抓取、轨迹标记不完整并说明原因。
* 每连接轨迹上限 **1 MiB**（按落盘 JSON 字节），超过即停止抓取并标记不完整。
* 写盘失败、轨迹目录不可用只影响轨迹，转发照常，轨迹标记不完整并说明原因。
* 正常连接必须等全部记录落盘、写入结束记录、`fsync` 并原子改名后才显示为
  **完整**；下载接口对仍在转发的连接返回 409。
* **不完整轨迹始终以 `.jsonl.partial` 命名**；磁盘上的 `.jsonl` 一定带有
  `complete=true` 的结束记录。断连（RST）或停机时留下的只会是 `.partial`
  或带 `complete=false` 尾记录的文件，不会被误标为完整。

轨迹文件为 JSON Lines（UTF-8）。容器内落盘到 `TRACE_DIR`（Compose 默认挂载
命名卷 `proxy-traces:/data/traces`）。

## 复现“慢链路收不到回执”

1. 把上行/下行延迟调大（例如各 2000 ms）；
2. 在“上传试传”里选一个文件开始试传；
3. 在“活动连接”里观察：上行缓冲被 64 KiB 上限顶住、EOF 等待队列冲刷后才
   半关闭、回执（下行尾部）在延迟后才到达——上传“结束”与回执到达之间的
   时间差即慢链路效应。

## 关键语义（实现于 `proxy/app/engine.py`）

* **延迟起算**：每个数据块的投递时刻 = 代理读到它的时刻 + 延迟；队列按
  FIFO 投递，连续数据流水通过延迟线，不按读取块数累加等待。
* **64 KiB 应用缓冲**：每方向“已读未写”字节数上限 64 KiB，满则暂停该向
  读取（反向与别的连接不受影响）。
* **半关闭**：收到单向 EOF 后，先把该向待发字节全部写出，再 `write_eof`
  转发半关闭；反向继续传输直到结束。
* **断连释放**：正常结束或对端 RST 都会取消泵任务、清空队列、关闭两侧
  连接，并把连接移入历史表。
* **配置快照**：每条连接在 accept 时快照配置，整条连接沿用。

## 本地开发（不用 Docker）

```bash
pip install -r proxy/requirements.txt
PORT=19000 python3 receiver/server.py &
cd proxy && LISTEN_PORT=17000 TARGET_HOST=127.0.0.1 TARGET_PORT=19000 \
  UP_DELAY_MS=300 DOWN_DELAY_MS=300 \
  uvicorn app.main:app --host 127.0.0.1 --port 18000
```

## 自测

```bash
# 1) 端到端（需按上面方式起好接收端与代理，端口见脚本头注释）
python3 tests/selftest.py
# 2) 抓取轨迹端到端（同上环境）
python3 tests/trace_selftest.py
# 3) 抓取故障注入（进程内，无需预启动服务）
python3 tests/trace_faults.py
```

`selftest.py` 覆盖：回执字节数/SHA-256 校验、延迟流水化（非按块累加）、
64 KiB 缓冲上限与峰值、EOF 先冲刷再半关闭、连接级配置快照、并发上传、
RST 断连释放。

`trace_selftest.py` 覆盖：轨迹内容逐字节还原、相对时间单调、半关闭顺序
（先上行后下行）、抓取开关只对新连接生效、超 1 MiB 标记不完整、RST 不误标
完整、下载头与文件名后缀。

`trace_faults.py` 覆盖：落盘变慢导致 64 KiB 缓冲满、写盘失败、目录不可用、
未收尾被取消（仅 `.partial`）、1 MiB 上限、以及慢轨迹下真实转发仍快速且
回执正确。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/config` | 当前配置、监听状态与抓取开关 |
| POST | `/api/config` | 更新配置（仅影响新连接） |
| POST | `/api/capture` | 开启/关闭抓取（`{"enabled": true}`，仅影响新连接） |
| GET | `/api/connections` | 活动+历史连接：字节数、缓冲、回执尾部、轨迹状态 |
| GET | `/api/connections/{id}/trace` | 下载该连接轨迹；`X-Trace-Complete` 头表示是否完整 |
| POST | `/api/test-upload` | multipart 上传文件，经代理试传并返回回执 |
| POST | `/api/history/clear` | 清空历史连接 |

## 目录结构

```
docker-compose.yml      # proxy + receiver 一键启动（轨迹卷 proxy-traces）
proxy/
  Dockerfile
  requirements.txt
  app/
    main.py             # FastAPI：配置/统计/抓取/轨迹下载/试传/网页
    engine.py           # 延迟代理引擎（asyncio，无第三方依赖）
    trace.py            # 按连接抓取轨迹（内存缓冲、异步落盘、完整性收尾）
    static/index.html   # 网页
receiver/
  Dockerfile
  server.py             # 示例接收端：欢迎行 + EOF 后回执(字节数+SHA-256)
tests/
  selftest.py           # 端到端自测
  trace_selftest.py     # 抓取轨迹端到端自测
  trace_faults.py       # 抓取故障注入自测
```
