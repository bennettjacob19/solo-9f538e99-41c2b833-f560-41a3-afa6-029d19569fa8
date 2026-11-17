"""TCP 延迟代理引擎（纯 asyncio，无第三方依赖）。

实现的语义：
* 延迟从代理读到字节时起算：每个数据块获得各自的投递时刻
  (read_time + delay)，按 FIFO 顺序写出，连续数据流水通过延迟线，
  不会按读取块数累加等待。
* 每个方向的应用缓冲（已读未写）上限 64 KiB；缓冲满时只暂停该方向的
  读取，反向及本机其他连接照常传输。
* 连接建立时快照配置，整条连接沿用，之后改配置不影响它。
* 收到单向 EOF 后，先把该方向队列中待发字节全部写出，再转发半关闭
  (write_eof)；反向方向继续传输直到结束。
* 断连（正常结束或对端 RST）时释放队列并关闭两侧连接。
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from .trace import (FLUSH_TIMEOUT, MAX_TRACE_BYTES, PENDING_LIMIT,
                    TraceSession, default_trace_dir)

BUFFER_LIMIT = 64 * 1024   # 每方向应用缓冲上限
READ_CHUNK = 16 * 1024     # 单次 read() 最大字节数
TAIL_KEEP = 4096           # 下行数据尾部保留长度（回执预览）
HISTORY_MAX = 100          # 历史连接保留条数
CONNECT_TIMEOUT = 15.0     # 连接目标的超时
SHUTDOWN_TRACE_WAIT = 20.0 # 停机时等待轨迹收尾的上限


@dataclass
class ProxyConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    target_host: str = "127.0.0.1"
    target_port: int = 9000
    up_delay_ms: float = 300.0    # 客户端 -> 目标
    down_delay_ms: float = 300.0  # 目标 -> 客户端


@dataclass
class DirectionStats:
    bytes: int = 0    # 已读累计字节
    peak: int = 0     # 应用缓冲峰值
    queued: int = 0   # 当前缓冲字节


@dataclass
class ConnInfo:
    id: int
    client: str
    cfg: ProxyConfig  # 建立时的配置快照
    started_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    state: str = "active"  # active | closed | error
    error: str | None = None
    up: DirectionStats = field(default_factory=DirectionStats)    # 客户端 -> 目标
    down: DirectionStats = field(default_factory=DirectionStats)  # 目标 -> 客户端
    downlink_tail: bytes = b""  # 下行数据尾部（回执预览）
    capture_requested: bool = False  # 建立时抓取开关快照
    trace: TraceSession | None = None
    trace_init_error: str | None = None


class DirectionPump:
    """单向延迟管道：从 reader 读，延迟后写入 writer。"""

    def __init__(self, name, reader, writer, delay_s, stats, on_data=None,
                 trace=None):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.delay = delay_s
        self.stats = stats
        self.on_data = on_data  # 写出后的回调（用于抓回执）
        self.trace = trace      # 抓取会话（未开启时为 None）
        self._queue: deque[tuple[float, bytes | None]] = deque()  # None = EOF 哨兵
        self._queued = 0
        self._space = asyncio.Event()   # 缓冲未满
        self._space.set()
        self._items = asyncio.Event()   # 队列非空

    async def run(self):
        rx = asyncio.create_task(self._read_loop(), name=f"pump-{self.name}-rx")
        tx = asyncio.create_task(self._write_loop(), name=f"pump-{self.name}-tx")
        try:
            await asyncio.gather(rx, tx)
        finally:
            for t in (rx, tx):
                t.cancel()
            await asyncio.gather(rx, tx, return_exceptions=True)
            # 断连/结束：释放队列
            self._queue.clear()
            self._queued = 0
            self.stats.queued = 0

    async def _read_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            await self._space.wait()  # 缓冲满则暂停该向读取（反向不受影响）
            max_read = min(READ_CHUNK, BUFFER_LIMIT - self._queued)
            chunk = await self.reader.read(max_read)
            ready_at = loop.time() + self.delay  # 延迟自读到字节时起算
            if chunk:
                self._queue.append((ready_at, chunk))
                self._queued += len(chunk)
                self.stats.bytes += len(chunk)
                self.stats.queued = self._queued
                if self._queued > self.stats.peak:
                    self.stats.peak = self._queued
                if self._queued >= BUFFER_LIMIT:
                    self._space.clear()
                self._items.set()
            else:
                # EOF 哨兵入队：半关闭只在该向待发字节全部写出后转发
                self._queue.append((ready_at, None))
                self._items.set()
                if self.trace is not None:
                    self.trace.record_event("eof_received", self.name)
                return

    async def _write_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            while not self._queue:
                self._items.clear()
                await self._items.wait()
            ready_at, chunk = self._queue[0]
            wait = ready_at - loop.time()
            if wait > 0:
                # 只等到队首块到期；后续块各自带投递时刻，流水发送
                await asyncio.sleep(wait)
            self._queue.popleft()
            if chunk is None:
                # 该向待发字节已全部写出 -> 转发半关闭
                try:
                    if self.writer.can_write_eof():
                        self.writer.write_eof()
                        await self.writer.drain()
                        if self.trace is not None:
                            self.trace.record_event(
                                "half_close_forwarded", self.name)
                    else:
                        self.writer.close()
                except (ConnectionError, OSError, RuntimeError) as exc:
                    if self.trace is not None:
                        self.trace.record_event(
                            "half_close_error", self.name,
                            error=f"{type(exc).__name__}: {exc}")
                return
            self.writer.write(chunk)
            await self.writer.drain()
            self._queued -= len(chunk)
            self.stats.queued = self._queued
            if self.trace is not None:
                # 只入内存队列，不等待磁盘；溢出/超限会由会话标记不完整
                self.trace.record_data(self.name, chunk)
            if self.on_data is not None:
                self.on_data(chunk)
            if self._queued < BUFFER_LIMIT:
                self._space.set()


class Connection:
    """一条代理连接：客户端 <-> 两个 DirectionPump <-> 目标。"""

    def __init__(self, client_reader, client_writer, cfg: ProxyConfig, conn_id: int,
                 trace_dir: Path | None = None, capture: bool = False,
                 is_shutting_down=None):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.target_reader = None
        self.target_writer = None
        self.pumps: list[DirectionPump] = []
        self.is_shutting_down = is_shutting_down
        peer = client_writer.get_extra_info("peername")
        client = f"{peer[0]}:{peer[1]}" if peer else "?"
        self.info = ConnInfo(id=conn_id, client=client, cfg=dataclasses.replace(cfg),
                             capture_requested=capture)
        if capture:
            try:
                self.info.trace = TraceSession(conn_id, client, cfg,
                                               trace_dir or default_trace_dir())
            except Exception as exc:
                # 轨迹目录不可用等：只影响抓取，转发继续
                self.info.trace_init_error = f"{type(exc).__name__}: {exc}"

    def _capture_downlink(self, data: bytes):
        tail = self.info.downlink_tail + data
        self.info.downlink_tail = tail[-TAIL_KEEP:]

    async def _finalize_trace(self, *, complete: bool, state: str,
                              error: str | None,
                              timeout: float = FLUSH_TIMEOUT):
        trace = self.info.trace
        if trace is None:
            return
        info = self.info
        reason = self.info.trace_init_error
        if reason is None and self.is_shutting_down is not None \
                and self.is_shutting_down():
            reason = "proxy shutdown before connection ended"
        await trace.finalize(
            complete=complete, state=state, error=error,
            reason=reason,
            up_bytes=info.up.bytes, down_bytes=info.down.bytes,
            timeout=timeout)

    async def run(self):
        info = self.info
        cfg = info.cfg
        try:
            self.target_reader, self.target_writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.target_host, cfg.target_port),
                timeout=CONNECT_TIMEOUT,
            )
        except Exception as exc:
            info.state = "error"
            info.error = f"connect target {cfg.target_host}:{cfg.target_port} failed: {exc}"
            if info.trace is not None:
                info.trace.record_event("error", error=info.error)
            self._close(self.client_writer)
            await self._wait_closed(self.client_writer)
            info.ended_at = time.time()
            await self._finalize_trace(complete=False, state="error",
                                       error=info.error)
            return

        trace = info.trace
        up = DirectionPump("up", self.client_reader, self.target_writer,
                           cfg.up_delay_ms / 1000.0, info.up, trace=trace)
        down = DirectionPump("down", self.target_reader, self.client_writer,
                             cfg.down_delay_ms / 1000.0, info.down,
                             on_data=self._capture_downlink, trace=trace)
        self.pumps = [up, down]
        tasks = [asyncio.create_task(up.run()), asyncio.create_task(down.run())]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            # 停机/上游取消：记为异常结束，并继续向上传播取消
            info.state = "error"
            info.error = "connection cancelled (proxy shutdown)"
            if trace is not None:
                trace.record_event("error", error=info.error)
            raise
        except Exception as exc:
            info.state = "error"
            info.error = f"{type(exc).__name__}: {exc}"
            if trace is not None:
                trace.record_event("error", error=info.error)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # 断连：关闭两侧连接（队列已在 pump 内释放）
            self._close(self.client_writer)
            self._close(self.target_writer)
            await self._wait_closed(self.client_writer)
            await self._wait_closed(self.target_writer)
            clean = info.state == "active"
            if clean:
                info.state = "closed"
            info.ended_at = time.time()
            await self._finalize_trace(
                complete=clean, state=info.state, error=info.error,
                timeout=FLUSH_TIMEOUT if clean else 1.0)

    @staticmethod
    def _close(writer):
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    async def _wait_closed(writer):
        if writer is not None:
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception:
                pass


class DelayProxy:
    """监听 TCP 端口，为每条连接建立到目标的延迟管道。"""

    def __init__(self, cfg: ProxyConfig, trace_dir: Path | None = None):
        self.cfg = cfg
        self.server: asyncio.Server | None = None
        self.listen_error: str | None = None
        self.active: dict[int, Connection] = {}
        self.history: deque[ConnInfo] = deque(maxlen=HISTORY_MAX)
        self._next_id = 1
        self._tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        self.capture = False   # 抓取开关：只对开启后新建的连接生效
        self.trace_dir = Path(trace_dir) if trace_dir else default_trace_dir()
        self.shutting_down = False

    def set_capture(self, enabled: bool) -> bool:
        """打开/关闭抓取。已有连接沿用建立时的快照，不受影响。"""
        self.capture = bool(enabled)
        return self.capture

    @property
    def listening(self) -> bool:
        return self.server is not None and self.server.is_serving()

    async def apply_config(self, new_cfg: ProxyConfig):
        """应用新配置；仅影响之后建立的连接。监听地址/端口变化时重启监听器。"""
        async with self._lock:
            restart = (
                self.server is None
                or new_cfg.listen_host != self.cfg.listen_host
                or new_cfg.listen_port != self.cfg.listen_port
            )
            self.cfg = dataclasses.replace(new_cfg)
            if not restart:
                return
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()
                self.server = None
            self.listen_error = None
            try:
                self.server = await asyncio.start_server(
                    self._on_client, new_cfg.listen_host, new_cfg.listen_port)
            except Exception as exc:
                self.listen_error = str(exc)

    def _on_client(self, reader, writer):
        cfg = dataclasses.replace(self.cfg)  # 建立时快照配置
        capture = self.capture            # 建立时快照抓取开关
        conn = Connection(reader, writer, cfg, self._next_id,
                          trace_dir=self.trace_dir, capture=capture,
                          is_shutting_down=lambda: self.shutting_down)
        self._next_id += 1
        cid = conn.info.id
        self.active[cid] = conn
        task = asyncio.create_task(conn.run(), name=f"conn-{cid}")
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._finish(cid)))

    def _finish(self, cid: int):
        conn = self.active.pop(cid, None)
        if conn is not None:
            self.history.appendleft(conn.info)

    async def shutdown(self):
        self.shutting_down = True
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            # 给连接的清理逻辑时间写下“不完整”结束记录并释放端口
            await asyncio.wait(tasks, timeout=SHUTDOWN_TRACE_WAIT)
        for conn in list(self.active.values()):
            trace = conn.info.trace
            if trace is not None:
                await trace.finalize(
                    complete=False, state=conn.info.state,
                    error=conn.info.error,
                    reason="proxy shutdown before connection ended",
                    up_bytes=conn.info.up.bytes,
                    down_bytes=conn.info.down.bytes, timeout=1.0)

    def find_info(self, conn_id: int) -> ConnInfo | None:
        conn = self.active.get(conn_id)
        if conn is not None:
            return conn.info
        for info in self.history:
            if info.id == conn_id:
                return info
        return None

    def trace_path(self, conn_id: int) -> Path | None:
        """返回该连接可下载的轨迹文件路径（完整为 .jsonl，不完整为 .partial）。"""
        info = self.find_info(conn_id)
        if info is None or info.trace is None:
            return None
        trace = info.trace
        if not trace._finalized or trace._available_path is None:
            return None
        return trace._available_path

    def snapshot(self) -> dict:
        def conn_dict(info: ConnInfo) -> dict:
            d = {
                "id": info.id,
                "client": info.client,
                "state": info.state,
                "error": info.error,
                "target": f"{info.cfg.target_host}:{info.cfg.target_port}",
                "up_delay_ms": info.cfg.up_delay_ms,
                "down_delay_ms": info.cfg.down_delay_ms,
                "started_at": info.started_at,
                "ended_at": info.ended_at,
                "up": dataclasses.asdict(info.up),
                "down": dataclasses.asdict(info.down),
                "receipt": info.downlink_tail.decode("utf-8", "replace"),
                "capture_requested": info.capture_requested,
                "trace_init_error": info.trace_init_error,
                "capture": (info.trace.meta() if info.trace is not None else
                            {"enabled": False}),
            }
            return d

        return {
            "listening": self.listening,
            "listen_error": self.listen_error,
            "config": dataclasses.asdict(self.cfg),
            "capture": {
                "enabled": self.capture,
                "dir": str(self.trace_dir),
                "pending_limit": PENDING_LIMIT,
                "max_bytes": MAX_TRACE_BYTES,
            },
            "active": [conn_dict(c.info) for c in self.active.values()],
            "history": [conn_dict(i) for i in self.history],
        }
