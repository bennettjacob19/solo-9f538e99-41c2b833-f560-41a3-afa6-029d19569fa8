"""按连接抓取代理实际转发的双向字节。

设计目标：抓取与转发隔离。
* 转发协程只做一次内存入队，绝不等待磁盘；
* 每连接待写抓取数据上限 64 KiB（按原始转发字节计），另有 ≤ 4 KiB 的
  控制记录（EOF/半关闭/异常等）缓冲。落盘变慢导致缓冲满时停止抓取并
  标记轨迹不完整，转发（上传、回执）完全不受影响；
* 单连接轨迹文件上限 1 MiB，超过后停止抓取并标记不完整；
* 写盘失败只影响轨迹，转发照常；
* 记录先写 ``*.jsonl.partial``，只有尾部结束记录写入、fsync 并原子
  改名后才算完整；正常连接必须等这些步骤完成，轨迹状态才会是 complete。
  断连/停机若来不及写完，留下的只会是 ``*.partial`` 或带
  ``"complete": false`` 尾记录的文件，不会被误标为完整。

记录格式为 JSON Lines（UTF-8）：
  {"v":1,"type":"start", ...}                    连接建立、配置快照
  {"v":1,"type":"data","t":...,"dir":"up",...}   实际写出的数据（base64）
  {"v":1,"type":"event","t":...,"event":...}     EOF、半关闭、异常等
  {"v":1,"type":"end","complete":true/false,...} 结束记录（最后一行）
``t`` 为相对连接建立的秒数（monotonic）。
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from pathlib import Path
from typing import Any

PENDING_LIMIT = 64 * 1024        # 每连接待写抓取数据上限（原始转发字节）
META_PENDING_LIMIT = 4 * 1024   # 控制记录缓冲上限（EOF/半关闭/异常等小记录，不计入数据上限）
MAX_TRACE_BYTES = 1024 * 1024   # 每连接轨迹文件上限（含 JSON/base64 开销）
FLUSH_TIMEOUT = 5.0             # 正常结束时等待落盘的上限（秒）
TRACE_ENV_DIR = "TRACE_DIR"


def default_trace_dir() -> Path:
    return Path(os.getenv(TRACE_ENV_DIR, "/tmp/tcp-delay-proxy/traces"))


class TraceSession:
    """一条连接的轨迹记录器。所有公开方法都不会因磁盘问题抛给调用方。"""

    def __init__(self, conn_id: int, client: str, cfg: Any, trace_dir: Path):
        self.conn_id = conn_id
        self.client = client
        self.cfg = cfg
        self.dir = Path(trace_dir)
        self.started_wall = time.time()
        self._t0 = time.monotonic()
        self.filename = f"conn-{conn_id:06d}-{int(self.started_wall * 1000)}.jsonl"
        self.path = self.dir / self.filename
        self.partial_path = self.dir / (self.filename + ".partial")

        self.status = "recording"   # recording | finalizing | complete | incomplete
        self.reason: str | None = None
        self.captured_bytes = 0     # 已写入轨迹的原始转发字节数
        self.half_close_order: list[str] = []
        self._serialized_total = 0  # 已入队记录的序列化字节数（1 MiB 文件上限）
        self._pending_data = 0      # 待写数据缓冲（原始转发字节）
        self._pending_meta = 0      # 待写控制记录缓冲（序列化字节）
        self._accepting = True
        self._write_error: str | None = None
        self._finalized = False
        self._available_path: Path | None = None  # 收尾后落盘位置（缓存）
        self._lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        # 队列项：(序列化行, 数据记账字节, 控制记账字节)
        self._queue: asyncio.Queue[tuple[bytes, int, int]] = asyncio.Queue()

        # 先入队 start 记录；写文件由后台任务完成，accept 路径不等待磁盘。
        self._queue_record(self._start_record(), force=True)
        self._task = asyncio.create_task(
            self._writer_loop(), name=f"trace-{conn_id}")

    # ------------------------------------------------------------------ 记录
    def _elapsed(self) -> float:
        return round(time.monotonic() - self._t0, 6)

    def _start_record(self) -> dict:
        return {
            "v": 1,
            "type": "start",
            "t": 0.0,
            "conn": self.conn_id,
            "client": self.client,
            "target": f"{self.cfg.target_host}:{self.cfg.target_port}",
            "up_delay_ms": self.cfg.up_delay_ms,
            "down_delay_ms": self.cfg.down_delay_ms,
            "directions": {"up": "client->target", "down": "target->client"},
            "limits": {"pending_data": PENDING_LIMIT,
                       "pending_meta": META_PENDING_LIMIT,
                       "trace_bytes": MAX_TRACE_BYTES},
            "started_at": round(self.started_wall, 3),
        }

    def _mark_incomplete(self, reason: str):
        self._accepting = False
        if reason and reason not in (self.reason or ""):
            self.reason = f"{self.reason}; {reason}" if self.reason else reason
        if self.status in ("recording", "finalizing"):
            self.status = "incomplete"

    def _queue_record(self, record: dict, *, force: bool = False,
                      data_cost: int = 0) -> bool:
        """把记录序列化后放入待写队列。

        ``data_cost`` > 0 表示数据记录，按原始转发字节计入 64 KiB 数据缓冲；
        其它记录（start/event）是小控制记录，计入 4 KiB 控制缓冲。
        返回 False 表示因缓冲满/超限/已停止而丢弃。不阻塞、不抛异常。
        """
        if not force and not self._accepting:
            return False
        try:
            line = (json.dumps(record, ensure_ascii=False,
                               separators=(",", ":")) + "\n").encode("utf-8")
        except Exception as exc:  # 不应发生；保守地停止抓取
            self._mark_incomplete(f"trace record encode failed: {exc}")
            return False
        if self._serialized_total + len(line) > MAX_TRACE_BYTES:
            self._mark_incomplete(
                f"trace size limit {MAX_TRACE_BYTES} bytes exceeded; "
                "capture stopped, forwarding unaffected")
            return False
        if data_cost:
            if self._pending_data + data_cost > PENDING_LIMIT:
                self._mark_incomplete(
                    f"trace pending buffer {PENDING_LIMIT} bytes full "
                    "(disk write too slow); capture stopped, forwarding unaffected")
                return False
            d_cost, m_cost = data_cost, 0
            self._pending_data += d_cost
        else:
            if self._pending_meta + len(line) > META_PENDING_LIMIT:
                self._mark_incomplete(
                    f"trace control buffer {META_PENDING_LIMIT} bytes full "
                    "(disk write too slow); capture stopped, forwarding unaffected")
                return False
            d_cost, m_cost = 0, len(line)
            self._pending_meta += m_cost
        self._serialized_total += len(line)
        self._idle.clear()
        self._queue.put_nowait((line, d_cost, m_cost))
        return True

    def record_data(self, direction: str, data: bytes):
        """记录代理已经实际写出的数据块（direction: up/down）。"""
        if not data or not self._accepting or self._write_error:
            return
        record = {
            "v": 1,
            "type": "data",
            "t": self._elapsed(),
            "dir": direction,
            "n": len(data),
            "b64": base64.b64encode(data).decode("ascii"),
        }
        if self._queue_record(record, data_cost=len(data)):
            self.captured_bytes += len(data)

    def record_event(self, event: str, direction: str | None = None,
                     **fields):
        """记录事件。半关闭顺序即使抓取已停止也保留在内存元数据中。"""
        if event == "half_close_forwarded" and direction:
            if direction not in self.half_close_order:
                self.half_close_order.append(direction)
        record = {"v": 1, "type": "event", "t": self._elapsed(),
                  "event": event}
        if direction:
            record["dir"] = direction
        record.update(fields)
        self._queue_record(record)

    # ------------------------------------------------------------- 后台落盘
    async def _writer_loop(self):
        try:
            await asyncio.to_thread(self._prepare_file)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._write_error = f"trace file create failed: {exc}"
            self._mark_incomplete(self._write_error)
            self._discard_pending()
            return
        try:
            while True:
                line, d_cost, m_cost = await self._queue.get()
                try:
                    await asyncio.to_thread(self._append, line)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._write_error = (
                        f"trace write failed: {type(exc).__name__}: {exc}")
                    self._mark_incomplete(
                        f"{self._write_error}; forwarding unaffected")
                    self._discard_pending()
                    return
                else:
                    self._release(d_cost, m_cost)
        finally:
            # 被取消（停机/崩溃模拟）时队列项的占用也要归还，避免元数据卡住
            self._discard_pending()

    def _release(self, d_cost: int, m_cost: int):
        self._pending_data -= d_cost
        self._pending_meta -= m_cost
        if self._pending_data < 0:
            self._pending_data = 0
        if self._pending_meta < 0:
            self._pending_meta = 0
        if self._pending_data == 0 and self._pending_meta == 0:
            self._idle.set()

    def _prepare_file(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.partial_path, "wb"):
            pass

    def _append(self, line: bytes):
        with open(self.partial_path, "ab") as fh:
            fh.write(line)

    def _discard_pending(self):
        while True:
            try:
                _line, d_cost, m_cost = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pending_data -= d_cost
            self._pending_meta -= m_cost
        if self._pending_data < 0:
            self._pending_data = 0
        if self._pending_meta < 0:
            self._pending_meta = 0
        self._idle.set()

    async def _wait_idle(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _finalize_file(self, footer: bytes, complete: bool):
        """追加结束记录并 fsync；仅当轨迹完整时才原子改名为最终文件。

        * complete=true：写尾记录 -> fsync -> os.replace 改名为 ``.jsonl``。
          因此磁盘上的 ``*.jsonl`` 一定是完整轨迹（尾记录 complete=true）。
        * complete=false：尾记录（complete=false，含原因）写入并 fsync，
          但文件保持 ``*.jsonl.partial``，文件名本身即表明不完整。
        任何一步中断都不会产生“名为完整、内容未收尾”的文件。
        """
        with open(self.partial_path, "ab") as fh:
            fh.write(footer)
            fh.flush()
            os.fsync(fh.fileno())
        if not complete:
            return
        os.replace(self.partial_path, self.path)
        # 目录 fsync 是尽力而为；失败不影响文件内容已落盘。
        try:
            dfd = os.open(str(self.dir), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass

    # --------------------------------------------------------------- 结束
    async def finalize(self, *, complete: bool, state: str,
                       error: str | None = None, reason: str | None = None,
                       up_bytes: int = 0, down_bytes: int = 0,
                       timeout: float = FLUSH_TIMEOUT):
        """结束轨迹。可重复调用（第二条会被忽略）。

        只有以下条件全部满足才写 complete=true 并改名：
        调用方认为连接正常结束、抓取期间无缓冲溢出/超限/写盘失败、
        队列在超时内全部落盘、尾记录 fsync + 改名成功。
        """
        async with self._lock:
            if self._finalized:
                return
            if self.status == "recording":
                self.status = "finalizing"   # 收尾中：等待落盘期间不算完整
            if reason:
                self._mark_incomplete(reason)
            self._accepting = False
            if not complete:
                self._mark_incomplete(reason or "connection ended abnormally")
            if self._write_error:
                complete = False
            if self.status == "incomplete":
                complete = False

            drained = await self._wait_idle(timeout)
            if not drained:
                # 磁盘仍未写完：不写尾记录、不改名，保留 .partial。
                self._mark_incomplete(
                    f"trace flush timeout after {timeout:.1f}s; "
                    "partial trace left unfinished")
                complete = False

            if drained:
                # 队列已空且没有写入在途；可以安全追加尾记录。
                footer_complete = complete and not self._write_error
                footer = {
                    "v": 1,
                    "type": "end",
                    "t": self._elapsed(),
                    "complete": footer_complete,
                    "state": state,
                    "error": error,
                    "reason": None if footer_complete else self.reason,
                    "up_bytes": up_bytes,
                    "down_bytes": down_bytes,
                    "captured_bytes": self.captured_bytes,
                    "half_close_order": list(self.half_close_order),
                }
                try:
                    line = (json.dumps(footer, ensure_ascii=False,
                                       separators=(",", ":")) + "\n").encode()
                    await asyncio.wait_for(
                        asyncio.to_thread(self._finalize_file, line,
                                          footer_complete),
                        timeout=timeout)
                except Exception as exc:
                    self._write_error = (
                        f"trace finalize failed: {type(exc).__name__}: {exc}")
                    self._mark_incomplete(f"{self._write_error}; "
                                          "partial trace not marked complete")
                    complete = False
                if footer_complete and complete:
                    self.status = "complete"
                    self.reason = None
                else:
                    complete = False
                    self._mark_incomplete(self.reason or "trace incomplete")
            else:
                complete = False

            if not complete:
                if not self.reason:
                    self.reason = "trace incomplete"
                self.status = "incomplete"   # 显式覆盖 finalizing

            # 缓存最终位置，供 API 轮询时不必反复 stat 磁盘
            if self.path.exists():
                self._available_path = self.path
            elif self.partial_path.exists():
                self._available_path = self.partial_path

            self._finalized = True
            task = getattr(self, "_task", None)
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    # --------------------------------------------------------------- 元数据
    def meta(self) -> dict:
        return {
            "enabled": True,
            "status": self.status,
            "reason": self.reason,
            "filename": self.filename,
            "available": self._available_path is not None,
            "finalized": self._finalized,
            "captured_bytes": self.captured_bytes,
            "pending_bytes": self._pending_data + self._pending_meta,
            "pending_data_bytes": self._pending_data,
            "pending_control_bytes": self._pending_meta,
            "buffer_limit": PENDING_LIMIT,
            "control_buffer_limit": META_PENDING_LIMIT,
            "max_bytes": MAX_TRACE_BYTES,
            "half_close_order": list(self.half_close_order),
            "download_url": f"/api/connections/{self.conn_id}/trace",
        }
