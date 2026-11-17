#!/usr/bin/env python3
"""抓取故障注入自测（进程内，不依赖已运行的服务）。

验证：落盘变慢导致 64 KiB 缓冲满、写盘失败、轨迹超过 1 MiB、轨迹目录
不可用时，转发不受影响，且轨迹绝不误标为完整；正常收尾必须 fsync+改名。

运行：python3 tests/trace_faults.py
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "proxy"))

from app import trace as trace_mod  # noqa: E402
from app.engine import DelayProxy, ProxyConfig  # noqa: E402

failures: list[str] = []


def check(name, cond, detail=""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def read_jsonl(path):
    import json
    out = []
    with open(path, "rb") as fh:
        for line in fh:
            if line.strip():
                out.append(json.loads(line))
    return out


async def wait_until(pred, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(0.02)
    return False


class SlowTrace(trace_mod.TraceSession):
    """每次写盘人为变慢，用于制造待写缓冲满。"""

    delay = 0.25

    def _append(self, line):
        time.sleep(self.delay)
        super()._append(line)


class FailTrace(trace_mod.TraceSession):
    calls = 0

    def _append(self, line):
        type(self).calls += 1
        if type(self).calls > 2:
            raise OSError(28, "No space left on device")
        super()._append(line)


class BadDirTrace(trace_mod.TraceSession):
    def _prepare_file(self):
        raise PermissionError(13, "Permission denied", str(self.dir))


async def run_session(session, *, chunks=40, chunk_size=16 * 1024,
                      finalize_timeout=1.0, cancel=False):
    """直接驱动会话：模拟转发循环的调用方式。返回耗时。"""
    t0 = time.monotonic()
    for _ in range(chunks):
        session.record_data("up", b"x" * chunk_size)
        await asyncio.sleep(0)  # 让出事件循环
    session.record_event("eof_received", "up")
    session.record_event("half_close_forwarded", "up")
    if cancel:
        # 等文件创建并已有内容落盘，再模拟停机/崩溃取消后台任务
        await wait_until(session.partial_path.exists, timeout=5.0)
        await asyncio.sleep(0.05)
        session._task.cancel()
        await asyncio.gather(session._task, return_exceptions=True)
        return time.monotonic() - t0
    await session.finalize(complete=True, state="closed", timeout=finalize_timeout)
    return time.monotonic() - t0


async def main():
    tmp = tempfile.mkdtemp(prefix="trace-faults-")
    cfg = ProxyConfig()
    try:
        # ---------------- 1. 正常会话：完整、fsync 后改名、无 .partial ----------
        d = os.path.join(tmp, "normal")
        s = trace_mod.TraceSession(1, "c", cfg, d)
        await run_session(s, chunks=2)
        check("normal session complete", s.status == "complete", s.status)
        check("normal renamed (no .partial)", s.path.exists()
              and not s.partial_path.exists(), str(os.listdir(d)))
        recs = read_jsonl(s.path)
        check("normal last line complete=true",
              recs[-1]["type"] == "end" and recs[-1]["complete"] is True, str(recs[-1]))

        # ---------------- 2. 落盘慢：缓冲满 -> 不完整，record_data 不阻塞 ----------
        d = os.path.join(tmp, "slow")
        s = SlowTrace(2, "c", cfg, d)
        t0 = time.monotonic()
        for _ in range(40):
            s.record_data("up", b"y" * (16 * 1024))
            await asyncio.sleep(0)
        loop_time = time.monotonic() - t0
        check("slow disk: enqueue non-blocking", loop_time < 0.5, f"{loop_time:.3f}s")
        check("slow disk: marked incomplete", s.status == "incomplete", s.status)
        check("slow disk: reason is buffer full",
              "pending buffer" in (s.reason or ""), str(s.reason))
        check("slow disk: captured <= 64 KiB + one chunk",
              s.captured_bytes <= 64 * 1024 + 16 * 1024, str(s.captured_bytes))
        await s.finalize(complete=True, state="closed", timeout=10.0)
        check("slow disk: final status incomplete", s.status == "incomplete", s.status)
        # 收尾时队列能排空，因此文件已改名，但尾记录显式 complete=false
        out_path = s.path if s.path.exists() else s.partial_path
        recs = read_jsonl(out_path)
        check("slow disk: footer complete=false",
              recs[-1]["type"] == "end" and recs[-1]["complete"] is False
              and "pending buffer" in (recs[-1]["reason"] or ""), str(recs[-1]))

        # ---------------- 3. 写盘失败：不完整且说明原因，转发侧无异常 ----------
        d = os.path.join(tmp, "fail")
        s = FailTrace(3, "c", cfg, d)
        for _ in range(10):
            s.record_data("up", b"z" * 1024)  # 不应抛异常
            await asyncio.sleep(0.05)
        check("write fail: no exception escaped", True)
        check("write fail: status incomplete", s.status == "incomplete", s.status)
        check("write fail: reason mentions write failed",
              "write failed" in (s.reason or ""), str(s.reason))
        await s.finalize(complete=True, state="closed", timeout=1.0)
        check("write fail: finalize still incomplete", s.status == "incomplete", s.status)
        check("write fail: no complete footer",
              not (s.path.exists() and read_jsonl(s.path)[-1].get("complete")),
              str(s.path.exists()))

        # ---------------- 4. 目录不可写：会话创建/写入失败不影响调用方 ----------
        d = os.path.join(tmp, "baddir")
        s = BadDirTrace(4, "c", cfg, d)
        for _ in range(5):
            s.record_data("up", b"q" * 500)
            await asyncio.sleep(0.01)
        await s.finalize(complete=True, state="closed", timeout=1.0)
        check("bad dir: status incomplete", s.status == "incomplete", s.status)
        check("bad dir: reason explains failure", bool(s.reason), str(s.reason))

        # ---------------- 5. 未收尾就被取消（模拟停机/崩溃）：不能有完整文件 ----------
        d = os.path.join(tmp, "cancel")
        s = SlowTrace(5, "c", cfg, d)
        await run_session(s, chunks=8, cancel=True)
        files = os.listdir(d) if os.path.isdir(d) else []
        check("cancel: only .partial file exists",
              bool(files) and all(f.endswith(".partial") for f in files), str(files))
        if files:
            recs = read_jsonl(os.path.join(d, files[0]))
            check("cancel: no complete end record",
                  not recs or recs[-1].get("type") != "end"
                  or recs[-1].get("complete") is not True, str(recs[-1] if recs else None))

        # ---------------- 6. 超过 1 MiB：停止抓取并标记原因 -----------------
        trace_mod.MAX_TRACE_BYTES = 64 * 1024   # 临时调小加速测试
        try:
            d = os.path.join(tmp, "limit")
            s = trace_mod.TraceSession(6, "c", cfg, d)
            n = 0
            while s.status == "recording" and n < 1000:
                s.record_data("up", b"m" * 4096)
                await asyncio.sleep(0)
                n += 1
            check("size limit: capture stopped", s.status == "incomplete", s.status)
            check("size limit: reason mentions limit",
                  "size limit" in (s.reason or ""), str(s.reason))
            await s.finalize(complete=True, state="closed", timeout=5.0)
            out = s.path if s.path.exists() else s.partial_path
            size = os.path.getsize(out)
            check("size limit: file bounded near limit", size <= 96 * 1024, str(size))
            check("size limit: not marked complete", s.status == "incomplete", s.status)
        finally:
            trace_mod.MAX_TRACE_BYTES = 1024 * 1024

        # ---------------- 7. 代理整体转发不受慢轨迹影响（本地回环） ----------
        await proxy_forwarding_with_slow_trace(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL FAULT CHECKS PASSED")


async def proxy_forwarding_with_slow_trace(tmp):
    """用 SlowTrace 替换会话类，跑真实代理转发，验证回执照常且轨迹不完整。"""
    delays = []

    class SlowEngineTrace(SlowTrace):
        delay = 0.3

        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            delays.append(self)

    orig = trace_mod.TraceSession
    import app.engine as engine_mod
    engine_mod.TraceSession = SlowEngineTrace
    receiver_server = await asyncio.start_server(echo_receiver, "127.0.0.1", 0)
    rport = receiver_server.sockets[0].getsockname()[1]
    cfg = ProxyConfig(listen_host="127.0.0.1", listen_port=0,
                      target_host="127.0.0.1", target_port=rport,
                      up_delay_ms=0, down_delay_ms=0)
    proxy = DelayProxy(cfg, trace_dir=os.path.join(tmp, "proxy"))
    proxy.set_capture(True)
    try:
        await proxy.apply_config(cfg)
        port = proxy.server.sockets[0].getsockname()[1]
        payload = os.urandom(1024 * 1024)
        t0 = time.monotonic()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(payload)
        await writer.drain()
        writer.write_eof()
        receipt = await asyncio.wait_for(reader.read(), 30)
        dt = time.monotonic() - t0
        writer.close()
        check("slow trace: forwarding still fast", dt < 5.0, f"{dt:.2f}s for 1 MiB")
        check("slow trace: receipt correct",
              receipt == f"OK {len(payload)} {hashlib_sha(payload)}".encode(),
              receipt[:80].decode(errors="replace"))
        ok = await wait_until(lambda: not proxy.active and proxy.history)
        check("slow trace: connection finished", ok)
        info = proxy.history[0]
        await asyncio.sleep(0.2)
        check("slow trace: trace incomplete", info.trace.status == "incomplete",
              f"{info.trace.status} / {info.trace.reason}")
        check("slow trace: connection state closed", info.state == "closed", info.state)
        check("slow trace: trace path served as partial only",
              proxy.trace_path(info.id) is not None
              and proxy.trace_path(info.id).suffix == ".partial",
              str(proxy.trace_path(info.id)))
    finally:
        await proxy.shutdown()
        receiver_server.close()
        await receiver_server.wait_closed()
        trace_mod.TraceSession = orig
        engine_mod.TraceSession = orig


def hashlib_sha(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()


async def echo_receiver(reader, writer):
    total = 0
    import hashlib
    h = hashlib.sha256()
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            break
        total += len(chunk)
        h.update(chunk)
    writer.write(f"OK {total} {h.hexdigest()}".encode())
    await writer.drain()
    writer.close()


if __name__ == "__main__":
    asyncio.run(main())
