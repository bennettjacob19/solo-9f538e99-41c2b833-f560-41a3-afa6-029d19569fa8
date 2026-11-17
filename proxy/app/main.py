"""FastAPI 应用：代理配置、连接统计、抓取开关、轨迹下载、上传试传、网页。"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .engine import DelayProxy, ProxyConfig

STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_CHUNK = 256 * 1024
TRACE_WAIT_SECONDS = 6.0  # 正常连接正在等轨迹落盘时，下载接口的等待上限


def _env_config() -> ProxyConfig:
    return ProxyConfig(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "7000")),
        target_host=os.getenv("TARGET_HOST", "127.0.0.1"),
        target_port=int(os.getenv("TARGET_PORT", "9000")),
        up_delay_ms=float(os.getenv("UP_DELAY_MS", "300")),
        down_delay_ms=float(os.getenv("DOWN_DELAY_MS", "300")),
    )


proxy = DelayProxy(_env_config())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await proxy.apply_config(proxy.cfg)  # 启动监听器
    yield
    await proxy.shutdown()


app = FastAPI(title="TCP Delay Proxy", lifespan=lifespan)


class ConfigIn(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = Field(ge=1, le=65535)
    target_host: str = Field(min_length=1)
    target_port: int = Field(ge=1, le=65535)
    up_delay_ms: float = Field(ge=0, le=3_600_000)
    down_delay_ms: float = Field(ge=0, le=3_600_000)


class CaptureIn(BaseModel):
    enabled: bool


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config():
    snap = proxy.snapshot()
    return {k: snap[k] for k in ("config", "listening", "listen_error", "capture")}


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    await proxy.apply_config(ProxyConfig(**cfg.model_dump()))
    snap = proxy.snapshot()
    return {"ok": snap["listening"],
            **{k: snap[k] for k in ("config", "listening", "listen_error", "capture")}}


@app.post("/api/capture")
async def set_capture(cfg: CaptureIn):
    """抓取开关。只影响之后建立的连接；已有连接沿用建立时的设置。"""
    proxy.set_capture(cfg.enabled)
    snap = proxy.snapshot()
    return {"ok": True, "capture": snap["capture"]}


@app.get("/api/connections")
async def connections():
    return proxy.snapshot()


@app.get("/api/connections/{conn_id}/trace")
async def download_trace(conn_id: int):
    """下载单条连接的轨迹文件（JSON Lines，UTF-8）。

    正常连接必须等轨迹完整落盘后才提供下载；期间短暂等待。若轨迹不完整
    （缓冲溢出、超限、写盘失败、断连/停机），仍返回已有内容，文件名带
    ``.partial``，并附 ``X-Trace-Complete: false`` 与 ``X-Trace-Reason``。
    """
    deadline = time.monotonic() + TRACE_WAIT_SECONDS
    while True:
        info = proxy.find_info(conn_id)
        if info is None or info.trace is None:
            raise HTTPException(404, f"connection {conn_id} has no trace")
        trace = info.trace
        if trace._finalized:
            break
        if info.state == "active":
            # 仍在转发的连接没有最终轨迹可下载
            raise HTTPException(
                409, f"trace for connection {conn_id} is still recording")
        if time.monotonic() >= deadline:
            raise HTTPException(
                409, f"trace for connection {conn_id} is still being written")
        await asyncio.sleep(0.1)

    if trace._available_path is not None:
        path = trace._available_path
    else:
        raise HTTPException(
            410, f"trace file for connection {conn_id} is unavailable: "
                 f"{trace.reason or trace._write_error or 'not written'}")

    complete = trace.status == "complete"
    filename = trace.filename if complete else trace.filename + ".partial"
    headers = {
        "X-Trace-Complete": "true" if complete else "false",
        "X-Trace-Status": trace.status,
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    if trace.reason:
        # HTTP 头只允许 latin-1；中文原因做 RFC 5987 编码
        headers["X-Trace-Reason"] = trace.reason.encode(
            "ascii", "replace").decode("ascii")
        try:
            from urllib.parse import quote
            headers["X-Trace-Reason-Utf8"] = quote(trace.reason, safe="")
        except Exception:
            pass
    return FileResponse(path, media_type="application/x-ndjson", headers=headers)


@app.post("/api/history/clear")
async def clear_history():
    proxy.history.clear()
    return {"ok": True}


async def _read_to_eof(reader: asyncio.StreamReader) -> bytes:
    parts = []
    while True:
        data = await reader.read(65536)
        if not data:
            return b"".join(parts)
        parts.append(data)


@app.post("/api/test-upload")
async def test_upload(file: UploadFile):
    """把上传的文件经本代理转发到目标：先读欢迎行，发完文件后半关闭，
    再读回执直到 EOF。整条路径都经过延迟代理，等同真实设备链路。"""
    snap = proxy.snapshot()
    if not snap["listening"]:
        raise HTTPException(503, f"proxy listener not running: {snap['listen_error']}")
    cfg = proxy.cfg
    delay_s = (cfg.up_delay_ms + cfg.down_delay_ms) / 1000.0
    result: dict = {"file_name": file.filename, "bytes_sent": 0, "ok": False}
    writer = None
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", cfg.listen_port), timeout=15)
        # 目标（示例接收端）先发的欢迎行；自定义目标可能不发，探测失败不阻塞上传
        try:
            welcome = await asyncio.wait_for(reader.readline(),
                                             timeout=max(5.0, delay_s + 5))
            result["welcome"] = welcome.decode("utf-8", "replace").strip()
        except Exception:
            result["welcome"] = None
        while True:
            chunk = await file.read(UPLOAD_CHUNK)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
            result["bytes_sent"] += len(chunk)
        # 半关闭：目标读到 EOF 后才返回回执
        if writer.can_write_eof():
            writer.write_eof()
        else:
            writer.close()
        receipt = await asyncio.wait_for(_read_to_eof(reader),
                                         timeout=max(120.0, 2 * delay_s + 60))
        result["receipt"] = receipt.decode("utf-8", "replace").strip()
        result["ok"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["duration_ms"] = round((time.monotonic() - t0) * 1000, 1)
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        await file.close()
    return result
