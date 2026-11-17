#!/usr/bin/env python3
"""抓取功能自测（在 selftest.py 的基础上单独验证轨迹）。

前置：接收端 127.0.0.1:19000，代理 127.0.0.1:17000，网页/API 127.0.0.1:18000。
运行：python3 tests/trace_selftest.py
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import sys

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY = (os.environ.get("PROXY_HOST", "127.0.0.1"),
         int(os.environ.get("PROXY_PORT", "17000")))
TARGET_HOST, TARGET_PORT = os.environ.get("TARGET", "127.0.0.1:19000").rsplit(":", 1)
TRACE_DIR = os.environ.get("TRACE_DIR", "/tmp/tcp-delay-proxy/traces")

failures: list[str] = []


def check(name, cond, detail=""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client, *, up=300, down=300):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY[1],
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down})
    r.raise_for_status()
    assert r.json()["listening"]


async def set_capture(client, enabled):
    r = await client.post("/api/capture", json={"enabled": enabled})
    r.raise_for_status()
    return r.json()["capture"]


async def wait_conn(client, pred, retries=60):
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if pred(c):
                return c
        await asyncio.sleep(0.2)
    return None


async def fetch_trace(client, cid):
    r = await client.get(f"/api/connections/{cid}/trace")
    assert r.status_code == 200, (r.status_code, r.text[:300])
    return r, r.text


def parse(text):
    recs = []
    for line in text.splitlines():
        if line.strip():
            recs.append(json.loads(line))
    return recs


def reassemble(recs, direction):
    return b"".join(base64.b64decode(r["b64"]) for r in recs
                    if r["type"] == "data" and r["dir"] == direction)


async def raw_exchange(payload: bytes, *, rst: bool = False, read_receipt=True):
    """直接连代理，发欢迎行/数据/EOF，返回 (receipt, welcome)。"""
    reader, writer = await asyncio.open_connection(*PROXY)
    welcome = await asyncio.wait_for(reader.readline(), 30)
    writer.write(payload)
    await writer.drain()
    if rst:
        import socket, struct
        sock = writer.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                        struct.pack("ii", 1, 0))
        writer.close()
        return b"", welcome
    writer.write_eof()
    receipt = await asyncio.wait_for(reader.read(), 60) if read_receipt else b""
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return receipt, welcome


async def main():
    global TRACE_DIR
    async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        await set_cfg(client)
        await client.post("/api/history/clear")  # 避免旧连接按字节数误匹配
        TRACE_DIR = (await client.get("/api/config")).json()["capture"]["dir"]
        cap = await set_capture(client, True)
        check("capture enabled", cap["enabled"] is True, str(cap))

        # ---------- 1. 正常连接：内容逐字节一致、相对时间单调、半关闭顺序、完整 ----------
        payload = os.urandom(200 * 1024 + int.from_bytes(os.urandom(2), "big"))
        receipt, welcome = await raw_exchange(payload)
        sha = hashlib.sha256(payload).hexdigest()
        check("receipt correct (forwarding unaffected)", sha in receipt.decode(errors="replace"),
              receipt.decode(errors="replace")[:120])

        conn = await wait_conn(client, lambda c: c["up"]["bytes"] == len(payload)
                               and c["capture"]["status"] in ("complete", "incomplete"))
        check("connection found in history", conn is not None)
        if conn:
            check("trace requested on connection", conn["capture_requested"] is True)
            check("trace status complete", conn["capture"]["status"] == "complete",
                  str(conn["capture"]))
            r, text = await fetch_trace(client, conn["id"])
            check("download reports complete", r.headers.get("x-trace-complete") == "true",
                  str(dict(r.headers)))
            recs = parse(text)
            check("trace has start record", recs[0]["type"] == "start", recs[0]["type"])
            check("trace last record is end/complete",
                  recs[-1]["type"] == "end" and recs[-1]["complete"] is True, str(recs[-1]))
            check("trace up bytes exact", reassemble(recs, "up") == payload,
                  f"{len(reassemble(recs, 'up'))} vs {len(payload)}")
            down = reassemble(recs, "down")
            check("trace down bytes contain welcome+receipt",
                  welcome in down and receipt.strip() in down, down[:80])
            ts = [rec["t"] for rec in recs]
            check("relative times non-decreasing", all(a <= b for a, b in zip(ts, ts[1:])),
                  f"{ts[:3]} … {ts[-3:]}")
            ends = [rec for rec in recs if rec["type"] == "end"][-1]
            check("half-close order up before down",
                  ends.get("half_close_order") == ["up", "down"], str(ends.get("half_close_order")))
            up_eof = [rec["t"] for rec in recs if rec.get("event") == "eof_received" and rec.get("dir") == "up"]
            up_hc = [rec["t"] for rec in recs if rec.get("event") == "half_close_forwarded" and rec.get("dir") == "up"]
            check("up EOF recorded before up half-close",
                  up_eof and up_hc and up_eof[0] <= up_hc[0], f"{up_eof} {up_hc}")
            check("half-close after all up data",
                  up_hc and up_hc[0] >= (recs[-1]["t"] - 3600), "")
            # 上行延迟：第一批数据的相对时间 >= 延迟附近
            first_up = next(rec for rec in recs if rec["type"] == "data" and rec["dir"] == "up")
            check("up data delay honored (>=0.25s)", first_up["t"] >= 0.25, str(first_up["t"]))
            # 轨迹文件不含 .partial 后缀
            check("download filename not partial",
                  ".partial" not in (r.headers.get("content-disposition") or ""),
                  r.headers.get("content-disposition", ""))

        # ---------- 2. 关闭抓取后新连接不抓，已有连接不受影响 ----------
        await set_capture(client, False)
        payload2 = b"no-capture-" + os.urandom(2000 + int.from_bytes(os.urandom(2), "big"))
        receipt2, _ = await raw_exchange(payload2)
        check("receipt ok with capture off", f"bytes={len(payload2)}".encode() in receipt2)
        conn2 = await wait_conn(client, lambda c: c["up"]["bytes"] == len(payload2)
                                and c["state"] in ("closed", "error"))
        check("capture off -> no trace session",
              conn2 is not None and conn2["capture"]["enabled"] is False
              and conn2["capture_requested"] is False, str(conn2 and conn2["capture"]))
        if conn2:
            r = await client.get(f"/api/connections/{conn2['id']}/trace")
            check("download 404 when no trace", r.status_code == 404, str(r.status_code))
        await set_capture(client, True)

        # ---------- 3. 大文件超过 1 MiB：转发正常，轨迹不完整并说明原因 ----------
        big = os.urandom(3 * 1024 * 1024 + int.from_bytes(os.urandom(2), "big"))
        receipt3, _ = await raw_exchange(big)
        check("big upload receipt still correct",
              hashlib.sha256(big).hexdigest() in receipt3.decode(errors="replace"),
              receipt3.decode(errors="replace")[:120])
        conn3 = await wait_conn(client, lambda c: c["up"]["bytes"] == len(big))
        check("big conn found", conn3 is not None)
        if conn3:
            cap3 = conn3["capture"]
            check("big trace status incomplete", cap3["status"] == "incomplete", str(cap3))
            check("big trace reason mentions size limit",
                  "size limit" in (cap3.get("reason") or ""), str(cap3.get("reason")))
            check("big captured bytes <= 1 MiB", cap3["captured_bytes"] <= 1024 * 1024,
                  str(cap3["captured_bytes"]))
            check("big connection state still closed", conn3["state"] == "closed",
                  conn3["state"])
            r, text = await fetch_trace(client, conn3["id"])
            check("big download header incomplete",
                  r.headers.get("x-trace-complete") == "false", str(dict(r.headers)))
            check("big filename marked partial",
                  ".partial" in (r.headers.get("content-disposition") or ""),
                  r.headers.get("content-disposition", ""))
            # 磁盘上不能存在同名的“完整”文件：不完整轨迹始终是 .jsonl.partial
            stem = os.path.join(TRACE_DIR, conn3["capture"]["filename"])
            check("big trace on disk is .partial only",
                  os.path.exists(stem + ".partial") and not os.path.exists(stem),
                  str([stem, stem + ".partial"]))
            recs = parse(text)
            check("big trace ends with complete=false",
                  recs[-1]["type"] == "end" and recs[-1]["complete"] is False
                  and bool(recs[-1]["reason"]), str(recs[-1]))

        # ---------- 4. RST 断连：轨迹不完整，不误标 complete ----------
        rst_receipt, _ = await raw_exchange(b"reset-trace-payload" + os.urandom(1000),
                                            rst=True)
        conn4 = None
        for _ in range(60):
            d = (await client.get("/api/connections")).json()
            hits = [c for c in d["history"] if c["capture_requested"]
                    and c["state"] == "error"]
            if hits:
                conn4 = hits[0]
                break
            await asyncio.sleep(0.2)
        check("RST connection recorded as error", conn4 is not None,
              str(conn4 and conn4["error"]))
        if conn4:
            r, text = await fetch_trace(client, conn4["id"])
            check("RST trace incomplete", r.headers.get("x-trace-complete") == "false",
                  str(dict(r.headers)))
            recs = parse(text)
            check("RST trace not marked complete",
                  recs[-1]["type"] == "end" and recs[-1]["complete"] is False
                  and bool(recs[-1]["reason"]), str(recs[-1]))

        # ---------- 5. 已建立连接沿原设置：开启抓取前建立的连接不抓 ----------
        await set_capture(client, False)
        reader, writer = await asyncio.open_connection(*PROXY)
        await asyncio.wait_for(reader.readline(), 30)
        await set_capture(client, True)  # 连接中途开启
        mid_payload = b"midconn-" + os.urandom(
            500 + int.from_bytes(os.urandom(2), "big"))
        writer.write(mid_payload)
        await writer.drain()
        writer.write_eof()
        await asyncio.wait_for(reader.read(), 60)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        conn5 = await wait_conn(client, lambda c: c["up"]["bytes"] == len(mid_payload)
                                and c["state"] in ("closed", "error"))
        check("connection established before toggle keeps no capture",
              conn5 is not None and conn5["capture_requested"] is False,
              str(conn5 and conn5["capture"]))

        await set_capture(client, False)
        await set_cfg(client)

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL TRACE CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
