"""HTTP 层测试：起真实 uvicorn 服务（等价 compose 里的 web1/web2）。

两个 uvicorn 进程共享同一份 SQLite 账本与缓存目录，验证跨进程协调；
用真实 TCP + httpx 客户端，避开 ASGI 测试 transport 与 StreamingResponse
在 disconnect 语义上的不兼容。
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
from conftest import DAY_START  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def server_env(object_source, cache_paths):
    """启动一个 uvicorn Web 进程，返回 base_url。"""
    base_url, _ = object_source
    port = _free_port()
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=base_url,
        CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
        PROCESS_LABEL="web-1",
        PYTHONPATH=str(ROOT),
    )
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "web.app:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    web_url = f"http://127.0.0.1:{port}"
    _wait_healthy(proc, web_url)
    yield web_url
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture()
def two_server_envs(object_source, cache_paths):
    """两个共享 SQLite 的 uvicorn 进程。"""
    base_url, _ = object_source
    procs, urls = [], []
    for label in ("web-1", "web-2"):
        port = _free_port()
        env = dict(os.environ)
        env.update(
            SQLITE_PATH=cache_paths["sqlite"],
            CACHE_DIR=cache_paths["files"],
            OBJECT_SOURCE_URL=base_url,
            CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
            PROCESS_LABEL=label,
            PYTHONPATH=str(ROOT),
        )
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "web.app:app",
                "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
            ],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        procs.append(proc)
        urls.append(f"http://127.0.0.1:{port}")
    for proc, url in zip(procs, urls):
        _wait_healthy(proc, url)
    yield urls
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _wait_healthy(proc: subprocess.Popen, url: str, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"Web 进程提前退出：\n{out}")
        try:
            r = httpx.get(f"{url}/api/health", timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_err = exc
        time.sleep(0.3)
    raise RuntimeError(f"Web 进程未在 {timeout}s 内就绪：{last_err}")


def test_query_csv_and_lease_release(server_env):
    with httpx.Client(base_url=server_env, timeout=30.0) as client:
        r = client.get("/api/query", params={
            "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60})
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["x-cache-etag"].startswith('"')
        lines = r.text.strip().splitlines()
        # pyarrow 默认给列名加引号。
        assert lines[0] == '"utc_second","temperature_c","salinity_psu"'
        assert len(lines) == 2  # 表头 + 1 行

        snap = client.get(f"{server_env}/api/cache").json()
        assert snap["lease_count"] == 0  # 响应结束租用即释放
        assert any(e["state"] == "complete" for e in snap["entries"])


def test_invalid_window_rejected(server_env):
    r = httpx.get(f"{server_env}/api/query",
                  params={"key": "day1.parquet", "start": 100, "end": 100}, timeout=10)
    assert r.status_code == 400


def test_404_for_unknown_object(server_env):
    r = httpx.get(f"{server_env}/api/query",
                  params={"key": "nope.parquet", "start": DAY_START, "end": DAY_START + 60},
                  timeout=10)
    assert r.status_code == 404


def test_two_instances_share_one_download(two_server_envs):
    url1, url2 = two_server_envs
    with httpx.Client(timeout=30.0) as client:
        results = [
            client.get(f"{url1}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60}),
            client.get(f"{url2}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60}),
        ]
        for r in results:
            assert r.status_code == 200
        assert results[0].headers["x-cache-etag"] == results[1].headers["x-cache-etag"]
        snap = client.get(f"{url1}/api/cache").json()
        assert len([e for e in snap["entries"] if e["key"] == "day1.parquet"]) == 1
        assert snap["lease_count"] == 0
        # 两个进程各自留痕（owner 不同），但文件只有一份。
        labels = {r.headers["x-served-by"] for r in results}
        assert labels == {"web-1", "web-2"}


def test_503_when_cache_full_and_all_busy(object_source, cache_paths):
    base_url, _ = object_source
    port = _free_port()
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=base_url,
        CACHE_LIMIT_BYTES="25000",  # 够一份（约 19KiB），不够两份
        PROCESS_LABEL="web-full",
        PYTHONPATH=str(ROOT),
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "web.app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    web_url = f"http://127.0.0.1:{port}"
    try:
        _wait_healthy(proc, web_url)
        with httpx.Client(timeout=30.0) as client:
            # 先把 day1 拉进缓存。
            ok = client.get(f"{web_url}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60})
            assert ok.status_code == 200
            etag1 = ok.headers["x-cache-etag"]

            # 在测试进程内通过同一本账本持有 day1 的租用，模拟“有读者在用”。
            from cache.coordination import Coordinator
            holder = Coordinator(cache_paths["sqlite"], 25000)
            holder.acquire_lease("day1.parquet", etag1, "test-holder", "test-proc")
            try:
                busy = client.get(f"{web_url}/api/query", params={
                    "key": "day2.parquet", "start": DAY_START, "end": DAY_START + 60})
                assert busy.status_code == 503
                assert "忙" in busy.json()["detail"]
            finally:
                holder.release_lease("test-holder")

            # 读者退出后，LRU 淘汰 day1，day2 可正常下载。
            retry = client.get(f"{web_url}/api/query", params={
                "key": "day2.parquet", "start": DAY_START, "end": DAY_START + 60})
            assert retry.status_code == 200
            snap = client.get(f"{web_url}/api/cache").json()
            assert {e["key"] for e in snap["entries"]} == {"day2.parquet"}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _start_web(label, port, object_url, cache_paths, ttl, demo=False):
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=object_url,
        CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
        STALE_DOWNLOAD_TTL=str(ttl),
        PROCESS_LABEL=label,
        PYTHONPATH=str(ROOT),
    )
    if demo:
        env["DEMO_MODE"] = "1"
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "web.app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    return proc


def test_downloader_killed_peer_reaps_and_retry_succeeds(object_source, cache_paths):
    """web1 下载中被杀死（无清理机会），web2 reaper 回收后重试只下载一次。"""
    base_url, store = object_source
    port1, port2 = _free_port(), _free_port()
    # 短 TTL，让存活进程能快速判定僵死。
    p1 = _start_web("web-kill", port1, base_url, cache_paths, ttl=1.0, demo=True)
    p2 = _start_web("web-live", port2, base_url, cache_paths, ttl=1.0)
    url1, url2 = f"http://127.0.0.1:{port1}", f"http://127.0.0.1:{port2}"
    try:
        _wait_healthy(p1, url1)
        _wait_healthy(p2, url2)

        with httpx.Client(timeout=30.0) as client:
            # 让 web1 的下一次下载冻结在首块前（心跳仍在跑）。
            assert client.post(f"{url1}/api/demo/freeze").status_code == 200

            # 查询在后台线程发起：冻结期间响应头尚未返回，主线程通过缓存
            # 面板观察 downloading 行，而不是阻塞在响应上。
            def _frozen_query():
                try:
                    return client.get(f"{url1}/api/query", params={
                        "key": "day1.parquet",
                        "start": DAY_START, "end": DAY_START + 60})
                except httpx.HTTPError:
                    return None  # 进程随后会被杀死，连接错误是预期的

            qthread = threading.Thread(target=_frozen_query, daemon=True)
            qthread.start()

            deadline = time.time() + 10
            downloading = False
            while time.time() < deadline:
                snap = client.get(f"{url1}/api/cache").json()
                if any(e["key"] == "day1.parquet" and e["state"] == "downloading"
                       for e in snap["entries"]):
                    downloading = True
                    break
                time.sleep(0.2)
            assert downloading, "冻结的下载行未在时限内出现"

            # 直接杀死下载进程，模拟“被回收/崩溃”，它没有机会清理。
            p1.kill()
            p1.wait(timeout=10)
            qthread.join(timeout=5)

            # web2 存活：其 reaper 在 ~TTL+reap 间隔内回收僵死行与残件。
            deadline = time.time() + 25
            recovered = False
            while time.time() < deadline:
                snap = client.get(f"{url2}/api/cache").json()
                dl = [e for e in snap["entries"] if e["key"] == "day1.parquet"]
                if not dl:
                    recovered = True
                    break
                time.sleep(0.5)
            assert recovered, "存活进程未在时限内回收僵死下载"
            assert snap["residue_bytes"] == 0

            # 重试：同档同版由 web2 重新预留接管并成功，仅一次有效下载。
            store._gets = 0
            original = store.stream

            def counting(key, write, flush):
                store._gets = getattr(store, "_gets", 0) + 1
                return original(key, write, flush)

            store.stream = counting
            r = client.get(f"{url2}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60})
            assert r.status_code == 200
            assert len(r.text.strip().splitlines()) == 2
            assert store._gets == 1  # 重试只触发一次源端 GET
            snap = client.get(f"{url2}/api/cache").json()
            assert len(snap["entries"]) == 1
            assert snap["entries"][0]["state"] == "complete"
            assert snap["residue_bytes"] == 0
    finally:
        if p1.poll() is None:
            p1.kill()
        p2.terminate()
        for proc in (p1, p2):
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
