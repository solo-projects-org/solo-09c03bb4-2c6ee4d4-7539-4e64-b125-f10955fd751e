"""端到端：对象源 → 协调下载 → PyArrow 分批筛取 → CSV。

用 asyncio.run 驱动，避免额外的 pytest-asyncio 依赖。
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from cache.coordination import CacheBusy, Coordinator, SingleFileTooLarge
from cache.manager import CacheManager, ETagMismatch
from web.query import stream_filtered_csv

from conftest import DAY_START


def _manager(base_url, cache_paths, limit, **kw):
    coord = Coordinator(
        cache_paths["sqlite"], limit, cache_dir=cache_paths["files"], **kw
    )
    return CacheManager(coord, base_url, cache_paths["files"], wait_timeout=30)


def _collect_csv(path, start, end):
    return b"".join(stream_filtered_csv(path, start, end))


def _row_count(csv_bytes: bytes) -> int:
    table = pacsv.read_csv(io.BytesIO(csv_bytes))
    return table.num_rows


def test_head_etag_is_strong_and_stable(object_source, cache_paths):
    base_url, store = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            etag2, size2 = await mgr.head_etag("day1.parquet")
            assert not etag1.startswith(("W/", "w/"))
            assert etag1 == etag2 and size1 == size2
            assert size1 == os.path.getsize(os.path.join(store.data_dir, "day1.parquet"))
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_full_flow_window_half_open(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            lease = await mgr.ensure("day1.parquet", etag, size)
            start, end = DAY_START, DAY_START + 3600  # 首小时
            csv_bytes = await asyncio.to_thread(_collect_csv, lease.path, start, end)
            await mgr.release(lease)
        finally:
            await mgr.close()

        # 60s 采样：窗口 [0, 3600) 应有 60 行（不含终点）。
        assert _row_count(csv_bytes) == 60
        table = pacsv.read_csv(io.BytesIO(csv_bytes))
        utc = table.column("utc_second").to_pylist()
        assert utc[0] == start
        assert utc[-1] < end
        assert min(utc) >= start and max(utc) < end
        assert table.column_names == ["utc_second", "temperature_c", "salinity_psu"]
    asyncio.run(scenario())


def test_empty_window_returns_header_only(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day2.parquet")
            lease = await mgr.ensure("day2.parquet", etag, size)
            # 落在 day1 范围的窗口对 day2 必为空。
            out = await asyncio.to_thread(
                _collect_csv, lease.path, DAY_START, DAY_START + 60
            )
            await mgr.release(lease)
        finally:
            await mgr.close()
        assert b"utc_second" in out
        assert _row_count(out) == 0
    asyncio.run(scenario())


def test_concurrent_same_version_downloads_once(object_source, cache_paths):
    base_url, store = object_source
    downloads = {"count": 0}
    original_stream = store.stream

    def counting_stream(key, write, flush):
        downloads["count"] += 1
        return original_stream(key, write, flush)

    store.stream = counting_stream
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            leases = await asyncio.gather(
                *[mgr.ensure("day1.parquet", etag, size) for _ in range(5)]
            )
            assert len(leases) == 5
            snap = await mgr.snapshot()
            assert sum(1 for e in snap["entries"] if e["key"] == "day1.parquet") == 1
            assert snap["lease_count"] == 5
            for lease in leases:
                await mgr.release(lease)
        finally:
            await mgr.close()
    asyncio.run(scenario())
    assert downloads["count"] == 1


def test_download_failure_deletes_residue_and_releases(object_source, cache_paths):
    base_url, store = object_source
    store.set_simulation("day1.parquet", delay=0.0, fail_after=512)
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            with pytest.raises((ConnectionAbortedError, httpx.HTTPError, ETagMismatch)):
                await mgr.ensure("day1.parquet", etag, size)
            snap = await mgr.snapshot()
            assert snap["total_bytes"] == 0 and snap["entries"] == []
            leftovers = [f for f in os.listdir(cache_paths["files"]) if f.endswith((".part", ".tmp"))]
            assert leftovers == []
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_etag_change_midflight_fails_and_recovers(object_source, cache_paths, object_dir):
    base_url, store = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag_v1, size_v1 = await mgr.head_etag("day1.parquet")
            # 模拟“查询固定版本后、GET 前源端更正”：替换对象内容。
            corrected = object_dir / "day1.corrected.parquet"
            payload = corrected.read_bytes()
            new_meta = store.correct("day1.parquet", payload)
            assert new_meta["etag"] != etag_v1

            with pytest.raises(ETagMismatch):
                await mgr.ensure("day1.parquet", etag_v1, size_v1)
            # 残件与预留都已清掉。
            snap = await mgr.snapshot()
            assert snap["entries"] == []

            # 新查询重新 HEAD 固定新版本，成功。
            etag_v2, size_v2 = await mgr.head_etag("day1.parquet")
            assert etag_v2 == new_meta["etag"]
            lease = await mgr.ensure("day1.parquet", etag_v2, size_v2)
            out = await asyncio.to_thread(
                _collect_csv, lease.path, DAY_START, DAY_START + 120
            )
            assert _row_count(out) == 2
            await mgr.release(lease)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_cache_full_single_file_rejected(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 1024)  # 小于任何日档
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            with pytest.raises(SingleFileTooLarge):
                await mgr.ensure("day1.parquet", etag, size)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_cache_full_busy_when_all_in_use(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        # 上限可放下一份（约 19 KiB），但放不下两份。
        mgr = _manager(base_url, cache_paths, 25_000)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            lease1 = await mgr.ensure("day1.parquet", etag1, size1)  # 占满且在用
            etag2, size2 = await mgr.head_etag("day2.parquet")
            with pytest.raises(CacheBusy):
                await mgr.ensure("day2.parquet", etag2, size2)
            # 释放读者后，LRU 淘汰 day1，day2 可下载。
            await mgr.release(lease1)
            lease2 = await mgr.ensure("day2.parquet", etag2, size2)
            snap = await mgr.snapshot()
            keys = {e["key"] for e in snap["entries"]}
            assert keys == {"day2.parquet"}
            await mgr.release(lease2)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_slow_download_does_not_block_other_days(slow_object_source, object_dir, tmp_path):
    base_url, store = slow_object_source
    # day1 每块睡 0.3s，day2 正常。
    store.set_simulation("day1.parquet", delay=0.3, fail_after=0)
    paths = {
        "sqlite": str(tmp_path / "coord2" / "c.db"),
        "files": str(tmp_path / "files2"),
    }
    async def scenario():
        mgr = _manager(base_url, paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            slow = asyncio.create_task(mgr.ensure("day1.parquet", etag1, size1))
            await asyncio.sleep(0.2)  # 让慢下载先跑起来
            etag2, size2 = await mgr.head_etag("day2.parquet")
            t0 = time.monotonic()
            lease2 = await mgr.ensure("day2.parquet", etag2, size2)
            elapsed = time.monotonic() - t0
            await lease2.release(mgr)
            assert elapsed < 5.0  # 未被慢日档堵住
            lease1 = await slow
            await lease1.release(mgr)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_parquet_filter_pushdown_matches_full_scan(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            lease = await mgr.ensure("day1.parquet", etag, size)
            start, end = DAY_START + 7200, DAY_START + 10800
            out = await asyncio.to_thread(_collect_csv, lease.path, start, end)
            full = pq.read_table(lease.path)
            utc = full.column("utc_second")
            expect = full.filter(
                pa.compute.and_(
                    pa.compute.greater_equal(utc, pa.scalar(start)),
                    pa.compute.less(utc, pa.scalar(end)),
                )
            ).num_rows
            await mgr.release(lease)
            assert _row_count(out) == expect
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_slow_connection_with_no_data_is_not_reaped(slow_object_source, cache_paths):
    """连接正常但长时间无数据：独立心跳保活，不能被 reaper 误判失效。"""
    base_url, store = slow_object_source
    # TTL 3s（远大于 2s 心跳节拍）；源端首块前挂 2.5s，跨多个心跳周期。
    mgr = _manager(
        base_url, cache_paths, 10 * 1024 * 1024, stale_download_ttl=3.0
    )
    store.set_simulation("day1.parquet", delay=0.0, fail_after=0, hold=2.5)

    async def scenario():
        await mgr.start()
        reaper = asyncio.create_task(_reap_forever(mgr, interval=0.2))
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            t0 = time.monotonic()
            lease = await mgr.ensure("day1.parquet", etag, size)
            assert time.monotonic() - t0 >= 2.0  # 确实经历了无数据窗口
            # 下载未被回收：恰好一个完整条目，无残件、无重复预留。
            snap = await mgr.snapshot()
            assert [e["key"] for e in snap["entries"]] == ["day1.parquet"]
            assert snap["entries"][0]["state"] == "complete"
            assert snap["residue_bytes"] == 0
            await mgr.release(lease)
        finally:
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper
            await mgr.close()
            store.clear_simulation("day1.parquet")

    asyncio.run(scenario())


def test_retry_after_downloader_process_exit_takes_over(tmp_path, object_dir):
    """下载进程中途退出（kill -9 语义）：reaper 清残件释放预留，重试只下载一次。"""
    import socket
    import subprocess
    import sys
    import threading
    from http.server import ThreadingHTTPServer

    import object_source.server as srv
    from object_source.server import Handler

    store = srv.ObjectStore(str(object_dir))
    srv.STORE = store
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base_url = f"http://127.0.0.1:{port}"

    paths = {
        "sqlite": str(tmp_path / "coord" / "c.db"),
        "files": str(tmp_path / "files"),
    }
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=paths["sqlite"], CACHE_DIR=paths["files"],
        OBJECT_SOURCE_URL=base_url, CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
        STALE_DOWNLOAD_TTL="0.5", PYTHONPATH=str(root),
    )
    # 源端中途断开，让子进程下载失败并留下残件后退出。
    store.set_simulation("day1.parquet", delay=0.0, fail_after=512)
    script = (
        "import asyncio, sys; sys.path.insert(0, %r);"
        "from cache.coordination import Coordinator;"
        "from cache.manager import CacheManager;"
        "async def main():\n"
        " c=Coordinator(%r, %d, cache_dir=%r, stale_download_ttl=0.5);"
        " m=CacheManager(c, %r, %r); await m.start();"
        " etag,size=await m.head_etag('day1.parquet');"
        " await m.ensure('day1.parquet', etag, size)\n"
        "asyncio.run(main())"
        % (
            str(root), paths["sqlite"], 10 * 1024 * 1024, paths["files"],
            base_url, paths["files"],
        )
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script], env=env, capture_output=True, text=True,
            timeout=30,
        )
        assert proc.returncode != 0  # 下载确实失败退出
        # 子进程的失败清理已删残件；即使没删成，新进程 reaper 也会兜底。
        coord = Coordinator(
            paths["sqlite"], 10 * 1024 * 1024,
            cache_dir=paths["files"], stale_download_ttl=0.5,
        )
        coord.reap()
        leftovers = [
            f for f in os.listdir(paths["files"]) if f.endswith((".part", ".tmp"))
        ]
        assert leftovers == []

        # 新进程重试：源端恢复后应成功，且只产生一次有效下载。
        store.clear_simulation("day1.parquet")
        gets = {"n": 0}
        original = store.stream

        def counting(key, write, flush):
            gets["n"] += 1
            return original(key, write, flush)

        store.stream = counting

        async def scenario():
            mgr = _manager(base_url, paths, 10 * 1024 * 1024, stale_download_ttl=0.5)
            await mgr.start()
            try:
                etag, size = await mgr.head_etag("day1.parquet")
                lease = await mgr.ensure("day1.parquet", etag, size)
                out = await asyncio.to_thread(
                    _collect_csv, lease.path, DAY_START, DAY_START + 120
                )
                assert _row_count(out) == 2
                await mgr.release(lease)
                snap = await mgr.snapshot()
                assert len(snap["entries"]) == 1
                assert snap["residue_bytes"] == 0
            finally:
                await mgr.close()

        asyncio.run(scenario())
        assert gets["n"] == 1  # 同档同版重试只下载一次
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_late_finishing_old_download_cannot_publish(slow_object_source, object_dir, tmp_path):
    """旧下载被接管后迟到结束：不能发布、覆盖或删除接替者文件。

    场景：旧 manager 的下载挂在首块前（模拟进程心跳停摆/僵死），存活方
    强制回收该下载行并由新 manager 接管下载发布；随后旧下载解冻“迟到
    结束”，fencing 必须保证它既不能发布也不能覆盖/删除接替者的文件。
    """
    base_url, store = slow_object_source
    paths = {
        "sqlite": str(tmp_path / "coord3" / "c.db"),
        "files": str(tmp_path / "files3"),
    }
    os.makedirs(paths["files"], exist_ok=True)

    async def scenario():
        # 旧 manager：冻结钩子让下载建立连接后挂在首块前。
        old = _manager(base_url, paths, 10 * 1024 * 1024, stale_download_ttl=5.0)
        await old.start()
        freeze = old.install_freeze_hook()
        etag, size = await old.head_etag("day1.parquet")
        old_task = asyncio.create_task(old.ensure("day1.parquet", etag, size))
        await asyncio.sleep(0.4)  # 让旧下载进入冻结状态

        # 存活方强制回收旧下载行（等价 reaper 对心跳停摆死进程的处理）。
        reaped = await asyncio.to_thread(
            old.coord.force_reap_download, "day1.parquet", etag
        )
        assert reaped is None or reaped is True
        assert await asyncio.to_thread(
            old.coord.get_entry, "day1.parquet", etag
        ) is None

        # 新 manager 接管并完成下载、发布。
        new = _manager(base_url, paths, 10 * 1024 * 1024, stale_download_ttl=5.0)
        await new.start()
        lease = await new.ensure("day1.parquet", etag, size)
        successor = lease.path
        with open(successor, "rb") as fh:
            good = fh.read()
        assert good
        successor_attempt = (await new.snapshot())["entries"][0]["attempt_id"]

        # 旧下载解冻、迟到结束：其 fencing 自检失败，旧尝试放弃发布。
        freeze.set()
        # 旧 ensure 会以新 attempt 在同一 manager 内重新接管，最终成功，
        # 但它绝不能覆盖已发布的接替者文件。
        old_lease = await asyncio.wait_for(old_task, timeout=20)
        with open(successor, "rb") as fh:
            assert fh.read() == good  # 接替者文件字节未被旧尝试改动
        await old.release(old_lease)
        await old.close()

        # 账本始终只有一个完整条目、零残件，成品路径唯一。
        snap = await new.snapshot()
        complete = [e for e in snap["entries"] if e["state"] == "complete"]
        assert len(complete) == 1
        assert complete[0]["attempt_id"] == successor_attempt
        assert snap["residue_bytes"] == 0
        parquets = [f for f in os.listdir(paths["files"]) if f.endswith(".parquet")]
        assert parquets == [os.path.basename(successor)]
        out = await asyncio.to_thread(
            _collect_csv, successor, DAY_START, DAY_START + 60
        )
        assert _row_count(out) == 1
        await new.release(lease)
        await new.close()

    asyncio.run(scenario())


async def _reap_forever(mgr, interval):
    while True:
        await mgr.reap()
        await asyncio.sleep(interval)
