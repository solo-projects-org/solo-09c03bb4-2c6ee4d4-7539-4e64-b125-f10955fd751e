"""端到端演示编排（本地进程版，不依赖 Docker）。

覆盖题目要求的三个场景：

1. ``--scenario slow``       慢下载：源端首块前挂住，连接正常但暂无数据，
                             独立心跳保活，下载不被误判，且不阻塞其他日档。
2. ``--scenario retry``      下载进程中途失败退出（残件 + 预留），新进程
                             重试时由 reaper 清残件、释放预留后接管，同档
                             同版只产生一次有效下载。
3. ``--scenario takeover``   旧下载僵在首块前被强制接管；旧下载“迟到结束”
                             也无法发布、覆盖或删除接替者文件（fencing）。

前置：已按 README 生成样例并启动对象源（默认 http://127.0.0.1:8080）。
本脚本只负责编排 Web 侧的两个 CacheManager（等价 compose 里的 web1/web2）。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cache.coordination import Coordinator  # noqa: E402
from cache.manager import CacheManager  # noqa: E402


def _make_manager(base_url: str, workdir: Path, label: str, ttl: float) -> CacheManager:
    coord = Coordinator(
        str(workdir / "coord" / "cache.db"),
        64 * 1024 * 1024,
        cache_dir=str(workdir / "files"),
        stale_download_ttl=ttl,
    )
    return CacheManager(coord, base_url, str(workdir / "files"), owner=label)


async def scenario_slow(base_url: str, key: str, workdir: Path) -> None:
    import urllib.request

    # 源端首块前挂 6 秒：连接已建立、HTTP 头已返回，但暂时无数据。
    body = (
        '{"key": "%s", "delay_per_chunk": 0.0, '
        '"fail_after_bytes": 0, "hold_seconds": 6.0}' % key
    ).encode()
    req = urllib.request.Request(
        f"{base_url}/admin/simulate", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()

    mgr = _make_manager(base_url, workdir, "web-demo", ttl=30.0)
    await mgr.start()
    try:
        etag, size = await mgr.head_etag(key)
        print(f"[slow] HEAD {key} etag={etag[:18]}… size={size}，开始下载（源端将挂 6s）")
        lease = await mgr.ensure(key, etag, size)
        snap = await mgr.snapshot()
        states = [(e["key"], e["state"]) for e in snap["entries"]]
        print(f"[slow] 下载完成且未被回收：{states}，残件={snap['residue_bytes']} B")
        await mgr.release(lease)
    finally:
        urllib.request.urlopen(
            urllib.request.Request(f"{base_url}/admin/simulate/{key}", method="DELETE"),
            timeout=10,
        ).read()
        await mgr.close()


async def scenario_retry(base_url: str, key: str, workdir: Path) -> None:
    import urllib.request

    # 让“旧进程”的下载在 512 字节处被源端切断。
    body = ('{"key": "%s", "fail_after_bytes": 512}' % key).encode()
    req = urllib.request.Request(
        f"{base_url}/admin/simulate", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()

    old = _make_manager(base_url, workdir, "web-old", ttl=5.0)
    await old.start()
    try:
        etag, size = await old.head_etag(key)
        try:
            await old.ensure(key, etag, size)
        except Exception as exc:  # 传输失败，旧进程就此退出
            print(f"[retry] 旧下载进程失败退出：{type(exc).__name__}")
    finally:
        await old.close()

    files = workdir / "files"
    leftovers = [f for f in os.listdir(files)]
    print(f"[retry] 旧进程退出后缓存目录内容：{leftovers}")

    # 新进程：reaper 兜底清残件，随后重新预留接管。
    urllib.request.urlopen(
        urllib.request.Request(f"{base_url}/admin/simulate/{key}", method="DELETE"),
        timeout=10,
    ).read()
    new = _make_manager(base_url, workdir, "web-new", ttl=5.0)
    await new.start()
    try:
        await new.reap()
        etag, size = await new.head_etag(key)
        lease = await new.ensure(key, etag, size)
        snap = await new.snapshot()
        print(
            f"[retry] 新进程接管成功：条目数={len(snap['entries'])} "
            f"残件={snap['residue_bytes']} B 状态={snap['entries'][0]['state']}"
        )
        await new.release(lease)
    finally:
        await new.close()


async def scenario_takeover(base_url: str, key: str, workdir: Path) -> None:
    old = _make_manager(base_url, workdir, "web-old", ttl=30.0)
    new = _make_manager(base_url, workdir, "web-new", ttl=30.0)
    await old.start()
    await new.start()
    try:
        etag, size = await old.head_etag(key)
        freeze = old.install_freeze_hook()
        old_task = asyncio.create_task(old.ensure(key, etag, size))
        await asyncio.sleep(0.5)
        print("[takeover] 旧下载已冻结在首块前（连接保持、心跳续期中）")

        # 存活方强制回收旧下载行（等价 reaper 对心跳停摆死进程的处理）。
        reaped = await asyncio.to_thread(old.coord.force_reap_download, key, etag)
        print(f"[takeover] 旧下载行被强制回收：{reaped}")

        lease = await new.ensure(key, etag, size)
        good = Path(lease.path).read_bytes()
        print(f"[takeover] 接替者已发布：{lease.path}（{len(good)} B）")

        freeze.set()  # 旧下载解冻、迟到结束
        old_lease = await asyncio.wait_for(old_task, timeout=30)
        await old.release(old_lease)

        untouched = Path(lease.path).read_bytes() == good
        parquets = [f for f in os.listdir(workdir / "files") if f.endswith(".parquet")]
        snap = await new.snapshot()
        print(
            f"[takeover] 旧下载迟到结束后：接替者文件未被覆盖={untouched}，"
            f"成品文件={parquets}，完整条目={sum(1 for e in snap['entries'] if e['state']=='complete')}，"
            f"残件={snap['residue_bytes']} B"
        )
        await new.release(lease)
    finally:
        await old.close()
        await new.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="下载回收三场景演示")
    parser.add_argument(
        "--scenario", choices=("slow", "retry", "takeover"), required=True
    )
    parser.add_argument("--source", default="http://127.0.0.1:8080")
    parser.add_argument(
        "--key", default="buoy-st-ts-1735689600.parquet",
        help="对象 key（需已由 generate_samples.py 生成）",
    )
    parser.add_argument("--workdir", default="", help="缓存工作目录，默认临时目录")
    args = parser.parse_args()

    if args.workdir:
        workdir = Path(args.workdir)
        shutil.rmtree(workdir, ignore_errors=True)
        workdir.mkdir(parents=True, exist_ok=True)
        keep = True
    else:
        workdir = Path(tempfile.mkdtemp(prefix="buoy-demo-"))
        keep = False

    runners = {
        "slow": scenario_slow,
        "retry": scenario_retry,
        "takeover": scenario_takeover,
    }
    try:
        asyncio.run(runners[args.scenario](args.source.rstrip("/"), args.key, workdir))
    finally:
        if not keep:
            shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
