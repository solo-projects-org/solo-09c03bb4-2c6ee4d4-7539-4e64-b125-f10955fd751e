"""SQLite 协调层的单元测试。

覆盖：容量与预留、LRU 淘汰、租用保护、失败清理、reaper 僵死回收、
attempt fencing（旧尝试不能发布/覆盖/删除接替者）、残件记账与不重复计账。
"""

from __future__ import annotations

import os
import time

import pytest

from cache.coordination import (
    STATE_COMPLETE,
    CacheBusy,
    Coordinator,
    ReserveResult,
    SingleFileTooLarge,
    VersionGone,
)


def make_coord(tmp_path, limit=1000, ttl=0.05):
    files = tmp_path / "files"
    files.mkdir(exist_ok=True)
    return (
        Coordinator(
            str(tmp_path / "c.db"),
            limit,
            cache_dir=str(files),
            stale_download_ttl=ttl,
            lease_ttl=ttl,
        ),
        files,
    )


def reserve(coord, key, etag, size, files, owner, attempt=None):
    attempt = attempt or f"att-{owner}-{key}"
    path = str(files / f"{key}.parquet")
    tmp = str(files / f".{key}.{owner}.{attempt}.part")
    return coord.reserve(key, etag, size, path, tmp, owner, attempt), path, tmp


def complete_entry(coord, key, etag, size, files, owner, attempt=None):
    """预留 → 写满临时文件 → fencing 发布，返回成品路径。"""
    attempt = attempt or f"att-{owner}-{key}"
    _, path, tmp = reserve(coord, key, etag, size, files, owner, attempt)
    with open(tmp, "wb") as fh:
        fh.write(b"x" * size)
    coord.publish_download(key, etag, owner, attempt, tmp)
    return path


def _tmp_of(coord, key, etag):
    return coord.get_entry(key, etag).tmp_path


def test_single_file_over_limit_rejected(tmp_path):
    coord, files = make_coord(tmp_path, limit=500)
    with pytest.raises(SingleFileTooLarge):
        reserve(coord, "k", '"a"', 600, files, "o1")


def test_reserve_total_counts_download_against_limit(tmp_path):
    coord, files = make_coord(tmp_path, limit=1000)
    r, _, _ = reserve(coord, "k1", '"a"', 600, files, "o1")
    assert r.outcome == "reserved"
    # 剩余 400：更大的预留应失败（没有可淘汰对象）。
    with pytest.raises(CacheBusy):
        reserve(coord, "k2", '"b"', 500, files, "o2")


def test_eviction_only_touches_unleased_complete_files_lru(tmp_path):
    coord, files = make_coord(tmp_path, limit=1000)
    complete_entry(coord, "k1", '"a"', 400, files, "o", attempt="a0")
    complete_entry(coord, "k2", '"b"', 400, files, "o", attempt="a1")
    coord.acquire_lease("k1", '"a"', "lease-1", "reader")
    with pytest.raises(CacheBusy):
        reserve(coord, "k3", '"c"', 700, files, "o2")
    assert coord.get_entry("k1", '"a"') is not None  # 在用文件保住了
    coord.release_lease("lease-1")
    r, _, _ = reserve(coord, "k3", '"c"', 700, files, "o2")
    assert r.outcome == "reserved"
    assert coord.get_entry("k1", '"a"') is None or coord.get_entry("k2", '"b"') is None


def test_eviction_succeeds_when_enough_space(tmp_path):
    coord, files = make_coord(tmp_path, limit=1000)
    complete_entry(coord, "k1", '"a"', 800, files, "o")
    r, _, _ = reserve(coord, "k2", '"b"', 500, files, "o2")
    assert r.outcome == "reserved"
    assert coord.get_entry("k1", '"a"') is None


def test_same_key_same_etag_single_download(tmp_path):
    coord, files = make_coord(tmp_path)
    first, _, _ = reserve(coord, "k", '"a"', 100, files, "o1")
    second, _, _ = reserve(coord, "k", '"a"', 100, files, "o2")
    assert first.outcome == "reserved"
    assert second.outcome == "downloading"  # 后来者不重复预留
    # 后来者拿到的仍是首个尝试的 token。
    assert second.attempt_id == first.attempt_id


def test_waiter_must_not_refresh_peer_heartbeat(tmp_path):
    coord, files = make_coord(tmp_path, ttl=0.05)
    first, _, _ = reserve(coord, "k", '"a"', 100, files, "o1")
    hb0 = coord.get_entry("k", '"a"').heartbeat
    time.sleep(0.02)
    # 后来者预留只应观察，不应代刷心跳。
    coord.reserve(
        "k", '"a"', 100,
        coord.get_entry("k", '"a"').path,
        coord.get_entry("k", '"a"').tmp_path,
        "o2", "att-o2-k",
    )
    assert coord.get_entry("k", '"a"').heartbeat == hb0


def test_fail_download_removes_residue_and_releases(tmp_path):
    coord, files = make_coord(tmp_path)
    _, _, tmp = reserve(coord, "k", '"a"', 100, files, "o1")
    with open(tmp, "wb") as fh:
        fh.write(b"partial")
    coord.fail_download("k", '"a"', "o1", "att-o1-k", tmp, str(files / "k.parquet"))
    assert not os.path.exists(tmp)
    assert coord.get_entry("k", '"a"') is None
    # 释放后可以重新预留。
    r, _, _ = reserve(coord, "k", '"a"', 100, files, "o2")
    assert r.outcome == "reserved"


def test_publish_checks_length_owner_and_attempt(tmp_path):
    coord, files = make_coord(tmp_path)
    _, path, tmp = reserve(coord, "k", '"a"', 100, files, "o1")
    with open(tmp, "wb") as fh:
        fh.write(b"short")
    # 长度不符不能发布。
    with pytest.raises(VersionGone):
        coord.publish_download("k", '"a"', "o1", "att-o1-k", tmp)
    # 别人/别的 attempt 不能发布。
    with pytest.raises(VersionGone):
        coord.publish_download("k", '"a"', "someone-else", "att-o1-k", tmp)
    with pytest.raises(VersionGone):
        coord.publish_download("k", '"a"', "o1", "stolen-attempt", tmp)
    with open(tmp, "wb") as fh:
        fh.write(b"x" * 100)
    coord.publish_download("k", '"a"', "o1", "att-o1-k", tmp)
    assert coord.get_entry("k", '"a"').state == STATE_COMPLETE
    assert os.path.exists(path) and not os.path.exists(tmp)


def test_lease_blocks_eviction_then_release_allows(tmp_path):
    coord, files = make_coord(tmp_path, limit=500)
    complete_entry(coord, "k", '"a"', 400, files, "o")
    coord.acquire_lease("k", '"a"', "L1", "r")
    with pytest.raises(CacheBusy):
        reserve(coord, "k2", '"b"', 400, files, "o2")
    coord.release_lease("L1")
    r, _, _ = reserve(coord, "k2", '"b"', 400, files, "o2")
    assert r.outcome == "reserved"


def test_reap_clears_stale_download_and_lease(tmp_path):
    coord, files = make_coord(tmp_path, limit=2000)
    _, _, tmp = reserve(coord, "k", '"a"', 100, files, "dead-owner")
    with open(tmp, "wb") as fh:
        fh.write(b"z")
    _, _, tmp2 = reserve(coord, "k2", '"b"', 100, files, "o")
    with open(tmp2, "wb") as fh:
        fh.write(b"x" * 100)
    coord.publish_download("k2", '"b"', "o", "att-o-k2", tmp2)
    coord.acquire_lease("k2", '"b"', "L", "r")
    time.sleep(0.1)
    coord.reap()
    assert coord.get_entry("k", '"a"') is None
    assert not os.path.exists(tmp)
    assert coord.snapshot()["lease_count"] == 0
    assert coord.get_entry("k2", '"b"').state == STATE_COMPLETE

def test_reap_deletes_residue_before_releasing_reservation(tmp_path):
    coord, files = make_coord(tmp_path, ttl=0.05)
    _, _, tmp = reserve(coord, "k", '"a"', 100, files, "dead")
    with open(tmp, "wb") as fh:
        fh.write(b"partial-bytes")
    time.sleep(0.1)
    coord.reap()
    # 残件先删、预留后释放：reap 后两者都应消失。
    assert not os.path.exists(tmp)
    assert coord.get_entry("k", '"a"') is None
    assert coord.snapshot()["total_bytes"] == 0


def test_stale_attempt_cannot_publish_after_takeover(tmp_path):
    """核心 fencing：旧尝试迟到结束不能发布/覆盖接替者文件。"""
    coord, files = make_coord(tmp_path, ttl=0.05)
    # 旧尝试预留并写了一半。
    _, old_final, old_tmp = reserve(
        coord, "k", '"a"', 100, files, "old", attempt="att-old"
    )
    with open(old_tmp, "wb") as fh:
        fh.write(b"o" * 100)
    time.sleep(0.1)
    coord.reap()  # 旧尝试被回收，残件删除。
    assert coord.get_entry("k", '"a"') is None

    # 接替者重新预留、写满并发布。
    _, new_final, new_tmp = reserve(
        coord, "k", '"a"', 100, files, "new", attempt="att-new"
    )
    with open(new_tmp, "wb") as fh:
        fh.write(b"n" * 100)
    coord.publish_download("k", '"a"', "new", "att-new", new_tmp)

    # 旧尝试迟到结束：发布必须被拒，且接替者文件内容不变。
    with pytest.raises(VersionGone):
        coord.publish_download("k", '"a"', "old", "att-old", old_tmp)
    with open(new_final, "rb") as fh:
        assert fh.read() == b"n" * 100
    # 旧尝试的失败清理也是无害空操作，不能删掉接替者。
    coord.fail_download(
        "k", '"a"', "old", "att-old", old_tmp, old_final
    )
    assert coord.get_entry("k", '"a"').state == STATE_COMPLETE
    assert os.path.exists(new_final)


def test_orphan_residue_counts_but_not_double_counted(tmp_path):
    """无主残件计入占用；有行下载的临时文件不与其预留重复计账。"""
    coord, files = make_coord(tmp_path, limit=1000)
    # 一个正在下载的预留 400（其临时文件属于该预留，不重复计）。
    _, _, tmp = reserve(coord, "k1", '"a"', 400, files, "o1")
    with open(tmp, "wb") as fh:
        fh.write(b"x" * 300)
    snap = coord.snapshot()
    assert snap["ledger_bytes"] == 400
    assert snap["residue_bytes"] == 0
    assert snap["total_bytes"] == 400

    # 模拟硬崩溃留下的无主残件：账本无引用，应单独计入且不与预留重复。
    orphan = files / ".crash.orphan.part"
    orphan.write_bytes(b"r" * 200)
    snap = coord.snapshot()
    assert snap["ledger_bytes"] == 400
    assert snap["residue_bytes"] == 200
    assert snap["total_bytes"] == 600  # 400 预留 + 200 残件，无重复

    # 再预留 700：即使先回收 200 无主残件，账本 400 + 700 = 1100 仍超限，应忙。
    with pytest.raises(CacheBusy):
        reserve(coord, "k2", '"b"', 700, files, "o2")
    # 无主残件回收是安全的文件系统操作，不随 SQL 事务回滚，已被清掉。
    assert not orphan.exists()
    assert coord.snapshot()["residue_bytes"] == 0

    # 残件已腾出空间，500 的预留（400+500=900 ≤ 1000）即可成功。
    r, _, _ = reserve(coord, "k2", '"b"', 500, files, "o2")
    assert r.outcome == "reserved"


def test_capacity_reclaims_orphan_residue_before_eviction(tmp_path):
    """空间不足时先安全回收无主残件，而不是直接淘汰/报忙。"""
    coord, files = make_coord(tmp_path, limit=1000)
    # 进行中预留 400；另有无主残件 200；真实占用 600。
    reserve(coord, "k1", '"a"', 400, files, "o1")
    (files / ".crash.orphan.part").write_bytes(b"r" * 200)
    # 再要 500：账本 400 + 残件 200 + 500 = 1100 超限；
    # 删掉无主残件后 400 + 500 = 900 可行，不应 CacheBusy。
    r, _, _ = reserve(coord, "k2", '"b"', 500, files, "o2")
    assert r.outcome == "reserved"
    assert not (files / ".crash.orphan.part").exists()
    assert coord.snapshot()["residue_bytes"] == 0


def test_reserve_returns_result_object(tmp_path):
    coord, files = make_coord(tmp_path)
    r, _, _ = reserve(coord, "k", '"a"', 100, files, "o1")
    assert isinstance(r, ReserveResult)
