"""缓存管理器：把对象源、SQLite 账本和文件落盘粘起来。

关键承诺
========
- 每次查询先取源端强 ETag 并固定该版本；取档时 ETag 不匹配即失败。
- 同档同版全集群只下载一次：先到者 ``reserve`` 成功负责下载，后来者
  在条件变量/SQLite 轮询上等待，不产生重复流量；持有者被回收后，
  下一次请求以新的 attempt 重新预留接管，同档同版仍只产生一次有效下载。
- 下载按源端 ``Content-Length`` 全长预留空间；失败先确认残件删净再释放预留。
- 心跳由独立后台任务按固定节拍发出，**与是否收到数据无关**：连接正常但
  源端长时间不发数据的慢下载不会被误判失效；只有心跳真正中断（进程退出/
  卡死）才会被 reaper 回收。
- 每次下载带唯一 fencing token（``attempt_id``）：被接管的旧尝试在每块
  数据前自检，发现失去持有权立即停止写入；其迟到结束也无法发布、覆盖或
  删除接替者的文件（发布是带 token 校验的原子 rename+转正）。
- 慢源/慢下载只阻塞同一 (key, etag) 的等待者，其他日档的查询照常推进。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import uuid
from dataclasses import dataclass

import httpx

from .coordination import (
    STATE_COMPLETE,
    Coordinator,
    VersionGone,
)

logger = logging.getLogger("buoy.cache")

# 单次“预留 → 下载”的最大尝试次数（含首次）。接管重试到上限后向调用方报错。
MAX_DOWNLOAD_ATTEMPTS = 5
# 心跳节拍；必须显著小于协调器 stale_download_ttl。
HEARTBEAT_INTERVAL = 2.0
# 每块数据前的 fencing 自检节拍。
FENCE_CHECK_INTERVAL = 1.0


class ETagMismatch(RuntimeError):
    """下载流中的 ETag 与查询开始时固定的版本不一致。"""


class DownloadAttemptsExhausted(RuntimeError):
    """多次接管/重试后下载仍未完成（下载者持续崩溃）。"""


class DownloadFenced(VersionGone):
    """本下载尝试已被 reaper 接管，协作式中止（不代表整体查询失败）。"""


@dataclass
class Lease:
    lease_id: str
    key: str
    etag: str
    path: str

    async def release(self, manager: "CacheManager") -> None:
        await manager.release(self)


class CacheManager:
    def __init__(
        self,
        coordinator: Coordinator,
        object_base_url: str,
        cache_dir: str,
        *,
        owner: str | None = None,
        wait_timeout: float = 120.0,
        poll_interval: float = 0.25,
        read_timeout: float = 300.0,
    ) -> None:
        self.coord = coordinator
        self.object_base_url = object_base_url.rstrip("/")
        self.cache_dir = cache_dir
        self.owner = owner or f"{os.uname().nodename}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.wait_timeout = wait_timeout
        self.poll_interval = poll_interval
        self.read_timeout = read_timeout
        self._client: httpx.AsyncClient | None = None
        # 演示钩子：下载循环在首个数据块前等待此事件；测试用于制造
        # “旧下载僵住 → 被接管 → 旧下载迟到结束”的竞态。生产路径不设置。
        self._freeze_event: asyncio.Event | None = None
        # 同进程等待者挂在这个条件上；跨进程等待者靠轮询 SQLite。
        self._entry_changed: asyncio.Condition | None = None
        os.makedirs(cache_dir, exist_ok=True)

    def _condition(self) -> asyncio.Condition:
        cond = self._entry_changed
        if cond is None:
            cond = asyncio.Condition()
            self._entry_changed = cond
        return cond

    # -------------------------------------------------------------测试/演示
    def install_freeze_hook(self) -> asyncio.Event:
        """安装“首块前冻结”钩子，返回供外部释放的事件（仅演示/测试使用）。"""
        self._freeze_event = asyncio.Event()
        return self._freeze_event

    def clear_freeze_hook(self) -> None:
        event = self._freeze_event
        if event is not None:
            event.set()
        self._freeze_event = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.object_base_url,
            # read 超时给慢源充分余量：真正的失效由心跳/reaper 判定，
            # 正常连接内长时间无数据不会在传输层被误杀。
            timeout=httpx.Timeout(
                connect=10.0, read=self.read_timeout, write=30.0, pool=10.0
            ),
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    # -------------------------------------------------------------元数据
    async def head_etag(self, key: str) -> tuple[str, int]:
        """HEAD 取强 ETag 与全长。强 ETag 形如 ``"..."``（无 ``W/`` 前缀）。"""
        assert self._client is not None
        resp = await self._client.head(f"/objects/{key}")
        if resp.status_code == 404:
            raise FileNotFoundError(f"对象不存在：{key}")
        resp.raise_for_status()
        etag = resp.headers.get("etag", "")
        if not etag:
            raise RuntimeError(f"源端未返回 ETag：{key}")
        if etag.startswith(("W/", "w/")):
            raise RuntimeError(f"源端仅提供弱 ETag，无法固定版本：{etag}")
        length = int(resp.headers.get("content-length", "0"))
        if length <= 0:
            raise RuntimeError(f"源端未返回有效 Content-Length：{key}")
        return etag, length

    # -------------------------------------------------------------取档主流程
    async def ensure(self, key: str, etag: str, size: int) -> Lease:
        """保证 (key, etag) 完整落盘并返回租用句柄。

        同档同版全程只允许一个有效下载：行存在即等待；行被回收则由本
        调用以新 attempt 重新预留（接管），循环次数有界。
        """
        for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
            attempt_id = uuid.uuid4().hex
            safe_name = key.replace("/", "_")
            tag = _etag_tag(etag)
            # 成品路径按 (key, etag) 确定；临时文件带 attempt 前缀，
            # 不同尝试/不同进程绝不共享临时文件名。
            path = os.path.join(self.cache_dir, f"{safe_name}__{tag}.parquet")
            tmp_path = os.path.join(
                self.cache_dir,
                f".{safe_name}__{tag}.{os.getpid()}.{attempt_id}.part",
            )

            result = await asyncio.to_thread(
                self.coord.reserve,
                key, etag, size, path, tmp_path, self.owner, attempt_id,
            )
            if result.outcome == "complete":
                return await self._lease(key, etag, path)
            if result.outcome == "downloading":
                # 等待当前持有者；它失败/被回收导致行消失时，回到循环
                # 顶部以新 attempt 接管，而不是直接判查询失败。
                await self._wait_for_peer(key, etag)
                entry = await asyncio.to_thread(self.coord.get_entry, key, etag)
                if entry is not None and entry.state == STATE_COMPLETE:
                    return await self._lease(key, etag, path)
                logger.info(
                    "%s@%s 的下载行已释放，第 %d 次尝试接管", key, etag, attempt
                )
                continue

            # outcome == "reserved"：本进程本任务负责下载。
            try:
                await self._download(
                    key, etag, size, path, tmp_path, attempt_id, attempt
                )
            except DownloadFenced:
                # 本尝试已被接管：清理动作（若有）由 reaper/接替者负责，
                # 直接回到循环等待或重新预留，绝不发布任何文件。
                logger.warning(
                    "下载 %s@%s attempt=%s 已被接管，停止写入并退出本尝试",
                    key, etag, attempt_id,
                )
                await self._notify()
                continue
            except Exception:
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(
                        self.coord.fail_download,
                        key, etag, self.owner, attempt_id, tmp_path, path,
                    )
                await self._notify()
                raise
            await asyncio.to_thread(
                self.coord.publish_download,
                key, etag, self.owner, attempt_id, tmp_path,
            )
            await self._notify()
            return await self._lease(key, etag, path)

        raise DownloadAttemptsExhausted(
            f"{key}@{etag} 经过 {MAX_DOWNLOAD_ATTEMPTS} 次下载尝试仍未完成"
        )

    async def _download(
        self,
        key: str,
        etag: str,
        size: int,
        path: str,
        tmp_path: str,
        attempt_id: str,
        attempt_no: int,
    ) -> None:
        assert self._client is not None
        written = 0
        last_fence_check = 0.0
        fenced = asyncio.Event()

        async def heartbeat_loop() -> None:
            """独立心跳：只要本进程事件环活着就固定节拍续期，与数据无关。"""
            while not fenced.is_set():
                try:
                    await asyncio.to_thread(
                        self.coord.heartbeat_download,
                        key, etag, self.owner, attempt_id,
                    )
                except Exception:
                    logger.exception("下载心跳失败 attempt=%s", attempt_id)
                try:
                    await asyncio.wait_for(fenced.wait(), timeout=HEARTBEAT_INTERVAL)
                except asyncio.TimeoutError:
                    continue

        hb_task = asyncio.create_task(heartbeat_loop())
        # 临时文件名含 pid + attempt uuid，独占创建：不同尝试/进程绝不共享
        # 临时文件，账本登记的 tmp_path 就是真实落盘文件，残件扫描可识别。
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(tmp_path, flags, 0o600)
        succeeded = False
        try:
            async with self._client.stream("GET", f"/objects/{key}") as resp:
                if resp.status_code == 404:
                    raise FileNotFoundError(f"下载时对象已消失：{key}")
                resp.raise_for_status()
                got_etag = resp.headers.get("etag", "")
                if got_etag != etag:
                    raise ETagMismatch(
                        f"取档 ETag {got_etag!r} 与固定版本 {etag!r} 不一致"
                    )
                got_len = int(resp.headers.get("content-length", "0"))
                if got_len and got_len != size:
                    raise ETagMismatch(
                        f"源端长度 {got_len} 与 HEAD 全长 {size} 不一致，对象已变更"
                    )

                # 演示钩子：在首个数据块前挂起本下载（连接已建立、心跳在跑）。
                if self._freeze_event is not None:
                    logger.info("演示冻结生效：%s attempt=%s 等待释放", key, attempt_id)
                    await self._freeze_event.wait()

                with open(tmp_path, "wb") as fh:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                        # 协作式 fencing：每块前确认本尝试仍持有下载权。
                        now = time.monotonic()
                        if now - last_fence_check >= FENCE_CHECK_INTERVAL:
                            still_mine = await asyncio.to_thread(
                                self.coord.check_attempt, key, etag, attempt_id
                            )
                            if not still_mine:
                                fenced.set()
                                raise DownloadFenced(
                                    f"attempt {attempt_id} 已失去 {key}@{etag} 持有权"
                                )
                            last_fence_check = now
                        fh.write(chunk)
                        written += len(chunk)
                        if written > size:
                            raise ETagMismatch(
                                f"实际写入 {written} 超过预留 {size}，对象已变更"
                            )
                    fh.flush()
                    os.fsync(fh.fileno())
            if written != size:
                raise VersionGone(f"实际下载 {written} 字节 != 预留 {size}")
            # 发布（含 fencing 校验 + rename + 转正）交给 ensure 统一执行；
            # 此处绝不自行 rename，杜绝迟到尝试覆盖接替者。
            succeeded = True
        finally:
            fenced.set()
            hb_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await hb_task
            try:
                os.close(fd)
            except OSError:
                pass
            # 成功：临时文件保留给 publish_download 的原子 rename 带走。
            # 失败/被接管：仅当本尝试仍持有下载权时清理自己的临时文件
            # （临时名含 attempt uuid，不可能命中接替者）；失去持有权则
            # 交由 reaper 处理，这里不越界删除任何文件。
            if succeeded:
                return
            still_mine = False
            with contextlib.suppress(Exception):
                still_mine = await asyncio.to_thread(
                    self.coord.check_attempt, key, etag, attempt_id
                )
            if still_mine:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(tmp_path)

    async def _wait_for_peer(self, key: str, etag: str) -> None:
        """等待别的持有者完成下载。本地事件优先，跨进程靠 SQLite 轮询。"""
        deadline = time.monotonic() + self.wait_timeout
        while True:
            entry = await asyncio.to_thread(self.coord.get_entry, key, etag)
            if entry is None:
                # 下载者失败/被回收已释放预留：由调用方重新预留接管。
                return
            if entry.state == STATE_COMPLETE:
                return
            if time.monotonic() > deadline:
                raise DownloadAttemptsExhausted(
                    f"等待 {key}@{etag} 下载超时（{self.wait_timeout}s）"
                )
            cond = self._condition()
            try:
                async with cond:
                    await asyncio.wait_for(cond.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _notify(self) -> None:
        cond = self._condition()
        async with cond:
            cond.notify_all()

    async def _lease(self, key: str, etag: str, path: str) -> Lease:
        lease_id = uuid.uuid4().hex
        await asyncio.to_thread(
            self.coord.acquire_lease, key, etag, lease_id, self.owner
        )
        return Lease(lease_id=lease_id, key=key, etag=etag, path=path)

    async def release(self, lease: Lease) -> None:
        await asyncio.to_thread(self.coord.release_lease, lease.lease_id)

    async def snapshot(self) -> dict:
        return await asyncio.to_thread(self.coord.snapshot)

    async def reap(self) -> None:
        await asyncio.to_thread(self.coord.reap)


def _etag_tag(etag: str) -> str:
    return etag.strip('"').replace("/", "_")
