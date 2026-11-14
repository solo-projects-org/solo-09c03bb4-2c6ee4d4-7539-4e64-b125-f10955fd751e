"""SQLite 协调层：两个 Web 进程共享同一份缓存账本。

设计要点
--------
- 只记录账本，不碰业务语义；所有写操作走 ``BEGIN IMMEDIATE``，配合 WAL 与
  busy_timeout 实现多进程互斥。
- 容量口径：``cache_entries.size`` 之和（下载预留按源端全长计入，残件是该预留
  的一部分，不重复计账）+ 无主残件的实际字节 = 磁盘真实占用，共享同一上限。
- 空间不足时只淘汰「无人租用（无 lease）的完整文件」，按 LRU 顺序；
  一个都淘汰不了就抛 :class:`CacheBusy`（调用方返回 503 忙）。
- 每次预留生成唯一 ``attempt_id``（fencing token）：下载心跳、协作式失效
  自检、发布、失败清理全部携带该 token。旧尝试即使迟到结束，也无法发布、
  覆盖或删除接替者的文件。
- 下载行/lease 带 owner + heartbeat，进程崩溃后由任意存活进程的 reaper
  「先删残件、后释放预留」；缓存目录内无账本引用的文件按无主残件扫描回收。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass

STATE_DOWNLOADING = "downloading"
STATE_COMPLETE = "complete"


class CacheBusy(Exception):
    """空间不足且没有可淘汰的完整文件（全在用）。"""


class SingleFileTooLarge(Exception):
    """单档全长超过缓存上限，直接拒绝。"""


class VersionGone(Exception):
    """指定 (key, etag) 已不存在、尚未完整或下载尝试已被接管（fencing 失败）。"""


@dataclass(frozen=True)
class ReserveResult:
    """``reserve`` 的结果。

    - ``outcome == "complete"``   ：已完整，直接按 ``path`` 租用即可；
    - ``outcome == "downloading"``：别的持有者正在下载，等待即可；
    - ``outcome == "reserved"``   ：本次调用成功预留，``attempt_id`` 即
      本次下载的 fencing token，调用方负责下载并在所有后续操作中携带。
    """

    outcome: str
    attempt_id: str


@dataclass(frozen=True)
class Entry:
    key: str
    etag: str
    state: str
    path: str
    tmp_path: str | None
    size: int
    owner: str | None
    attempt_id: str
    heartbeat: float | None
    last_used_at: float
    leases: int


class Coordinator:
    """同步 SQLite 协调器；协程通过 ``anyio.to_thread`` 调用。"""

    def __init__(
        self,
        db_path: str,
        limit_bytes: int,
        *,
        cache_dir: str | None = None,
        stale_download_ttl: float = 30.0,
        lease_ttl: float = 30.0,
    ) -> None:
        self.db_path = db_path
        self.limit_bytes = int(limit_bytes)
        # 残件扫描目录；未注入时（纯账本单测）跳过文件系统扫描。
        self.cache_dir = cache_dir
        self.stale_download_ttl = stale_download_ttl
        self.lease_ttl = lease_ttl
        self._fs_lock = threading.Lock()  # 同进程内串行化文件 unlink，避免竞态
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with closing(self._connect()) as conn:
            self._init_schema(conn)

    # ------------------------------------------------------------------基础
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=15.0,
            isolation_level=None,  # 显式事务
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    @staticmethod
    def _init_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                key          TEXT    NOT NULL,
                etag         TEXT    NOT NULL,
                state        TEXT    NOT NULL CHECK (state IN ('downloading','complete')),
                path         TEXT    NOT NULL,
                tmp_path     TEXT,
                size         INTEGER NOT NULL,
                owner        TEXT,
                attempt_id   TEXT    NOT NULL,
                heartbeat    REAL,
                created_at   REAL    NOT NULL,
                last_used_at REAL    NOT NULL,
                PRIMARY KEY (key, etag)
            );
            CREATE TABLE IF NOT EXISTS cache_leases (
                lease_id   TEXT PRIMARY KEY,
                key        TEXT NOT NULL,
                etag       TEXT NOT NULL,
                owner      TEXT NOT NULL,
                created_at REAL NOT NULL,
                heartbeat  REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_leases_entry
                ON cache_leases(key, etag);
            """
        )
        # 旧库迁移：补 attempt_id 列（历史行用随机 token 回填，天然无法被
        # 任何存活下载者持有， fencing 语义对它们一律拒绝）。
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(cache_entries)")}
        if "attempt_id" not in cols:
            conn.execute("ALTER TABLE cache_entries ADD COLUMN attempt_id TEXT")
            conn.execute(
                "UPDATE cache_entries SET attempt_id=? "
                "WHERE attempt_id IS NULL OR attempt_id=''",
                (f"legacy-{uuid.uuid4().hex}",),
            )

    @staticmethod
    def _rollback_safely(conn: sqlite3.Connection) -> None:
        """回滚；若事务已结束（如前面显式回滚过）则忽略。"""
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    @staticmethod
    def _safe_unlink(path: str | None) -> bool:
        """删除文件；返回调用后文件是否确实不存在。"""
        if not path:
            return True
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            return False
        return not os.path.lexists(path)

    def _referenced_paths(self, conn: sqlite3.Connection) -> set[str]:
        refs: set[str] = set()
        for row in conn.execute("SELECT path, tmp_path FROM cache_entries"):
            refs.add(row["path"])
            if row["tmp_path"]:
                refs.add(row["tmp_path"])
        return refs

    def _residue_files(self, conn: sqlite3.Connection) -> list[str]:
        """缓存目录内存在、但账本任何行都不引用的普通文件（无主残件）。"""
        if not self.cache_dir:
            return []
        refs = self._referenced_paths(conn)
        try:
            names = os.listdir(self.cache_dir)
        except FileNotFoundError:
            return []
        out = []
        for name in names:
            full = os.path.join(self.cache_dir, name)
            if full in refs:
                continue
            if os.path.isfile(full):
                out.append(full)
        return out

    def _residue_bytes(self, conn: sqlite3.Connection) -> int:
        total = 0
        for path in self._residue_files(conn):
            try:
                total += os.path.getsize(path)
            except OSError:
                pass
        return total

    # -------------------------------------------------------------预留/下载
    def reserve(
        self,
        key: str,
        etag: str,
        size: int,
        path: str,
        tmp_path: str,
        owner: str,
        attempt_id: str,
    ) -> ReserveResult:
        """为 (key, etag) 预留空间，``attempt_id`` 由调用方生成并终身携带。

        已存在 downloading 行时**绝不代刷心跳**——心跳只允许持有者本人更新，
        否则僵死下载会被等待者“续命”而永远无法回收。
        """
        if size > self.limit_bytes:
            raise SingleFileTooLarge(
                f"对象 {key} 全长 {size} 字节超过单档上限 {self.limit_bytes} 字节"
            )
        now = time.time()
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT state, attempt_id FROM cache_entries "
                    "WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is not None:
                    if row["state"] == STATE_COMPLETE:
                        conn.execute("COMMIT")
                        return ReserveResult(STATE_COMPLETE, row["attempt_id"])
                    conn.execute("COMMIT")
                    return ReserveResult(STATE_DOWNLOADING, row["attempt_id"])

                if not self._ensure_capacity(conn, size):
                    self._rollback_safely(conn)
                    raise CacheBusy(
                        f"缓存已满：预留 {size} 字节，可淘汰的完整文件为 0"
                    )
                conn.execute(
                    """
                    INSERT INTO cache_entries
                        (key, etag, state, path, tmp_path, size, owner,
                         attempt_id, heartbeat, created_at, last_used_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        key,
                        etag,
                        STATE_DOWNLOADING,
                        path,
                        tmp_path,
                        size,
                        owner,
                        attempt_id,
                        now,
                        now,
                        now,
                    ),
                )
                conn.execute("COMMIT")
                return ReserveResult("reserved", attempt_id)
            except Exception:
                self._rollback_safely(conn)
                raise

    def _ensure_capacity(self, conn: sqlite3.Connection, need: int) -> bool:
        """在当前事务内腾出 ``need`` 字节。

        记账口径包含无主残件：残件实际占盘且尚无账本行，必须计入占用；
        有行下载的临时文件是其预留的一部分，不重复计账。腾退顺序：
        1) 先删无主残件（不被任何账本引用，删除绝对安全）；
        2) 仍不足再按 LRU 淘汰无租用的完整文件。
        """
        ledger_total = conn.execute(
            "SELECT COALESCE(SUM(size),0) FROM cache_entries"
        ).fetchone()[0]
        total = ledger_total + self._residue_bytes(conn)
        if total + need <= self.limit_bytes:
            return True
        # 1) 无主残件优先回收：它们不属于任何进行中的下载或完整缓存。
        for orphan in self._residue_files(conn):
            orphan_size = self._safe_size(orphan)
            if self._safe_unlink(orphan):
                total -= orphan_size
            if total + need <= self.limit_bytes:
                return True
        # 2) 仍不足：LRU 淘汰无人租用的完整文件。
        victims = conn.execute(
            """
            SELECT e.key, e.etag, e.path, e.size
            FROM cache_entries e
            WHERE e.state = 'complete'
              AND NOT EXISTS (
                  SELECT 1 FROM cache_leases l
                  WHERE l.key = e.key AND l.etag = e.etag
              )
            ORDER BY e.last_used_at ASC
            """
        ).fetchall()
        freed = 0
        for victim in victims:
            # 事务内 unlink：SQLite 写锁只阻塞其他写者，淘汰量通常很小。
            self._safe_unlink(victim["path"])
            conn.execute(
                "DELETE FROM cache_entries WHERE key=? AND etag=?",
                (victim["key"], victim["etag"]),
            )
            freed += victim["size"]
            if total - freed + need <= self.limit_bytes:
                return True
        return False

    @staticmethod
    def _safe_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0

    def heartbeat_download(
        self, key: str, etag: str, owner: str, attempt_id: str
    ) -> None:
        """仅持有者本人、且 attempt 仍属本尝试时才能刷新心跳。"""
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE cache_entries SET heartbeat=? "
                "WHERE key=? AND etag=? AND owner=? AND attempt_id=? "
                "AND state='downloading'",
                (time.time(), key, etag, owner, attempt_id),
            )

    def check_attempt(self, key: str, etag: str, attempt_id: str) -> bool:
        """协作式 fencing 自检：本尝试是否仍是当前持有者。

        下载循环每块数据前参考本结果；一旦返回 ``False`` 立即停止写入，
        保证被接管的旧下载不会继续落盘。
        """
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT attempt_id, state FROM cache_entries "
                "WHERE key=? AND etag=?",
                (key, etag),
            ).fetchone()
            return (
                row is not None
                and row["state"] == STATE_DOWNLOADING
                and row["attempt_id"] == attempt_id
            )

    def publish_download(
        self,
        key: str,
        etag: str,
        owner: str,
        attempt_id: str,
        tmp_path: str,
    ) -> None:
        """fencing 保护的原子发布：校验归属 → 核长 → rename → 转正，同一事务。

        旧尝试迟到结束时，行要么已不存在、要么归属接替者，本方法直接抛
        :class:`VersionGone`，不会执行 rename，因此不可能覆盖或删除接替者
        的文件，也不会把残件发布成完整缓存。
        """
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if (
                    row is None
                    or row["state"] != STATE_DOWNLOADING
                    or row["owner"] != owner
                    or row["attempt_id"] != attempt_id
                ):
                    self._rollback_safely(conn)
                    raise VersionGone(
                        f"下载尝试已被接管或回收，拒绝发布：{key} {etag} "
                        f"attempt={attempt_id}"
                    )
                try:
                    actual = os.path.getsize(tmp_path)
                except OSError as exc:
                    self._rollback_safely(conn)
                    raise VersionGone(f"临时文件已不存在：{tmp_path}：{exc}")
                if actual != row["size"]:
                    self._rollback_safely(conn)
                    raise VersionGone(
                        f"落盘长度 {actual} 与预留 {row['size']} 不一致"
                    )
                final_path = row["path"]
                # 只有 fencing 校验通过的当前尝试才能走到这个 rename；
                # 文件名带 attempt 前缀，接替者的文件即使已发布也不受影响。
                os.replace(tmp_path, final_path)
                conn.execute(
                    """
                    UPDATE cache_entries
                    SET state='complete', tmp_path=NULL, last_used_at=?
                    WHERE key=? AND etag=? AND attempt_id=?
                    """,
                    (time.time(), key, etag, attempt_id),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def fail_download(
        self,
        key: str,
        etag: str,
        owner: str,
        attempt_id: str,
        tmp_path: str,
        final_path: str,
    ) -> None:
        """下载失败：先确认残件删净，再释放预留。

        全程按 attempt fencing：只有当前行仍归属本尝试时才允许动文件。
        旧尝试被接管后调用本方法是无害空操作——既不会删接替者的成品文件，
        也不会删接替者的账本/临时文件。
        """
        with self._fs_lock:
            with closing(self._connect()) as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    row = conn.execute(
                        "SELECT state, owner, attempt_id, tmp_path, path "
                        "FROM cache_entries WHERE key=? AND etag=?",
                        (key, etag),
                    ).fetchone()
                    # 行已消失（已被 reaper 清理）或归属他人（已被接管）：
                    # 本尝试无权删除任何现存文件，直接退出。
                    if (
                        row is None
                        or row["state"] != STATE_DOWNLOADING
                        or row["owner"] != owner
                        or row["attempt_id"] != attempt_id
                    ):
                        self._rollback_safely(conn)
                        return
                    tmp_gone = self._safe_unlink(tmp_path)
                    final_gone = self._safe_unlink(final_path)
                    if not (tmp_gone and final_gone):
                        # 残件没删净就保留预留行，等下一轮 reap 兜底，绝不虚释放。
                        self._rollback_safely(conn)
                        raise OSError("残件删除失败，保留预留等待 reaper 回收")
                    conn.execute(
                        "DELETE FROM cache_entries "
                        "WHERE key=? AND etag=? AND owner=? AND attempt_id=? "
                        "AND state='downloading'",
                        (key, etag, owner, attempt_id),
                    )
                    conn.execute("COMMIT")
                except Exception:
                    self._rollback_safely(conn)
                    raise

    # ----------------------------------------------------------------租用
    def acquire_lease(
        self, key: str, etag: str, lease_id: str, owner: str
    ) -> None:
        now = time.time()
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT state FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is None or row["state"] != STATE_COMPLETE:
                    self._rollback_safely(conn)
                    raise VersionGone(f"版本不可租用：{key} {etag}")
                conn.execute(
                    """
                    INSERT INTO cache_leases
                        (lease_id, key, etag, owner, created_at, heartbeat)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (lease_id, key, etag, owner, now, now),
                )
                conn.execute(
                    "UPDATE cache_entries SET last_used_at=? WHERE key=? AND etag=?",
                    (now, key, etag),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def release_lease(self, lease_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM cache_leases WHERE lease_id=?", (lease_id,))

    def heartbeat_lease(self, lease_id: str) -> bool:
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "UPDATE cache_leases SET heartbeat=? WHERE lease_id=?",
                (time.time(), lease_id),
            )
            return cur.rowcount > 0

    # -------------------------------------------------------------维护/查询
    def reap(self) -> None:
        """清理僵死下载行与过期 lease，并扫描无主残件。任意存活进程可执行。

        顺序保证：对每个僵死下载，先把残件（临时文件 + 可能已 rename 的
        成品文件）删净，再删除下载行释放预留——账本释放永远晚于残件清理。
        """
        now = time.time()
        with self._fs_lock, closing(self._connect()) as conn:
            # 1) 无主残件扫描（硬崩溃/进程被 kill -9 的遗留），先于账本操作。
            conn.execute("BEGIN IMMEDIATE")
            try:
                for orphan in self._residue_files(conn):
                    self._safe_unlink(orphan)
                stale_dl = conn.execute(
                    "SELECT key, etag, owner, attempt_id, tmp_path, path "
                    "FROM cache_entries "
                    # 僵死判定以心跳为准，但给“刚建档、首条独立心跳尚未落库”
                    # 的下载一个完整 TTL 宽限（created_at 兜底），避免把正在
                    # 建立连接、连接正常但暂无数据的活下载误判失效。
                    "WHERE state='downloading' "
                    "AND heartbeat < ? AND created_at < ?",
                    (
                        now - self.stale_download_ttl,
                        now - self.stale_download_ttl,
                    ),
                ).fetchall()
                for row in stale_dl:
                    tmp_gone = self._safe_unlink(row["tmp_path"])
                    final_gone = self._safe_unlink(row["path"])
                    if not (tmp_gone and final_gone):
                        # 残件仍在（例如权限问题）：本轮不释放预留，下轮再试。
                        continue
                    conn.execute(
                        "DELETE FROM cache_entries "
                        "WHERE key=? AND etag=? AND attempt_id=? "
                        "AND state='downloading' AND heartbeat < ? AND created_at < ?",
                        (
                            row["key"],
                            row["etag"],
                            row["attempt_id"],
                            now - self.stale_download_ttl,
                            now - self.stale_download_ttl,
                        ),
                    )
                conn.execute(
                    "DELETE FROM cache_leases WHERE heartbeat < ?",
                    (now - self.lease_ttl,),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def force_reap_download(self, key: str, etag: str) -> bool:
        """显式回收一个下载行（先删残件、后释放预留）。

        语义与 reaper 对“心跳停摆的死进程”所做处理完全一致，独立成方法是
        为了让演示/测试不必真的等待 TTL 或杀死进程即可确定地制造接管；
        返回该行是否被本调用回收。完整状态的行不受影响。
        """
        with self._fs_lock, closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT key, etag, attempt_id, tmp_path, path, state "
                    "FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is None or row["state"] != STATE_DOWNLOADING:
                    self._rollback_safely(conn)
                    return False
                tmp_gone = self._safe_unlink(row["tmp_path"])
                final_gone = self._safe_unlink(row["path"])
                if not (tmp_gone and final_gone):
                    self._rollback_safely(conn)
                    raise OSError("残件删除失败，保留预留")
                conn.execute(
                    "DELETE FROM cache_entries WHERE key=? AND etag=? AND attempt_id=?",
                    (row["key"], row["etag"], row["attempt_id"]),
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                self._rollback_safely(conn)
                raise

    def get_entry(self, key: str, etag: str) -> Entry | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT e.*,
                       (SELECT COUNT(*) FROM cache_leases l
                         WHERE l.key=e.key AND l.etag=e.etag) AS leases
                FROM cache_entries e
                WHERE e.key=? AND e.etag=?
                """,
                (key, etag),
            ).fetchone()
            return self._to_entry(row) if row else None

    def snapshot(self) -> dict:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT e.*,
                       (SELECT COUNT(*) FROM cache_leases l
                         WHERE l.key=e.key AND l.etag=e.etag) AS leases
                FROM cache_entries e
                ORDER BY e.last_used_at DESC
                """
            ).fetchall()
            ledger_bytes = conn.execute(
                "SELECT COALESCE(SUM(size),0) FROM cache_entries"
            ).fetchone()[0]
            lease_count = conn.execute(
                "SELECT COUNT(*) FROM cache_leases"
            ).fetchone()[0]
            residue_bytes = self._residue_bytes(conn)
        return {
            "limit_bytes": self.limit_bytes,
            # 账本口径：下载预留（全长）+ 完整缓存；临时残件含在所属预留内。
            "ledger_bytes": ledger_bytes,
            # 无主残件：不在任何账本行内、但实际占盘的文件。
            "residue_bytes": residue_bytes,
            # 真实磁盘占用；两类互斥，不存在重复计账。
            "total_bytes": ledger_bytes + residue_bytes,
            "lease_count": lease_count,
            "entries": [self._to_entry(r).__dict__ for r in rows],
        }

    @staticmethod
    def _to_entry(row: sqlite3.Row) -> Entry:
        return Entry(
            key=row["key"],
            etag=row["etag"],
            state=row["state"],
            path=row["path"],
            tmp_path=row["tmp_path"],
            size=row["size"],
            owner=row["owner"],
            attempt_id=row["attempt_id"],
            heartbeat=row["heartbeat"],
            last_used_at=row["last_used_at"],
            leases=row["leases"],
        )
