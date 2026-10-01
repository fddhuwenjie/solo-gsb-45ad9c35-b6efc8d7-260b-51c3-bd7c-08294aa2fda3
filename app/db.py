"""SQLite 持久层：不可变特征库版本、判定记录、现场设备批次链与草稿回放报告。

特征库以版本为单位不可变存储：

- ``library_versions``：版本元数据，状态机 draft → active → historical；
- ``library_version_aids``：每个版本的完整航标快照（版本不可变，可复算差异）；
- ``library_state``：单行表，指向当前激活版本。

草稿回放报告（上线前评估草稿会如何改写既有判定）：

- ``replay_reports``：报告元数据——目标草稿版本、全局唯一的幂等键、开始时
  冻结的判定水位（当前最大 ``judgment_id``）与各类计数；
- ``replay_report_items``：逐条明细——服务端库记录保存新旧最佳候选证据与
  四分类（``unchanged`` / ``match_gained`` / ``match_lost`` /
  ``winner_changed``），内联库记录以 ``item_status='skipped'`` 加跳过原因
  保存。幂等键全局唯一（唯一约束兜底并发）；回放不写入 ``judgments``。

现场设备批次链：

- ``device_batches``：每个设备按 sequence 递增的批次记录（含观测时间戳、
  /judge 四项阈值、批次摘要与首次受理响应），(device_id, sequence) 与
  (device_id, batch_id) 均唯一。``status`` 固定为首次受理事实 ``accepted``，
  另以 ``replay_count`` / ``last_replayed_at`` 持久化幂等重放；
- ``device_chains``：每个设备的链头游标（last_sequence、head_digest）与
  最近一次事件（受理或重放）的状态/序号/时刻。

多下游消费者确认与安全归档水位：

- ``device_consumers``：每台设备的**必需消费者**登记（注册后不可删除），
  并缓存该消费者的连续确认水位 ``watermark``（已连续确认到的最大序号，
  初始 0；归档之后才登记的消费者从设备 ``archived_through`` 起步）；
  (device_id, consumer_id) 唯一；
- ``device_batch_acks``：每个消费者对批次的**稀疏确认**（只保存已确认的
  序号与当时摘要），(device_id, consumer_id, sequence) 唯一。

确认在单个事务内「校验消费者与批次 → 插入稀疏确认 → 把该消费者水位推进
至首个缺口」；写锁 + BEGIN IMMEDIATE 串行化并发确认，水位在事务内基于
最新状态整体重算，不会丢失推进。设备的 ``safe_sequence`` 取所有必需
消费者水位的最小值（无消费者或任一消费者水位为 0 时为 0）。

批次归档（清除已被全部必需消费者处理的连续前缀的明细载荷）：

- ``device_chains.archived_through``：已归档连续前缀的右端序号（单调不
  退，初始 0），``archived_at`` 为最近一次归档时刻；
- ``device_batches.archived_at`` 非空即**墓碑**：``status='archived'``，
  ``observations`` / ``thresholds`` 明细被清空为 NULL，仅保留
  ``batch_id``、序号、``previous_digest``、``digest``、条数、首次受理
  时刻/响应与重放统计。归档目标必须落在同一写事务内核验的
  ``safe_sequence`` 之内，越界抛 :class:`ArchiveConflict`（409，附当前
  水位与阻塞消费者）；重复或更小目标幂等。已归档批次仍可按摘要接受同
  载荷重放并累计次数，载荷变化仍返回 409；链头摘要与后续上传不变。

批次提交在单个事务内完成「幂等/链头校验 → 追加记录 → 更新链头」；重放也在
同一幂等事务内原子累加 ``replay_count`` 并刷新批次与链游标的最近事件——
不新增批次、不推进链头、不改写首次受理响应。写锁 + BEGIN IMMEDIATE +
唯一约束保证并发提交串行化，不会形成分叉、不会丢失重放计数。

连续批次归档前巡检判定：

- ``batch_judgments``：一次区间判定的不可变来源——全局唯一幂等键、设备、
  起止序号、固定的 ``library_version``、合并观测与四项阈值、底层
  ``judgments.id``；
- ``batch_judgment_batches``：区间内各批的摘要（序号、batch_id、前序/本批
  摘要、条数）。创建在单个 BEGIN IMMEDIATE 事务内完成幂等检查、区间校验
  （缺批次/墓碑/阈值不一致整体拒绝）、固定激活版本、判定与落库；同键重试
  返回原判定，改范围/换设备 409；与上传/归档共用写锁串行点，已保存结果
  不会引用半个范围，且批次随后归档不影响已保存来源的复核。

首次启动时把旧版平铺 ``aids`` 表整体迁移为初始激活版本（source='migration'），
并给 ``judgments`` 表补充 ``library_version`` 列；同时为旧库的批次表补充
``replay_count`` / ``last_replayed_at`` / ``archived_at`` 列（明细列改为
可空以承载归档墓碑）、为链游标表补充最近事件三列与
``archived_through`` / ``archived_at`` 列（旧记录重放次数为零、归档水位
回填为 0，最近事件回填为链头批次的受理事件）。批次表补墓碑列需要
SQLite 表重建，迁移在单个事务内完成。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .batches import canonical_payload, compute_digest, merge_batch_observations

SCHEMA = """
CREATE TABLE IF NOT EXISTS library_versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    status       TEXT NOT NULL CHECK (status IN ('draft', 'active', 'historical')),
    base_version INTEGER REFERENCES library_versions(version_id),
    source       TEXT NOT NULL DEFAULT 'api',
    created_at   REAL NOT NULL,
    activated_at REAL
);
CREATE TABLE IF NOT EXISTS library_version_aids (
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id),
    aid_id     TEXT NOT NULL,
    period     REAL NOT NULL,
    flashes    TEXT NOT NULL,
    PRIMARY KEY (version_id, aid_id)
);
CREATE TABLE IF NOT EXISTS library_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id)
);
CREATE TABLE IF NOT EXISTS judgments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    best_aid_id     TEXT,
    matched         INTEGER NOT NULL DEFAULT 0,
    missed          INTEGER NOT NULL DEFAULT 0,
    false_count     INTEGER NOT NULL DEFAULT 0,
    drift           REAL,
    library_version INTEGER,
    request         TEXT NOT NULL,
    response        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_chains (
    device_id           TEXT PRIMARY KEY,
    last_sequence       INTEGER NOT NULL,
    head_digest         TEXT NOT NULL,
    batch_count         INTEGER NOT NULL,
    updated_at          REAL NOT NULL,
    last_event_status   TEXT NOT NULL DEFAULT 'accepted'
        CHECK (last_event_status IN ('accepted', 'replayed')),
    last_event_sequence INTEGER NOT NULL DEFAULT 0,
    last_event_at       REAL NOT NULL DEFAULT 0.0,
    archived_through    INTEGER NOT NULL DEFAULT 0,
    archived_at         REAL
);
CREATE TABLE IF NOT EXISTS device_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    sequence          INTEGER NOT NULL,
    batch_id          TEXT NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    thresholds        TEXT,
    observations      TEXT,
    response          TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'archived')),
    replay_count      INTEGER NOT NULL DEFAULT 0,
    last_replayed_at  REAL,
    archived_at       REAL,
    created_at        REAL NOT NULL,
    UNIQUE (device_id, sequence),
    UNIQUE (device_id, batch_id)
);
CREATE TABLE IF NOT EXISTS device_consumers (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id      TEXT NOT NULL,
    consumer_id    TEXT NOT NULL,
    watermark      INTEGER NOT NULL DEFAULT 0,
    registered_at  REAL NOT NULL,
    UNIQUE (device_id, consumer_id)
);
CREATE TABLE IF NOT EXISTS device_batch_acks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    consumer_id      TEXT NOT NULL,
    sequence         INTEGER NOT NULL,
    batch_digest     TEXT NOT NULL,
    confirmed_at     REAL NOT NULL,
    UNIQUE (device_id, consumer_id, sequence)
);
CREATE TABLE IF NOT EXISTS replay_reports (
    report_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES library_versions(version_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    watermark       INTEGER NOT NULL,
    total_count     INTEGER NOT NULL,
    replayed_count  INTEGER NOT NULL,
    skipped_count   INTEGER NOT NULL,
    unchanged       INTEGER NOT NULL DEFAULT 0,
    match_gained    INTEGER NOT NULL DEFAULT 0,
    match_lost      INTEGER NOT NULL DEFAULT 0,
    winner_changed  INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS replay_report_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id           INTEGER NOT NULL REFERENCES replay_reports(report_id),
    judgment_id         INTEGER NOT NULL,
    item_status         TEXT NOT NULL
        CHECK (item_status IN ('replayed', 'skipped')),
    classification      TEXT
        CHECK (classification IS NULL OR classification IN
               ('unchanged', 'match_gained', 'match_lost', 'winner_changed')),
    skip_reason         TEXT,
    old_status          TEXT,
    old_best_aid_id     TEXT,
    old_reason          TEXT,
    old_library_version INTEGER,
    old_best            TEXT,
    new_status          TEXT,
    new_best_aid_id     TEXT,
    new_reason          TEXT,
    new_best            TEXT,
    UNIQUE (report_id, judgment_id)
);
CREATE INDEX IF NOT EXISTS idx_replay_items_cursor
    ON replay_report_items (report_id, judgment_id);
CREATE TABLE IF NOT EXISTS batch_judgments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL UNIQUE,
    device_id        TEXT NOT NULL,
    from_sequence    INTEGER NOT NULL,
    to_sequence      INTEGER NOT NULL,
    library_version  INTEGER,
    observation_count INTEGER NOT NULL,
    thresholds       TEXT NOT NULL,
    observations     TEXT NOT NULL,
    judgment_id      INTEGER NOT NULL REFERENCES judgments(id),
    created_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batch_judgments_device
    ON batch_judgments (device_id, id);
CREATE TABLE IF NOT EXISTS batch_judgment_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_judgment_id INTEGER NOT NULL REFERENCES batch_judgments(id),
    sequence          INTEGER NOT NULL,
    batch_id          TEXT NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    UNIQUE (batch_judgment_id, sequence)
);
"""


class VersionNotFound(Exception):
    """引用的版本不存在。"""


class VersionStateError(Exception):
    """版本状态不允许该操作（如激活非草稿、以草稿为基线）。"""


class VersionConflict(Exception):
    """激活时草稿的 base_version 已不再是当前激活版本（乐观并发冲突）。"""


class ReplayConflict(Exception):
    """回放幂等键已被另一个版本占用（HTTP 409），detail 附占用方信息。"""

    def __init__(
        self,
        *,
        message: str,
        idempotency_key: str,
        occupied_by_version: int,
        occupied_by_report: int,
    ):
        super().__init__(message)
        self.detail = {
            "error": "replay_conflict",
            "reason": "idempotency_key_in_use",
            "message": message,
            "idempotency_key": idempotency_key,
            "occupied_by_version": occupied_by_version,
            "occupied_by_report": occupied_by_report,
        }


class BatchConflict(Exception):
    """批次上传冲突（HTTP 409）：载荷变化或链头过期，detail 附期望值。"""

    def __init__(
        self,
        *,
        reason: str,
        message: str,
        expected_sequence: int,
        expected_previous_digest: str,
        existing: dict | None = None,
    ):
        super().__init__(message)
        self.detail = {
            "error": "batch_conflict",
            "reason": reason,
            "message": message,
            "expected_sequence": expected_sequence,
            "expected_previous_digest": expected_previous_digest,
        }
        if existing is not None:
            self.detail["existing_batch"] = existing


class ConsumerConflict(Exception):
    """消费者确认冲突（HTTP 409）：摘要不符或消费者未登记，detail 附上下文。"""

    def __init__(self, *, reason: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "consumer_conflict", "reason": reason, "message": message}
        self.detail.update(extra)


class BatchNotFound(Exception):
    """确认的批次在该设备上不存在（HTTP 404）。"""

    def __init__(self, device_id: str, sequence: int):
        super().__init__(f"batch {sequence} of device {device_id!r} not found")
        self.device_id = device_id
        self.sequence = sequence


class ArchiveConflict(Exception):
    """归档目标越过同一事务内核验的 safe_sequence（HTTP 409），detail 附水位上下文。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "archive_conflict", "message": message}
        self.detail.update(extra)


class BatchJudgmentConflict(Exception):
    """连续批次判定冲突（HTTP 409）：范围非法或幂等键已被其他范围/设备占用。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "batch_judgment_conflict", "message": message}
        self.detail.update(extra)


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _diff_aids(base_aids: list[dict], new_aids: list[dict]) -> dict:
    """按航标编号比较两个快照，给出增/删/改（period 或 flashes 不同即为改）。"""
    base = {a["aid_id"]: a for a in base_aids}
    new = {a["aid_id"]: a for a in new_aids}
    return {
        "added": sorted(k for k in new if k not in base),
        "removed": sorted(k for k in base if k not in new),
        "modified": sorted(
            k for k in new.keys() & base.keys()
            if base[k]["period"] != new[k]["period"] or base[k]["flashes"] != new[k]["flashes"]
        ),
    }


class Database:
    """按操作开启连接的轻量封装：串行写由锁保证，多步写包在单个事务内。"""

    def __init__(self, path: str):
        self._path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._migrate(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        # 写锁竞争时等待而非立即抛 database is locked，使
        # 「BEGIN IMMEDIATE 串行化写事务」在多连接（多 worker）下同样成立
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def _write_tx(self):
        """串行化的单事务写：BEGIN IMMEDIATE … COMMIT，异常时 ROLLBACK。"""
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 首次迁移 ----------------

    def _migrate(self, conn) -> None:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "aids" in tables:  # 旧版平铺特征库 → 初始激活版本
            rows = conn.execute(
                "SELECT aid_id, period, flashes FROM aids ORDER BY aid_id"
            ).fetchall()
            now = time.time()
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', NULL, 'migration', ?, ?)",
                (now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, r["aid_id"], r["period"], r["flashes"]) for r in rows],
            )
            conn.execute("INSERT INTO library_state (id, version_id) VALUES (1, ?)", (vid,))
            conn.execute("DROP TABLE aids")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(judgments)")}
        if "library_version" not in cols:
            conn.execute("ALTER TABLE judgments ADD COLUMN library_version INTEGER")
        self._migrate_device_tables(conn)

    def _migrate_device_tables(self, conn) -> None:
        """旧库兼容：批次表补重放/归档列，链表补最近事件与归档水位列。

        旧记录的重放次数为零（``last_replayed_at`` 为 NULL）、归档水位为 0
        （``archived_at`` 为 NULL）；链游标最近事件回填为当前链头批次的受理
        事件，与新批次受理后的游标含义保持一致。归档要求批次明细列
        （``observations``/``thresholds``）可空并新增 ``archived_at`` 墓碑
        列，旧库这两列为 NOT NULL 且无墓碑列，SQLite 不能直接放宽 NOT NULL，
        故按官方做法在同一事务内「建新表 → 拷数据 → 删旧表 → 改名」重建
        （(device_id, sequence) / (device_id, batch_id) 唯一约束随新表保留）。
        """
        batch_info = {
            r[1]: r for r in conn.execute("PRAGMA table_info(device_batches)")
        }
        need_rebuild = bool(batch_info) and (
            "archived_at" not in batch_info
            or batch_info["observations"][3]  # notnull 标志（PRAGMA 第 4 列）
            or batch_info["thresholds"][3]
        )
        if need_rebuild:
            # 先把旧库能直接 ALTER 补上的列补齐，重建时随数据一起搬过去
            if "replay_count" not in batch_info:
                conn.execute(
                    "ALTER TABLE device_batches ADD COLUMN "
                    "replay_count INTEGER NOT NULL DEFAULT 0"
                )
            if "last_replayed_at" not in batch_info:
                conn.execute(
                    "ALTER TABLE device_batches ADD COLUMN last_replayed_at REAL"
                )
            conn.execute(
                """
                CREATE TABLE device_batches__new (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id         TEXT NOT NULL,
                    sequence          INTEGER NOT NULL,
                    batch_id          TEXT NOT NULL,
                    previous_digest   TEXT NOT NULL,
                    digest            TEXT NOT NULL,
                    observation_count INTEGER NOT NULL,
                    thresholds        TEXT,
                    observations      TEXT,
                    response          TEXT NOT NULL,
                    status            TEXT NOT NULL DEFAULT 'accepted'
                        CHECK (status IN ('accepted', 'archived')),
                    replay_count      INTEGER NOT NULL DEFAULT 0,
                    last_replayed_at  REAL,
                    archived_at       REAL,
                    created_at        REAL NOT NULL,
                    UNIQUE (device_id, sequence),
                    UNIQUE (device_id, batch_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO device_batches__new
                    (id, device_id, sequence, batch_id, previous_digest, digest,
                     observation_count, thresholds, observations, response, status,
                     replay_count, last_replayed_at, archived_at, created_at)
                SELECT id, device_id, sequence, batch_id, previous_digest, digest,
                       observation_count, thresholds, observations, response, status,
                       replay_count, last_replayed_at, NULL, created_at
                FROM device_batches
                """
            )
            conn.execute("DROP TABLE device_batches")
            conn.execute("ALTER TABLE device_batches__new RENAME TO device_batches")

        chain_info = {
            r[1]: r for r in conn.execute("PRAGMA table_info(device_chains)")
        }
        migrated_chain = False
        if chain_info:
            if "last_event_status" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_status TEXT NOT NULL DEFAULT 'accepted'"
                )
                migrated_chain = True
            if "last_event_sequence" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_sequence INTEGER NOT NULL DEFAULT 0"
                )
                migrated_chain = True
            if "last_event_at" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_at REAL NOT NULL DEFAULT 0.0"
                )
                migrated_chain = True
            if "archived_through" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "archived_through INTEGER NOT NULL DEFAULT 0"
                )
            if "archived_at" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN archived_at REAL"
                )
            if migrated_chain:
                # 旧链的最近事件即链头批次的首次受理：状态 accepted、序号为链头序号、
                # 时刻取该批次的 created_at
                conn.execute(
                    "UPDATE device_chains SET "
                    "last_event_status = 'accepted', "
                    "last_event_sequence = last_sequence, "
                    "last_event_at = COALESCE("
                    "  (SELECT b.created_at FROM device_batches b "
                    "   WHERE b.device_id = device_chains.device_id "
                    "   AND b.sequence = device_chains.last_sequence), 0.0)"
                )

    # ---------------- 特征库版本 ----------------

    @staticmethod
    def _aids_of(conn, version_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT aid_id, period, flashes FROM library_version_aids "
            "WHERE version_id = ? ORDER BY aid_id",
            (version_id,),
        ).fetchall()
        return [
            {"aid_id": r["aid_id"], "period": r["period"], "flashes": json.loads(r["flashes"])}
            for r in rows
        ]

    def _version_meta(self, conn, row) -> dict:
        vid = row["version_id"]
        base = row["base_version"]
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM library_version_aids WHERE version_id = ?", (vid,)
        ).fetchone()["c"]
        diff = None
        if base is not None:
            diff = _diff_aids(self._aids_of(conn, base), self._aids_of(conn, vid))
        return {
            "version_id": vid,
            "status": row["status"],
            "base_version": base,
            "source": row["source"],
            "aid_count": count,
            "created_at": _iso(row["created_at"]),
            "activated_at": _iso(row["activated_at"]) if row["activated_at"] is not None else None,
            "diff": diff,
        }

    def create_version(self, aids, *, base_version: int, source: str = "api") -> dict:
        """保存完整特征库为草稿版本；基线可以是当前激活版本，也可以是历史版本。"""
        rows = [(a.aid_id, a.period, json.dumps(a.flashes)) for a in aids]
        with self._write_tx() as conn:
            base = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?", (base_version,)
            ).fetchone()
            if base is None:
                raise VersionNotFound(f"base_version {base_version} not found")
            if base["status"] == "draft":
                raise VersionStateError(
                    f"base_version {base_version} is still a draft; "
                    "base must be an activated (current or historical) version"
                )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at) "
                "VALUES ('draft', ?, ?, ?)",
                (base_version, source, time.time()),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, aid_id, period, flashes) for aid_id, period, flashes in rows],
            )
        return self.get_version(vid)

    def activate_version(self, version_id: int) -> dict:
        """单事务激活：确认草稿的 base_version 仍是当前激活版本，然后原子切换。"""
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if row is None:
                raise VersionNotFound(f"version {version_id} not found")
            if row["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {row['status']}; only a draft can be activated"
                )
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            current = state["version_id"] if state is not None else None
            if row["base_version"] != current:
                raise VersionConflict(
                    f"base_version {row['base_version']} is no longer the active version "
                    f"(currently active: {current}); re-create the draft on top of the "
                    "current active version and retry"
                )
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            conn.execute(
                "UPDATE library_versions SET status = 'active', activated_at = ? "
                "WHERE version_id = ?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (version_id,),
            )
        return self.get_version(version_id)

    def publish_import(self, aids, *, replace: bool) -> tuple[int, dict]:
        """/library/import：基于当前激活版本合并/替换，单事务创建并直接激活新版本。"""
        with self._write_tx() as conn:
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            base_id = state["version_id"] if state is not None else None
            merged: dict[str, dict] = {}
            if not replace and base_id is not None:
                for a in self._aids_of(conn, base_id):
                    merged[a["aid_id"]] = a
            for a in aids:
                merged[a.aid_id] = {
                    "aid_id": a.aid_id,
                    "period": a.period,
                    "flashes": list(a.flashes),
                }
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', ?, 'import', ?, ?)",
                (base_id, now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [
                    (vid, a["aid_id"], a["period"], json.dumps(a["flashes"]))
                    for a in merged.values()
                ],
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (vid,),
            )
        return len(aids), self.get_version(vid)

    def list_versions(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM library_versions ORDER BY version_id"
            ).fetchall()
            return [self._version_meta(conn, r) for r in rows]

    def get_version(self, version_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            return self._version_meta(conn, row) if row is not None else None

    def get_version_aids(self, version_id: int) -> list[dict]:
        with closing(self._connect()) as conn:
            return self._aids_of(conn, version_id)

    def get_active_snapshot(self) -> tuple[int | None, list[dict]]:
        """/judge 请求开始时固定一次：同一读事务内取激活版本号与完整快照。"""
        with closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN")
                state = conn.execute(
                    "SELECT version_id FROM library_state WHERE id = 1"
                ).fetchone()
                vid = state["version_id"] if state is not None else None
                aids = self._aids_of(conn, vid) if vid is not None else []
            finally:
                conn.rollback()  # 只读事务，释放读锁
        return vid, aids

    @staticmethod
    def _active_snapshot_of(conn) -> tuple[int | None, list[dict]]:
        """在给定连接（可为写事务中的连接）内取当前激活版本号与完整快照。

        连续批次判定在写事务内调用：事务持写锁期间激活版本不可能被其他请求
        切换，故判定自请求开始固定的 ``library_version`` 与最终保存值严格
        同快照。
        """
        state = conn.execute(
            "SELECT version_id FROM library_state WHERE id = 1"
        ).fetchone()
        vid = state["version_id"] if state is not None else None
        return vid, Database._aids_of(conn, vid) if vid is not None else []

    # ---------------- 判定记录 ----------------

    def save_judgment(
        self,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        with self._write_tx() as conn:
            return self._insert_judgment(
                conn,
                status=status,
                best_aid_id=best_aid_id,
                matched=matched,
                missed=missed,
                false_count=false_count,
                drift=drift,
                library_version=library_version,
                request=request,
                response=response,
            )

    @staticmethod
    def _insert_judgment(
        conn,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        """在给定连接（可为其他写事务中的连接）内插入一条判定记录并返回其 id。"""
        cur = conn.execute(
            "INSERT INTO judgments "
            "(created_at, status, best_aid_id, matched, missed, false_count, drift, "
            "library_version, request, response) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                time.time(),
                status,
                best_aid_id,
                matched,
                missed,
                false_count,
                drift,
                library_version,
                json.dumps(request, ensure_ascii=False),
                json.dumps(response, ensure_ascii=False),
            ),
        )
        return int(cur.lastrowid)

    def list_judgments(self, limit: int = 50, offset: int = 0) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version "
                "FROM judgments ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "created_at": _iso(r["created_at"]),
                "status": r["status"],
                "best_aid_id": r["best_aid_id"],
                "matched": r["matched"],
                "missed": r["missed"],
                "false": r["false_count"],
                "drift": r["drift"],
                "library_version": r["library_version"],
            }
            for r in rows
        ]

    def get_judgment(self, judgment_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version, request, response "
                "FROM judgments WHERE id = ?",
                (judgment_id,),
            ).fetchone()
        if r is None:
            return None
        return {
            "id": r["id"],
            "created_at": _iso(r["created_at"]),
            "status": r["status"],
            "best_aid_id": r["best_aid_id"],
            "matched": r["matched"],
            "missed": r["missed"],
            "false": r["false_count"],
            "drift": r["drift"],
            "library_version": r["library_version"],
            "request": json.loads(r["request"]),
            "response": json.loads(r["response"]),
        }

    # ---------------- 现场设备批次链 ----------------

    def submit_batch(self, device_id: str, req) -> tuple[dict, bool]:
        """单事务提交批次：幂等/链头校验 → 追加记录/累加重放 → 更新设备游标。

        返回 ``(响应体, 是否新接受)``。相同载荷重放（摘要一致）在同一幂等事务
        内原子累加该批次 ``replay_count``、刷新其 ``last_replayed_at`` 并把设备
        游标的最近事件改为本次重放，但**不**新增批次、**不**推进链头、**不**
        改写首次受理响应；响应仍返回原结果并标记 ``replayed``。载荷变化或链头
        过期抛 :class:`BatchConflict`。写锁 + BEGIN IMMEDIATE 串行化并发提交，
        唯一约束兜底，不会形成分叉，并发重放也不会丢失计数。
        """
        digest = compute_digest(canonical_payload(device_id, req))
        prev = req.previous_digest or ""
        thresholds = {
            "tolerance": req.tolerance,
            "max_drift": req.max_drift,
            "max_missed": req.max_missed,
            "max_false": req.max_false,
        }
        observations = [
            {"local_id": o.local_id, "captured_at": o.captured_at}
            for o in req.observations
        ]
        now = time.time()
        with self._write_tx() as conn:
            row_bid = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                (device_id, req.batch_id),
            ).fetchone()
            row_seq = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, req.sequence),
            ).fetchone()
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
            last_seq = chain["last_sequence"] if chain is not None else 0
            head = chain["head_digest"] if chain is not None else ""

            if row_bid is not None or row_seq is not None:
                # 同一 batch_id 且同一 sequence 且摘要一致 → 相同载荷重放
                if (
                    row_bid is not None
                    and row_seq is not None
                    and row_bid["id"] == row_seq["id"]
                    and row_bid["digest"] == digest
                ):
                    # 幂等重放：原子累加计数并刷新批次与链游标的最近事件；
                    # 不新增记录、不推进链头、不改写首次受理响应（response 列）
                    conn.execute(
                        "UPDATE device_batches SET "
                        "replay_count = replay_count + 1, last_replayed_at = ? "
                        "WHERE id = ?",
                        (now, row_bid["id"]),
                    )
                    conn.execute(
                        "UPDATE device_chains SET "
                        "last_event_status = 'replayed', "
                        "last_event_sequence = ?, last_event_at = ? "
                        "WHERE device_id = ?",
                        (row_bid["sequence"], now, device_id),
                    )
                    body = json.loads(row_bid["response"])
                    body["status"] = "replayed"
                    return body, False
                existing = row_bid if row_bid is not None else row_seq
                raise BatchConflict(
                    reason="payload_changed",
                    message=(
                        f"batch_id/sequence already recorded with a different "
                        f"payload (existing sequence {existing['sequence']}, "
                        f"batch_id {existing['batch_id']!r})"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                    existing={
                        "sequence": existing["sequence"],
                        "batch_id": existing["batch_id"],
                        "digest": existing["digest"],
                    },
                )

            if req.sequence != last_seq + 1 or prev != head:
                raise BatchConflict(
                    reason="stale_chain_head",
                    message=(
                        f"batch does not extend the chain head: got sequence "
                        f"{req.sequence} with previous_digest {prev!r}, expected "
                        f"sequence {last_seq + 1} with previous_digest {head!r}"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                )

            body = {
                "status": "accepted",
                "device_id": device_id,
                "batch_id": req.batch_id,
                "sequence": req.sequence,
                "digest": digest,
                "previous_digest": prev,
                "observation_count": len(observations),
                "received_at": _iso(now),
                "chain": {
                    "head_sequence": req.sequence,
                    "head_digest": digest,
                    "next_sequence": req.sequence + 1,
                    "expected_previous_digest": digest,
                },
            }
            conn.execute(
                "INSERT INTO device_batches "
                "(device_id, sequence, batch_id, previous_digest, digest, "
                "observation_count, thresholds, observations, response, status, "
                "created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?)",
                (
                    device_id,
                    req.sequence,
                    req.batch_id,
                    prev,
                    digest,
                    len(observations),
                    json.dumps(thresholds, ensure_ascii=False),
                    json.dumps(observations, ensure_ascii=False),
                    json.dumps(body, ensure_ascii=False),
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO device_chains "
                "(device_id, last_sequence, head_digest, batch_count, updated_at, "
                "last_event_status, last_event_sequence, last_event_at) "
                "VALUES (?, ?, ?, 1, ?, 'accepted', ?, ?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "last_sequence = excluded.last_sequence, "
                "head_digest = excluded.head_digest, "
                "batch_count = device_chains.batch_count + 1, "
                "updated_at = excluded.updated_at, "
                "last_event_status = 'accepted', "
                "last_event_sequence = excluded.last_event_sequence, "
                "last_event_at = excluded.last_event_at",
                (device_id, req.sequence, digest, now, req.sequence, now),
            )
        return body, True

    def get_device_cursor(self, device_id: str) -> dict:
        """设备批次链游标：链头位置、下一批期望值与最近一次事件（受理/重放）。

        ``last_event_status`` 为最近事件的 ``accepted``/``replayed``，
        ``last_event_sequence`` 为该事件所属批次序号，``last_event_at`` 为其
        时刻；未上传过的设备三者分别为 ``None``/``0``/``None``。
        """
        with closing(self._connect()) as conn:
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
        if chain is None:
            return {
                "device_id": device_id,
                "last_sequence": 0,
                "head_digest": "",
                "batch_count": 0,
                "next_sequence": 1,
                "expected_previous_digest": "",
                "updated_at": None,
                "last_event_status": None,
                "last_event_sequence": 0,
                "last_event_at": None,
                "archived_through": 0,
                "archived_at": None,
            }
        return {
            "device_id": device_id,
            "last_sequence": chain["last_sequence"],
            "head_digest": chain["head_digest"],
            "batch_count": chain["batch_count"],
            "next_sequence": chain["last_sequence"] + 1,
            "expected_previous_digest": chain["head_digest"],
            "updated_at": _iso(chain["updated_at"]),
            "last_event_status": chain["last_event_status"],
            "last_event_sequence": chain["last_event_sequence"],
            "last_event_at": _iso(chain["last_event_at"]),
            "archived_through": chain["archived_through"],
            "archived_at": _iso(chain["archived_at"]),
        }

    def list_device_batches(
        self, device_id: str, *, after_sequence: int = 0, limit: int = 50
    ) -> dict:
        """按 sequence 游标分页列出设备批次（升序）。

        每条返回首次受理事实 ``status='accepted'``（归档墓碑为
        ``'archived'``，不带明细）及与详情一致的
        ``replay_count`` / ``last_replayed_at``；页顶另返回设备当前
        ``archived_through``。
        """
        with closing(self._connect()) as conn:
            chain = conn.execute(
                "SELECT archived_through, archived_at FROM device_chains "
                "WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            rows = conn.execute(
                "SELECT sequence, batch_id, previous_digest, digest, "
                "observation_count, status, replay_count, last_replayed_at, "
                "archived_at, created_at "
                "FROM device_batches "
                "WHERE device_id = ? AND sequence > ? ORDER BY sequence LIMIT ?",
                (device_id, after_sequence, limit + 1),
            ).fetchall()
        archived_through = chain["archived_through"] if chain is not None else 0
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for r in rows:
            item = {
                "sequence": r["sequence"],
                "batch_id": r["batch_id"],
                "previous_digest": r["previous_digest"],
                "digest": r["digest"],
                "observation_count": r["observation_count"],
                "status": r["status"],
                "archived": r["archived_at"] is not None,
                "replay_count": r["replay_count"],
                "last_replayed_at": _iso(r["last_replayed_at"]),
                "archived_at": _iso(r["archived_at"]),
                "created_at": _iso(r["created_at"]),
            }
            items.append(item)
        return {
            "device_id": device_id,
            "count": len(items),
            "archived_through": archived_through,
            "batches": items,
            "next_cursor": items[-1]["sequence"] if has_more and items else None,
        }

    def get_device_batch(self, device_id: str, sequence: int) -> dict | None:
        """单条批次详情：摘要、条数、首次受理状态与重放信息。

        未归档批次另含观测时间戳与 /judge 四项阈值；已归档批次为墓碑
        （``status='archived'``、``archived=true``），明细已清空，
        ``observations`` / ``thresholds`` 为 ``None``。
        """
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
        if r is None:
            return None
        archived = r["archived_at"] is not None
        return {
            "status": r["status"],
            "archived": archived,
            "replay_count": r["replay_count"],
            "last_replayed_at": _iso(r["last_replayed_at"]),
            "archived_at": _iso(r["archived_at"]),
            "device_id": r["device_id"],
            "sequence": r["sequence"],
            "batch_id": r["batch_id"],
            "digest": r["digest"],
            "previous_digest": r["previous_digest"],
            "observation_count": r["observation_count"],
            # 墓碑明细载荷已清空：观测与阈值不再返回
            "observations": None if archived else json.loads(r["observations"]),
            "thresholds": None if archived else json.loads(r["thresholds"]),
            "created_at": _iso(r["created_at"]),
        }

    # ---------------- 必需消费者登记 / 批次确认 / 归档水位 ----------------

    def register_consumer(self, device_id: str, consumer_id: str) -> tuple[dict, bool]:
        """登记设备的必需下游消费者；登记后不可删除。

        同一 ``(device_id, consumer_id)`` 重复登记幂等：返回既有登记信息并以
        ``created=False`` 标记。新登记 ``created=True``。设备本身无需预先存在
        （消费者可在任何批次上传之前登记）。在设备已有归档前缀之后登记的消费
        者，其水位从 ``archived_through`` 起步——这些批次的明细已被清除，
        无需也无法再逐个确认，其首个缺口直接指向归档前缀的下一批。
        """
        now = time.time()
        with self._write_tx() as conn:
            existing = conn.execute(
                "SELECT consumer_id, watermark, registered_at FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            chain = conn.execute(
                "SELECT archived_through, archived_at FROM device_chains "
                "WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            archived_through = chain["archived_through"] if chain is not None else 0
            if existing is not None:
                return {
                    "status": "already_registered",
                    "device_id": device_id,
                    "consumer_id": consumer_id,
                    "watermark": existing["watermark"],
                    "archived_through": archived_through,
                    "registered_at": _iso(existing["registered_at"]),
                }, False
            conn.execute(
                "INSERT INTO device_consumers (device_id, consumer_id, watermark, registered_at) "
                "VALUES (?, ?, ?, ?)",
                (device_id, consumer_id, archived_through, now),
            )
            return {
                "status": "registered",
                "device_id": device_id,
                "consumer_id": consumer_id,
                "watermark": archived_through,
                "archived_through": archived_through,
                "registered_at": _iso(now),
            }, True

    @staticmethod
    def _consumer_statuses(conn, device_id: str, head_sequence: int = -1) -> list[dict]:
        """组装各消费者水位与首个缺口（水位之后的第一个未确认序号）。

        水位定义为「从 1 起连续确认的最大序号」，因此首个缺口恒为
        ``watermark + 1``；当已追平链头（``watermark == head_sequence``）时
        链上再无缺口，返回 ``None``。
        """
        consumers = conn.execute(
            "SELECT id, consumer_id, watermark, registered_at FROM device_consumers "
            "WHERE device_id = ? ORDER BY id",
            (device_id,),
        ).fetchall()
        statuses: list[dict] = []
        for c in consumers:
            watermark = c["watermark"]
            first_gap = None if 0 <= head_sequence <= watermark else watermark + 1
            statuses.append(
                {
                    "consumer_id": c["consumer_id"],
                    "watermark": watermark,
                    "first_gap": first_gap,
                    "registered_at": _iso(c["registered_at"]),
                }
            )
        return statuses

    def acknowledge_batch(
        self, device_id: str, consumer_id: str, sequence: int, batch_digest: str
    ) -> tuple[dict, bool]:
        """单事务保存消费者的稀疏确认并把其连续水位推进至首个缺口。

        允许乱序、并发及重复提交。仅接受已存在且摘要相符的批次：批次不存在
        返回 404；消费者未登记或摘要冲突返回 409（:class:`ConsumerConflict`）。
        相同确认（同一消费者/序号/摘要）幂等，HTTP 200 且 ``status=duplicate``；
        新确认 HTTP 201 且 ``status=confirmed``。

        水位在写事务内按最新稀疏确认整体重算（从当前水位的下一个序号起逐个
        找首个缺口），并发确认由写锁串行化，不会丢失推进。
        """
        now = time.time()
        with self._write_tx() as conn:
            consumer = conn.execute(
                "SELECT id, watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            if consumer is None:
                raise ConsumerConflict(
                    reason="consumer_not_registered",
                    message=(
                        f"consumer {consumer_id!r} is not registered as a required "
                        f"consumer of device {device_id!r}; register it before "
                        "acknowledging batches"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                )
            batch = conn.execute(
                "SELECT digest FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
            if batch is None:
                # 404 在路由层按 BatchNotFound 处理
                raise BatchNotFound(device_id, sequence)
            if batch["digest"] != batch_digest:
                raise ConsumerConflict(
                    reason="digest_mismatch",
                    message=(
                        f"batch_digest does not match batch {sequence} of device "
                        f"{device_id!r}: acknowledgment refused"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                    sequence=sequence,
                    provided_digest=batch_digest,
                    expected_digest=batch["digest"],
                )

            existing = conn.execute(
                "SELECT batch_digest, confirmed_at FROM device_batch_acks "
                "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                (device_id, consumer_id, sequence),
            ).fetchone()
            created = False
            if existing is None:
                # 唯一约束兜底并发：同一 (消费者, 序号) 只可能有一条确认，
                # 摘要不符的并发写入会在此处转为 digest_mismatch 409
                try:
                    conn.execute(
                        "INSERT INTO device_batch_acks "
                        "(device_id, consumer_id, sequence, batch_digest, confirmed_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (device_id, consumer_id, sequence, batch_digest, now),
                    )
                except sqlite3.IntegrityError:
                    existing = conn.execute(
                        "SELECT batch_digest, confirmed_at FROM device_batch_acks "
                        "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                        (device_id, consumer_id, sequence),
                    ).fetchone()
                    if existing["batch_digest"] != batch_digest:
                        raise ConsumerConflict(
                            reason="digest_mismatch",
                            message=(
                                f"batch {sequence} already acknowledged with a different "
                                "digest by this consumer"
                            ),
                            device_id=device_id,
                            consumer_id=consumer_id,
                            sequence=sequence,
                            provided_digest=batch_digest,
                            expected_digest=existing["batch_digest"],
                        )
                else:
                    created = True

            # 在同一事务内基于最新稀疏确认把水位推进至首个缺口：
            # 从当前水位 + 1 起，连续存在的确认序号逐个越过，遇到缺口即停。
            watermark = consumer["watermark"]
            while True:
                nxt = conn.execute(
                    "SELECT 1 FROM device_batch_acks "
                    "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                    (device_id, consumer_id, watermark + 1),
                ).fetchone()
                if nxt is None:
                    break
                watermark += 1
            if watermark != consumer["watermark"]:
                conn.execute(
                    "UPDATE device_consumers SET watermark = ? WHERE id = ?",
                    (watermark, consumer["id"]),
                )

            # 在同一事务内基于最新稀疏确认把水位推进至首个缺口：
            # 从当前水位 + 1 起，连续存在的确认序号逐个越过，遇到缺口即停。
            # 归档之后登记的消费者水位从 archived_through 起步，其首个缺口
            # 即归档前缀的下一批；归档区间内无需也无法再逐个确认。
            watermark = consumer["watermark"]
            while True:
                nxt = conn.execute(
                    "SELECT 1 FROM device_batch_acks "
                    "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                    (device_id, consumer_id, watermark + 1),
                ).fetchone()
                if nxt is None:
                    break
                watermark += 1
            if watermark != consumer["watermark"]:
                conn.execute(
                    "UPDATE device_consumers SET watermark = ? WHERE id = ?",
                    (watermark, consumer["id"]),
                )

            confirmed_at = _iso(now if existing is None else existing["confirmed_at"])
            body = {
                "status": "confirmed" if created else "duplicate",
                "device_id": device_id,
                "consumer_id": consumer_id,
                "sequence": sequence,
                "batch_digest": batch_digest,
                "watermark": watermark,
                "confirmed_at": confirmed_at,
            }
            # safe_sequence / archived_through 在同一写事务内读出，
            # 与本次确认的水位推进严格同快照
            body.update(self._archive_status_fields(conn, device_id))
            return body, created

    @staticmethod
    def _archive_status_fields(conn, device_id: str) -> dict:
        """在给定连接（可为写事务中的连接）内核验并组装归档水位字段。

        ``safe_sequence`` 取所有必需消费者水位的最小值；尚无消费者或任一消费
        者水位仍为 0 时为 0。``archived_through`` 取链游标上的已归档前缀右端
        （单调不退，初始 0）。``blocking_consumers`` 列出水位等于该最小值且
        尚未追平链头的消费者（空列表表示链上批次已全部可安全归档）。
        """
        chain = conn.execute(
            "SELECT last_sequence, archived_through FROM device_chains "
            "WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        head_sequence = chain["last_sequence"] if chain is not None else 0
        archived_through = chain["archived_through"] if chain is not None else 0
        statuses = Database._consumer_statuses(conn, device_id, head_sequence)
        if not statuses:
            safe_sequence = 0
            blocking: list[str] = []
        else:
            safe_sequence = min(s["watermark"] for s in statuses)
            blocking = (
                sorted(
                    s["consumer_id"]
                    for s in statuses
                    if s["watermark"] == safe_sequence
                )
                if safe_sequence < head_sequence
                else []
            )
        return {
            "head_sequence": head_sequence,
            "consumer_count": len(statuses),
            "safe_sequence": safe_sequence,
            "first_gap": safe_sequence + 1,
            "blocking_consumers": blocking,
            "consumers": statuses,
            "archived_through": archived_through,
        }

    def get_archive_status(self, device_id: str) -> dict:
        """设备的多消费者归档状态：各水位、首个缺口、阻塞消费者与归档前缀。"""
        with closing(self._connect()) as conn:
            status = self._archive_status_fields(conn, device_id)
            archived_at = conn.execute(
                "SELECT archived_at FROM device_chains WHERE device_id = ?",
                (device_id,),
            ).fetchone()
        status["device_id"] = device_id
        status["archived_at"] = (
            _iso(archived_at["archived_at"]) if archived_at is not None else None
        )
        # 顶层字段顺序稳定：device_id 在最前
        return {
            "device_id": device_id,
            "head_sequence": status["head_sequence"],
            "consumer_count": status["consumer_count"],
            "safe_sequence": status["safe_sequence"],
            "first_gap": status["first_gap"],
            "blocking_consumers": status["blocking_consumers"],
            "consumers": status["consumers"],
            "archived_through": status["archived_through"],
            "archived_at": status["archived_at"],
        }

    def archive_device_prefix(self, device_id: str, through_sequence: int) -> tuple[dict, bool]:
        """归档连续前缀 ``1..through_sequence`` 的明细载荷（单事务）。

        目标不得越过**同一写事务内**核定的 ``safe_sequence``（全部必需消费者
        都已确认的连续前缀）；越界抛 :class:`ArchiveConflict`（HTTP 409），
        detail 携带当前 ``safe_sequence`` / ``archived_through`` /
        ``head_sequence`` / ``first_gap`` 与阻塞消费者。归档把目标范围内批次
        的 ``observations`` / ``thresholds`` 清空并置墓碑
        （``status='archived'``、``archived_at``），只保留 ``batch_id``、
        序号、前序/本批摘要、条数、首次受理时刻/响应与重放统计；链头摘要与
        后续上传不变。

        目标不超过已归档前缀时**幂等**（HTTP 200，``status=already_archived``，
        不刷新归档时刻、不改动重放统计）；新扩大前缀 HTTP 201
        （``status=archived``）。写锁 + BEGIN IMMEDIATE 与确认/登记/上传共用
        同一串行点：并发确认、消费者登记与归档互不越过未确认批次，
        ``archived_through`` 只进不退。
        """
        now = time.time()
        with self._write_tx() as conn:
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
            head_sequence = chain["last_sequence"] if chain is not None else 0
            archived_through = chain["archived_through"] if chain is not None else 0
            fields = self._archive_status_fields(conn, device_id)
            safe_sequence = fields["safe_sequence"]

            if through_sequence <= archived_through:
                # 重复或更小目标：幂等返回当前归档状态，不刷新 archived_at
                return self._archive_response(
                    device_id,
                    status="already_archived",
                    requested=through_sequence,
                    archived_through=archived_through,
                    archived_at=chain["archived_at"] if chain is not None else None,
                    fields=fields,
                ), False

            if through_sequence > safe_sequence:
                raise ArchiveConflict(
                    message=(
                        f"cannot archive through sequence {through_sequence}: it exceeds "
                        f"safe_sequence {safe_sequence} (all required consumers must "
                        f"acknowledge the contiguous prefix first)"
                    ),
                    reason="beyond_safe_sequence",
                    requested_through=through_sequence,
                    safe_sequence=safe_sequence,
                    archived_through=archived_through,
                    head_sequence=head_sequence,
                    first_gap=fields["first_gap"],
                    blocking_consumers=fields["blocking_consumers"],
                )

            # 通过 <= safe_sequence 保证目标范围内每个批次都存在
            # （1..safe_sequence 必为已上传的连续前缀）。
            cur = conn.execute(
                "UPDATE device_batches SET "
                "observations = NULL, thresholds = NULL, "
                "status = 'archived', archived_at = ? "
                "WHERE device_id = ? AND sequence > ? AND sequence <= ?",
                (now, device_id, archived_through, through_sequence),
            )
            archived_count = cur.rowcount
            if chain is None:
                # 极端情况下无链行（安全水位必为 0，目标 >=1 不可能走到这里；
                # 兜底插入，保持 archived_through 与实际墓碑一致）
                conn.execute(
                    "INSERT INTO device_chains "
                    "(device_id, last_sequence, head_digest, batch_count, updated_at, "
                    "archived_through, archived_at) "
                    "VALUES (?, 0, '', 0, ?, ?, ?)",
                    (device_id, now, through_sequence, now),
                )
            else:
                conn.execute(
                    "UPDATE device_chains SET archived_through = ?, archived_at = ? "
                    "WHERE device_id = ?",
                    (through_sequence, now, device_id),
                )
            fields = self._archive_status_fields(conn, device_id)
            body = self._archive_response(
                device_id,
                status="archived",
                requested=through_sequence,
                archived_through=through_sequence,
                archived_at=now,
                fields=fields,
                archived_count=archived_count,
            )
            return body, True

    @staticmethod
    def _archive_response(
        device_id: str,
        *,
        status: str,
        requested: int,
        archived_through: int,
        archived_at: float | None,
        fields: dict,
        archived_count: int = 0,
    ) -> dict:
        return {
            "status": status,
            "device_id": device_id,
            "requested_through": requested,
            "archived_through": archived_through,
            "archived_at": _iso(archived_at),
            "newly_archived_count": archived_count,
            "head_sequence": fields["head_sequence"],
            "safe_sequence": fields["safe_sequence"],
            "first_gap": fields["safe_sequence"] + 1,
            "blocking_consumers": fields["blocking_consumers"],
            "consumers": fields["consumers"],
        }

    # ---------------- 草稿回放报告 ----------------

    @staticmethod
    def _replay_summary(report_row) -> dict:
        """组装回放报告摘要（不含逐条明细，明细走游标分页接口）。"""
        return {
            "report_id": report_row["report_id"],
            "version_id": report_row["version_id"],
            "idempotency_key": report_row["idempotency_key"],
            "watermark_judgment_id": report_row["watermark"],
            "created_at": _iso(report_row["created_at"]),
            "total_judgments": report_row["total_count"],
            "replayed_count": report_row["replayed_count"],
            "skipped_count": report_row["skipped_count"],
            "changes": {
                "unchanged": report_row["unchanged"],
                "match_gained": report_row["match_gained"],
                "match_lost": report_row["match_lost"],
                "winner_changed": report_row["winner_changed"],
            },
        }

    def create_replay_report(
        self, version_id: int, idempotency_key: str, runner
    ) -> tuple[dict, bool]:
        """为草稿创建回放报告；整个流程（冻结水位 → 重放 → 落库）在单个
        BEGIN IMMEDIATE 事务内完成。

        - 目标版本必须存在且为 ``draft``（不存在抛 :class:`VersionNotFound`，
          非草稿抛 :class:`VersionStateError`）；
        - ``idempotency_key`` 全局唯一：同版本同键重试返回原报告
          （``created=False``，HTTP 200）；键已被**其他版本**占用抛
          :class:`ReplayConflict`（HTTP 409）。写锁串行化 + 唯一约束兜底，
          并发请求不会生成重复报告；
        - 开始时冻结当前最大 ``judgment_id`` 作为水位，只重放水位内的记录：
          请求携带内联 ``library`` 的记录按原因跳过，载荷缺字段的历史记录
          同样跳过；其余沿用原观测与四项阈值，由调用方注入的 ``runner``
          （草稿快照, 待回放记录）执行与 /judge 相同的判定算法。

        回放结果只写入 ``replay_reports`` / ``replay_report_items``，
        不写入 ``judgments``。
        """
        from .replay import (
            SKIP_INLINE_LIBRARY,
            SKIP_MISSING_INPUT,
            classify_change,
            has_replay_input,
            is_inline_judgment,
        )

        now = time.time()
        with self._write_tx() as conn:
            version = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?",
                (version_id,),
            ).fetchone()
            if version is None:
                raise VersionNotFound(f"version {version_id} not found")
            if version["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {version['status']}; "
                    "replay reports can only be created for a draft"
                )

            existing = conn.execute(
                "SELECT * FROM replay_reports WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["version_id"] == version_id:
                    return self._replay_summary(existing), False
                raise ReplayConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"replay report of version {existing['version_id']}"
                    ),
                    idempotency_key=idempotency_key,
                    occupied_by_version=existing["version_id"],
                    occupied_by_report=existing["report_id"],
                )

            # 冻结水位：事务已持写锁，后续 /judge 的写入在本事务提交后才能进行，
            # 新判定的 id 必然大于该水位，不会混入本次报告。
            watermark_row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS m FROM judgments"
            ).fetchone()
            watermark = int(watermark_row["m"])

            judgment_rows = conn.execute(
                "SELECT id, status, best_aid_id, library_version, request, response "
                "FROM judgments WHERE id <= ? ORDER BY id",
                (watermark,),
            ).fetchall()

            to_replay: list[dict] = []
            rows_by_id: dict[int, sqlite3.Row] = {}
            skipped: list[tuple[int, str, sqlite3.Row]] = []
            for r in judgment_rows:
                request = json.loads(r["request"])
                if is_inline_judgment(request):
                    skipped.append((r["id"], SKIP_INLINE_LIBRARY, r))
                elif not has_replay_input(request):
                    skipped.append((r["id"], SKIP_MISSING_INPUT, r))
                else:
                    rows_by_id[r["id"]] = r
                    to_replay.append({"judgment_id": r["id"], "request": request})

            outcomes = runner(self._aids_of(conn, version_id), to_replay)
            outcomes_by_id = {o["judgment_id"]: o for o in outcomes}

            try:
                cur = conn.execute(
                    "INSERT INTO replay_reports "
                    "(version_id, idempotency_key, watermark, total_count, replayed_count, "
                    "skipped_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        idempotency_key,
                        watermark,
                        len(judgment_rows),
                        len(to_replay),
                        len(skipped),
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 并发兜底：写锁通常已把同键请求串行化到上面的 SELECT；
                # 若仍有并发事务抢先占用该键，按幂等/跨版本冲突处理
                rival = conn.execute(
                    "SELECT * FROM replay_reports WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if rival is not None and rival["version_id"] == version_id:
                    return self._replay_summary(rival), False
                raise ReplayConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"replay report of version {rival['version_id'] if rival else '?'}"
                    ),
                    idempotency_key=idempotency_key,
                    occupied_by_version=int(rival["version_id"]) if rival else version_id,
                    occupied_by_report=int(rival["report_id"]) if rival else 0,
                )
            report_id = int(cur.lastrowid)

            for judgment_id, skip_reason, r in skipped:
                conn.execute(
                    "INSERT INTO replay_report_items "
                    "(report_id, judgment_id, item_status, skip_reason, "
                    "old_status, old_best_aid_id, old_reason, old_library_version) "
                    "VALUES (?, ?, 'skipped', ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        judgment_id,
                        skip_reason,
                        r["status"],
                        r["best_aid_id"],
                        None,
                        r["library_version"],
                    ),
                )

            class_counts = {
                "unchanged": 0,
                "match_gained": 0,
                "match_lost": 0,
                "winner_changed": 0,
            }
            for rec in to_replay:
                r = rows_by_id[rec["judgment_id"]]
                old_response = json.loads(r["response"])
                old_best = old_response.get("best")
                old_reason = old_response.get("reason")
                # 裁决状态/最佳编号以 judgments 权威列为准，证据以保存的首次响应为准
                old_status = r["status"]
                outcome = outcomes_by_id[rec["judgment_id"]]
                new_best = outcome["best"]
                new_status = outcome["status"]
                classification = classify_change(
                    old_status, old_best, new_status, new_best
                )
                class_counts[classification] += 1
                conn.execute(
                    "INSERT INTO replay_report_items "
                    "(report_id, judgment_id, item_status, classification, "
                    "old_status, old_best_aid_id, old_reason, old_library_version, "
                    "old_best, new_status, new_best_aid_id, new_reason, new_best) "
                    "VALUES (?, ?, 'replayed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        rec["judgment_id"],
                        classification,
                        old_status,
                        r["best_aid_id"],
                        old_reason,
                        r["library_version"],
                        json.dumps(old_best, ensure_ascii=False),
                        new_status,
                        new_best["aid_id"] if new_best is not None else None,
                        outcome["reason"],
                        json.dumps(new_best, ensure_ascii=False),
                    ),
                )

            conn.execute(
                "UPDATE replay_reports SET unchanged = ?, match_gained = ?, "
                "match_lost = ?, winner_changed = ? WHERE report_id = ?",
                (
                    class_counts["unchanged"],
                    class_counts["match_gained"],
                    class_counts["match_lost"],
                    class_counts["winner_changed"],
                    report_id,
                ),
            )
            report = conn.execute(
                "SELECT * FROM replay_reports WHERE report_id = ?", (report_id,)
            ).fetchone()
            summary = self._replay_summary(report)
        return summary, True

    def get_replay_report(self, version_id: int, report_id: int) -> tuple[int, dict] | None:
        """读取指定版本下的回放报告摘要；不属于该版本或不存在返回 None。

        返回 ``(version_id, summary)``。
        """
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT * FROM replay_reports WHERE report_id = ? AND version_id = ?",
                (report_id, version_id),
            ).fetchone()
        if r is None:
            return None
        return int(r["version_id"]), self._replay_summary(r)

    def list_replay_changes(
        self,
        version_id: int,
        report_id: int,
        *,
        after_judgment_id: int = 0,
        limit: int = 50,
        classification: str | None = None,
        include_skipped: bool = False,
    ) -> dict | None:
        """按原 judgment_id 游标分页返回回放明细（升序）。

        默认只返回已重放记录（可按四分类之一过滤）；``include_skipped=true``
        时改返回被跳过记录（此时不允许再按 classification 过滤）。报告不
        属于该版本或不存在时返回 None。
        """
        with closing(self._connect()) as conn:
            report = conn.execute(
                "SELECT 1 FROM replay_reports WHERE report_id = ? AND version_id = ?",
                (report_id, version_id),
            ).fetchone()
            if report is None:
                return None
            if include_skipped:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'skipped' "
                    "AND judgment_id > ? ORDER BY judgment_id LIMIT ?",
                    (report_id, after_judgment_id, limit + 1),
                ).fetchall()
            elif classification is not None:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'replayed' "
                    "AND classification = ? AND judgment_id > ? "
                    "ORDER BY judgment_id LIMIT ?",
                    (report_id, classification, after_judgment_id, limit + 1),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'replayed' "
                    "AND judgment_id > ? ORDER BY judgment_id LIMIT ?",
                    (report_id, after_judgment_id, limit + 1),
                ).fetchall()

        has_more = len(rows) > limit
        rows = rows[:limit]
        items: list[dict] = []
        for r in rows:
            if r["item_status"] == "skipped":
                items.append(
                    {
                        "judgment_id": r["judgment_id"],
                        "item_status": "skipped",
                        "skip_reason": r["skip_reason"],
                        "old_status": r["old_status"],
                        "old_best_aid_id": r["old_best_aid_id"],
                        "old_library_version": r["old_library_version"],
                    }
                )
            else:
                items.append(
                    {
                        "judgment_id": r["judgment_id"],
                        "item_status": "replayed",
                        "classification": r["classification"],
                        "old": {
                            "status": r["old_status"],
                            "best_aid_id": r["old_best_aid_id"],
                            "reason": r["old_reason"],
                            "library_version": r["old_library_version"],
                            "best": json.loads(r["old_best"]),
                        },
                        "new": {
                            "status": r["new_status"],
                            "best_aid_id": r["new_best_aid_id"],
                            "reason": r["new_reason"],
                            "best": json.loads(r["new_best"]),
                        },
                    }
                )
        return {
            "report_id": report_id,
            "version_id": version_id,
            "classification": classification,
            "include_skipped": include_skipped,
            "count": len(items),
            "changes": items,
            "next_cursor": items[-1]["judgment_id"] if has_more and items else None,
        }

    # ---------------- 连续批次归档前巡检判定 ----------------

    def create_batch_judgment(
        self,
        device_id: str,
        idempotency_key: str,
        from_sequence: int,
        to_sequence: int,
        runner,
    ) -> tuple[dict, bool]:
        """对设备连续批次区间创建一次归档前巡检判定（单事务，幂等）。

        整个流程在单个 BEGIN IMMEDIATE 事务内完成，写锁与上传/归档/激活共用
        同一串行点：

        1. 幂等键检查：相同键 + 相同设备/范围重试返回原判定
           （``created=False``，HTTP 200）；相同键但设备或范围不同抛
           :class:`BatchJudgmentConflict`（HTTP 409）；
        2. 区间校验：``from_sequence..to_sequence`` 内每个序号都必须存在、
           且明细仍在（非墓碑）。范围含墓碑抛 ``range_archived``，含缺批次
           抛 ``range_not_continuous``——两种情况整体拒绝，不落任何记录；
        3. 各批保存的 /judge 四项阈值必须完全一致，否则抛
           ``thresholds_mismatch``（同样整体拒绝）；
        4. 在同一事务内固定当前激活特征库版本与快照（持锁期间版本不可能被
           切换），按 ``(captured_at, sequence, local_id)`` 合并观测，由
           调用方注入的 ``runner`` 复用与 /judge 完全相同的判定算法；
        5. 判定结果写入 ``judgments``（可被 /judgments 与草稿回放复核），
           再保存 ``batch_judgments``（幂等键、设备、序号范围、
           ``library_version``、合并观测与阈值、judgment_id）与各批摘要
           （``batch_judgment_batches``）作为不可变来源。
        """
        now = time.time()
        with self._write_tx() as conn:
            existing = conn.execute(
                "SELECT id FROM batch_judgments WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                saved = self._get_batch_judgment(conn, int(existing["id"]))
                if (
                    saved is not None
                    and saved["device_id"] == device_id
                    and saved["from_sequence"] == from_sequence
                    and saved["to_sequence"] == to_sequence
                ):
                    return saved, False
                raise BatchJudgmentConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"batch judgment of device {saved['device_id']!r} range "
                        f"{saved['from_sequence']}..{saved['to_sequence']}"
                    ),
                    reason="idempotency_key_in_use",
                    idempotency_key=idempotency_key,
                    occupied_by_batch_judgment_id=saved["batch_judgment_id"],
                    occupied_device_id=saved["device_id"],
                    occupied_from_sequence=saved["from_sequence"],
                    occupied_to_sequence=saved["to_sequence"],
                )

            rows = conn.execute(
                "SELECT sequence, batch_id, previous_digest, digest, "
                "observation_count, thresholds, observations, archived_at "
                "FROM device_batches "
                "WHERE device_id = ? AND sequence >= ? AND sequence <= ? "
                "ORDER BY sequence",
                (device_id, from_sequence, to_sequence),
            ).fetchall()
            chain = conn.execute(
                "SELECT last_sequence FROM device_chains WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            head_sequence = chain["last_sequence"] if chain is not None else 0

            by_seq = {r["sequence"]: r for r in rows}
            missing = [
                s for s in range(from_sequence, to_sequence + 1) if s not in by_seq
            ]
            if missing:
                raise BatchJudgmentConflict(
                    message=(
                        f"cannot judge range {from_sequence}..{to_sequence}: "
                        f"missing batch sequence(s) {', '.join(map(str, missing))}"
                    ),
                    reason="range_not_continuous",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    head_sequence=head_sequence,
                    missing_sequences=missing,
                )
            archived = [
                r["sequence"]
                for r in rows
                if r["archived_at"] is not None or r["observations"] is None
            ]
            if archived:
                raise BatchJudgmentConflict(
                    message=(
                        f"cannot judge range {from_sequence}..{to_sequence}: "
                        f"batch sequence(s) {', '.join(map(str, archived))} are "
                        "archived tombstones without observations"
                    ),
                    reason="range_archived",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    archived_sequences=archived,
                )

            thresholds = json.loads(rows[0]["thresholds"])
            mismatched: list[int] = []
            for r in rows[1:]:
                if json.loads(r["thresholds"]) != thresholds:
                    mismatched.append(r["sequence"])
            if mismatched:
                raise BatchJudgmentConflict(
                    message=(
                        "all batches in the range must share the exact same /judge "
                        "thresholds (tolerance/max_drift/max_missed/max_false); "
                        f"sequence(s) {', '.join(map(str, mismatched))} differ from "
                        f"sequence {from_sequence}"
                    ),
                    reason="thresholds_mismatch",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    reference_sequence=from_sequence,
                    mismatched_sequences=mismatched,
                )

            batch_details = [
                {
                    "sequence": r["sequence"],
                    "observations": json.loads(r["observations"]),
                }
                for r in rows
            ]
            merged = merge_batch_observations(batch_details)
            timestamps = [o["captured_at"] for o in merged]

            # 持写锁固定激活版本：判定与保存之间不可能发生版本切换
            library_version, aids = self._active_snapshot_of(conn)
            request_payload, response_payload = runner(
                timestamps, thresholds, library_version, aids
            )

            # 保存点：若唯一约束兜底发现同键已被并发请求占用（多 worker 下
            # 可能越过进程内写锁串行），回滚本请求已写的 judgments 行，
            # 绝不留下孤儿判定或半个范围
            conn.execute("SAVEPOINT batch_judgment_insert")
            judgment_id = self._insert_judgment(
                conn,
                status=response_payload["status"],
                best_aid_id=response_payload["best"]
                and response_payload["best"]["aid_id"],
                matched=response_payload["best"]["matched"]
                if response_payload["best"] is not None
                else 0,
                missed=response_payload["best"]["missed"]
                if response_payload["best"] is not None
                else 0,
                false_count=response_payload["best"]["false"]
                if response_payload["best"] is not None
                else 0,
                drift=response_payload["best"]["drift"]
                if response_payload["best"] is not None
                else None,
                library_version=library_version,
                request=request_payload,
                response=response_payload,
            )
            response_payload["judgment_id"] = judgment_id

            try:
                cur = conn.execute(
                    "INSERT INTO batch_judgments "
                    "(idempotency_key, device_id, from_sequence, to_sequence, "
                    "library_version, observation_count, thresholds, observations, "
                    "judgment_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        idempotency_key,
                        device_id,
                        from_sequence,
                        to_sequence,
                        library_version,
                        len(merged),
                        json.dumps(thresholds, ensure_ascii=False),
                        json.dumps(merged, ensure_ascii=False),
                        judgment_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 唯一约束兜底并发同键：写锁通常已串行化到上面的 SELECT；
                # 多 worker 下若仍有并发事务抢先占用该键，撤销本请求写入
                conn.execute("ROLLBACK TO SAVEPOINT batch_judgment_insert")
                rival = conn.execute(
                    "SELECT * FROM batch_judgments WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                saved = self._get_batch_judgment(conn, int(rival["id"]))
                if (
                    saved is not None
                    and saved["device_id"] == device_id
                    and saved["from_sequence"] == from_sequence
                    and saved["to_sequence"] == to_sequence
                ):
                    return saved, False
                raise BatchJudgmentConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"batch judgment of device {saved['device_id']!r} range "
                        f"{saved['from_sequence']}..{saved['to_sequence']}"
                    ),
                    reason="idempotency_key_in_use",
                    idempotency_key=idempotency_key,
                    occupied_by_batch_judgment_id=saved["batch_judgment_id"],
                    occupied_device_id=saved["device_id"],
                    occupied_from_sequence=saved["from_sequence"],
                    occupied_to_sequence=saved["to_sequence"],
                )
            conn.execute("RELEASE SAVEPOINT batch_judgment_insert")
            batch_judgment_id = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO batch_judgment_batches "
                "(batch_judgment_id, sequence, batch_id, previous_digest, digest, "
                "observation_count) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        batch_judgment_id,
                        r["sequence"],
                        r["batch_id"],
                        r["previous_digest"],
                        r["digest"],
                        r["observation_count"],
                    )
                    for r in rows
                ],
            )
            saved = self._get_batch_judgment(conn, batch_judgment_id)
        return saved, True

    @staticmethod
    def _batch_judgment_batches(conn, batch_judgment_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT sequence, batch_id, previous_digest, digest, observation_count "
            "FROM batch_judgment_batches WHERE batch_judgment_id = ? ORDER BY sequence",
            (batch_judgment_id,),
        ).fetchall()
        return [
            {
                "sequence": r["sequence"],
                "batch_id": r["batch_id"],
                "previous_digest": r["previous_digest"],
                "digest": r["digest"],
                "observation_count": r["observation_count"],
            }
            for r in rows
        ]

    def _get_batch_judgment(self, conn, batch_judgment_id: int) -> dict | None:
        """在给定连接内组装一次连续批次判定的完整不可变来源。"""
        r = conn.execute(
            "SELECT * FROM batch_judgments WHERE id = ?", (batch_judgment_id,)
        ).fetchone()
        if r is None:
            return None
        judgment = conn.execute(
            "SELECT response FROM judgments WHERE id = ?", (r["judgment_id"],)
        ).fetchone()
        judgment_payload = (
            json.loads(judgment["response"]) if judgment is not None else None
        )
        if judgment_payload is not None:
            # judgments 保存的是回填 id 前的响应（同 GET /judgments/{id}），
            # 这里补上来源判定 id 便于直接复核
            judgment_payload["judgment_id"] = r["judgment_id"]
        return {
            "batch_judgment_id": r["id"],
            "idempotency_key": r["idempotency_key"],
            "device_id": r["device_id"],
            "from_sequence": r["from_sequence"],
            "to_sequence": r["to_sequence"],
            "library_version": r["library_version"],
            "observation_count": r["observation_count"],
            "thresholds": json.loads(r["thresholds"]),
            "observations": json.loads(r["observations"]),
            "source_judgment_id": r["judgment_id"],
            "created_at": _iso(r["created_at"]),
            "batches": self._batch_judgment_batches(conn, r["id"]),
            "judgment": judgment_payload,
        }

    def get_batch_judgment(self, device_id: str, batch_judgment_id: int) -> dict | None:
        """查询一次连续批次判定的完整来源；不属于该设备或不存在返回 None（404）。"""
        with closing(self._connect()) as conn:
            found = self._get_batch_judgment(conn, batch_judgment_id)
        if found is None or found["device_id"] != device_id:
            return None
        return found

    def list_batch_judgments(
        self, device_id: str, *, after_id: int = 0, limit: int = 50
    ) -> dict:
        """按 id 游标分页列出设备的连续批次判定（升序，仅摘要，不含判定证据）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM batch_judgments "
                "WHERE device_id = ? AND id > ? ORDER BY id LIMIT ?",
                (device_id, after_id, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [
            {
                "batch_judgment_id": r["id"],
                "idempotency_key": r["idempotency_key"],
                "device_id": r["device_id"],
                "from_sequence": r["from_sequence"],
                "to_sequence": r["to_sequence"],
                "library_version": r["library_version"],
                "observation_count": r["observation_count"],
                "thresholds": json.loads(r["thresholds"]),
                "source_judgment_id": r["judgment_id"],
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]
        return {
            "device_id": device_id,
            "count": len(items),
            "batch_judgments": items,
            "next_cursor": items[-1]["batch_judgment_id"] if has_more and items else None,
        }
