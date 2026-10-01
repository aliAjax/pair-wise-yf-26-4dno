"""数字档案长期保存服务：SQLite 多副本、哈希校验、修复与迁移。"""
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import sqlite3
import threading
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "preservation.db"
MAX_FILE_SIZE = 10 * 1024 * 1024


class BusinessError(Exception):
    def __init__(self, message: str, status: int = 400, code: str = "bad_request"):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def verify_manifest(files: object) -> list[dict]:
    if not isinstance(files, list) or not files:
        raise BusinessError("files 必须是非空数组", 422, "invalid_manifest")
    result, seen = [], set()
    for item in files:
        if not isinstance(item, dict):
            raise BusinessError("文件条目必须是对象", 422, "invalid_manifest")
        raw_path = str(item.get("path", "")).strip().replace("\\", "/")
        pure = PurePosixPath(raw_path)
        if not raw_path or pure.is_absolute() or ".." in pure.parts or pure.name in {"", ".", ".."}:
            raise BusinessError(f"档案路径不安全: {raw_path}", 422, "unsafe_path")
        if raw_path in seen:
            raise BusinessError(f"档案路径重复: {raw_path}", 409, "duplicate_path")
        seen.add(raw_path)
        encoded = item.get("content_b64")
        if not isinstance(encoded, str):
            raise BusinessError(f"{raw_path} 缺少 content_b64", 422, "content_required")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            raise BusinessError(f"{raw_path} 不是合法 Base64", 422, "invalid_base64")
        if len(content) > MAX_FILE_SIZE:
            raise BusinessError(f"{raw_path} 超过单文件大小限制", 413, "file_too_large")
        result.append(
            {"path": raw_path, "content": content, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)}
        )
    return result


class PreservationStore:
    def __init__(self, db_path: str | Path = DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def init_schema(self) -> None:
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS institutions(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('owner','archivist','auditor')),
                    institution_id TEXT REFERENCES institutions(id)
                );
                INSERT OR IGNORE INTO institutions(id,name) VALUES
                    ('org-a','甲数字保存机构'),
                    ('org-b','乙数字保存机构');
                CREATE TABLE IF NOT EXISTS archives(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    owner_id TEXT NOT NULL REFERENCES users(id),
                    custodian_id TEXT REFERENCES institutions(id),
                    retention_until TEXT NOT NULL,
                    restricted INTEGER NOT NULL DEFAULT 1 CHECK(restricted IN (0,1)),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS archive_members(
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    user_id TEXT NOT NULL REFERENCES users(id),
                    permission TEXT NOT NULL CHECK(permission IN ('read','write')),
                    PRIMARY KEY(archive_id,user_id)
                );
                CREATE TABLE IF NOT EXISTS archive_versions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    version INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'verified' CHECK(state IN ('verified','degraded')),
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL,
                    UNIQUE(archive_id,version)
                );
                CREATE TABLE IF NOT EXISTS archive_files(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    UNIQUE(version_id,path)
                );
                CREATE TABLE IF NOT EXISTS copies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    location TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'healthy' CHECK(state IN ('healthy','corrupt','degraded')),
                    created_at TEXT NOT NULL,
                    last_verified_at TEXT,
                    UNIQUE(version_id,location)
                );
                CREATE TABLE IF NOT EXISTS copy_files(
                    copy_id INTEGER NOT NULL REFERENCES copies(id),
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    PRIMARY KEY(copy_id,path)
                );
                CREATE TABLE IF NOT EXISTS migrations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    target_version_id INTEGER NOT NULL UNIQUE REFERENCES archive_versions(id),
                    source_path TEXT NOT NULL,
                    target_path TEXT NOT NULL,
                    target_format TEXT NOT NULL,
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transfer_batches(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    kind TEXT NOT NULL DEFAULT 'transfer' CHECK(kind IN ('transfer','legacy')),
                    source_institution_id TEXT NOT NULL REFERENCES institutions(id),
                    target_institution_id TEXT NOT NULL REFERENCES institutions(id),
                    state TEXT NOT NULL CHECK(state IN ('pending','completed','invalidated')),
                    retention_snapshot TEXT NOT NULL,
                    blockers TEXT NOT NULL DEFAULT '[]',
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL,
                    ended_at TEXT
                );
                CREATE TABLE IF NOT EXISTS transfer_items(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES transfer_batches(id),
                    copy_id INTEGER REFERENCES copies(id) ON DELETE SET NULL,
                    location TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('pending','written','confirmed','failed','blocked')),
                    written_at TEXT,
                    confirmed_by TEXT REFERENCES users(id),
                    confirmed_at TEXT,
                    UNIQUE(batch_id,seq)
                );
                CREATE TABLE IF NOT EXISTS transfer_item_files(
                    item_id INTEGER NOT NULL REFERENCES transfer_items(id),
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    PRIMARY KEY(item_id,path)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_transfer_pending_archive
                    ON transfer_batches(archive_id) WHERE state='pending';
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)

    def _has_column(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        return any(r["name"] == column for r in conn.execute(f"PRAGMA table_info({table})").fetchall())

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """把移交功能上线前的旧库升级到当前结构，并补齐机构、保管机构和单件批次。"""
        if not self._has_column(conn, "users", "institution_id"):
            conn.execute("ALTER TABLE users ADD COLUMN institution_id TEXT REFERENCES institutions(id)")
        if not self._has_column(conn, "archives", "custodian_id"):
            conn.execute("ALTER TABLE archives ADD COLUMN custodian_id TEXT REFERENCES institutions(id)")
        conn.execute(
            "INSERT OR IGNORE INTO users(id,name,role,institution_id) VALUES ('system','系统迁移','auditor',NULL)"
        )
        conn.execute(
            "UPDATE users SET institution_id='org-a' WHERE id IN ('owner','archivist') AND institution_id IS NULL"
        )
        conn.execute(
            """UPDATE archives SET custodian_id=
                   COALESCE((SELECT u.institution_id FROM users u WHERE u.id=archives.owner_id),'org-a')
               WHERE custodian_id IS NULL"""
        )
        self._ensure_legacy_batches(conn)

    def seed(self) -> None:
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name,role,institution_id) VALUES(?,?,?,?)",
                [
                    ("owner", "机构档案负责人", "owner", "org-a"),
                    ("archivist", "档案管理员", "archivist", "org-a"),
                    ("owner-b", "接收机构负责人", "owner", "org-b"),
                    ("archivist-b", "接收机构档案员", "archivist", "org-b"),
                    ("auditor", "独立审计员", "auditor", None),
                    ("outsider", "未授权访客", "archivist", None),
                ],
            )

    def _ensure_legacy_batches(self, conn: sqlite3.Connection) -> int:
        """旧档案没有移交批次：每件档案补一条已完成的单件批次，之后照常查看和校验。"""
        rows = conn.execute(
            """SELECT a.* FROM archives a
               WHERE NOT EXISTS (SELECT 1 FROM transfer_batches t WHERE t.archive_id=a.id)"""
        ).fetchall()
        for archive in rows:
            ts = now()
            cur = conn.execute(
                """INSERT INTO transfer_batches(
                       archive_id,kind,source_institution_id,target_institution_id,state,
                       retention_snapshot,blockers,created_by,created_at,ended_at)
                   VALUES(?,?,?,?, 'completed', ?, '[]', 'system', ?,?)""",
                (archive["id"], "legacy", archive["custodian_id"], archive["custodian_id"],
                 archive["retention_until"], ts, ts),
            )
            conn.execute(
                """INSERT INTO transfer_items(batch_id,copy_id,location,seq,state,confirmed_by,confirmed_at)
                   VALUES(?,?, 'archive:' || ?, 1, 'confirmed', 'system', ?)""",
                (cur.lastrowid, None, archive["name"], ts),
            )
        return len(rows)

    def _user(self, conn, user_id: str | None, roles: set[str] | None = None) -> sqlite3.Row:
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在", 401, "unknown_user")
        if roles and user["role"] not in roles:
            raise BusinessError("当前角色无权执行此操作", 403, "forbidden")
        return user

    def _access(
        self, conn, archive_id: int, user: sqlite3.Row, require_write: bool = False,
        freeze: bool | None = None,
    ) -> sqlite3.Row:
        """返回档案行。移交进行中双方可查看，但写入（新建版本/增删副本/迁移）被冻结。"""
        archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
        if not archive:
            raise BusinessError("档案不存在", 404, "not_found")
        if user["role"] == "auditor":
            return archive
        participant = user["institution_id"] is not None and user["institution_id"] == archive["custodian_id"]
        if not participant:
            # 进行中：双方可查看；失效后：双方仍需查看阻塞项；完成后：仅新保管机构
            rows = conn.execute(
                """SELECT state,source_institution_id AS src,target_institution_id AS tgt
                   FROM transfer_batches WHERE archive_id=? AND kind='transfer' AND state IN ('pending','invalidated')""",
                (archive_id,),
            ).fetchall()
            parties = set()
            for r in rows:
                parties.add(r["src"]); parties.add(r["tgt"])
            participant = user["institution_id"] in parties
        if archive["owner_id"] != user["id"] and not participant:
            row = conn.execute(
                "SELECT permission FROM archive_members WHERE archive_id=? AND user_id=?", (archive_id, user["id"])
            ).fetchone()
            if not row or (require_write and row["permission"] != "write"):
                raise BusinessError("没有该受限档案的访问权限", 403, "forbidden")
        if freeze is None:
            freeze = require_write
        if freeze:
            pending = conn.execute(
                "SELECT id FROM transfer_batches WHERE archive_id=? AND state='pending'", (archive_id,)
            ).fetchone()
            if pending:
                raise BusinessError(
                    f"档案正在移交（批次 {pending['id']}）：冻结期内不能新建版本或移除副本", 409, "transfer_frozen"
                )
        return archive

    def _active_transfer(self, conn, archive_id: int) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM transfer_batches WHERE archive_id=? AND state='pending'", (archive_id,)
        ).fetchone()

    def _audit(self, conn, archive_id: int, actor: str, action: str, detail: dict) -> None:
        conn.execute(
            "INSERT INTO audit_log(archive_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (archive_id, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    def create_archive(self, user_id: str, name: str, retention_until: str, restricted: bool = True) -> dict:
        name = name.strip()
        if len(name) < 2:
            raise BusinessError("档案名称至少 2 字", 422, "invalid_name")
        try:
            deadline = date.fromisoformat(retention_until)
        except ValueError:
            raise BusinessError("retention_until 必须是 YYYY-MM-DD", 422, "invalid_retention")
        if deadline < date.today():
            raise BusinessError("保留期限不能早于今天", 422, "retention_in_past")
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist"})
            try:
                cur = conn.execute(
                    "INSERT INTO archives(name,owner_id,custodian_id,retention_until,restricted,created_at) VALUES(?,?,?,?,?,?)",
                    (name, user_id, user["institution_id"], retention_until, int(bool(restricted)), now()),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("档案名称已存在", 409, "archive_exists")
            archive_id = cur.lastrowid
            conn.execute(
                "INSERT INTO archive_members(archive_id,user_id,permission) VALUES(?,?,'write')", (archive_id, user_id)
            )
            self._audit(conn, archive_id, user_id, "archive.create", {"retention_until": retention_until, "restricted": restricted})
            return {"id": archive_id, "name": name, "retention_until": retention_until, "restricted": restricted}

    def grant(self, actor_id: str, archive_id: int, user_id: str, permission: str) -> dict:
        if permission not in {"read", "write"}:
            raise BusinessError("permission 必须是 read 或 write", 422, "invalid_permission")
        with self.connect() as conn:
            actor = self._user(conn, actor_id)
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
            if not archive:
                raise BusinessError("档案不存在", 404, "not_found")
            if archive["owner_id"] != actor_id:
                raise BusinessError("只有档案所有者可以授权", 403, "forbidden")
            self._user(conn, user_id)
            conn.execute(
                """INSERT INTO archive_members(archive_id,user_id,permission) VALUES(?,?,?)
                   ON CONFLICT(archive_id,user_id) DO UPDATE SET permission=excluded.permission""",
                (archive_id, user_id, permission),
            )
            self._audit(conn, archive_id, actor_id, "access.grant", {"user_id": user_id, "permission": permission})
            return {"archive_id": archive_id, "user_id": user_id, "permission": permission}

    def ingest_version(self, actor_id: str, archive_id: int, files: object) -> dict:
        manifest = verify_manifest(files)
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            self._access(conn, archive_id, actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (archive_id,)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (archive_id, version_no, actor_id, now()),
                )
                version_id = cur.lastrowid
                for item in manifest:
                    conn.execute(
                        "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                        (version_id, item["path"], item["sha256"], item["size"], item["content"]),
                    )
                self._audit(
                    conn, archive_id, actor_id, "version.ingest",
                    {"version_id": version_id, "version": version_no, "files": len(manifest),
                     "manifest": [{"path": x["path"], "sha256": x["sha256"], "size": x["size"]} for x in manifest]},
                )
                return {"id": version_id, "archive_id": archive_id, "version": version_no, "file_count": len(manifest)}
            except Exception:
                conn.rollback()
                raise

    def add_copy(self, actor_id: str, version_id: int, location: str) -> dict:
        location = location.strip()
        if len(location) < 2:
            raise BusinessError("副本位置不能为空", 422, "invalid_location")
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not version:
                raise BusinessError("档案版本不存在", 404, "not_found")
            self._access(conn, version["archive_id"], actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    "INSERT INTO copies(version_id,location,created_at,last_verified_at) VALUES(?,?,?,?)",
                    (version_id, location, now(), now()),
                )
                copy_id = cur.lastrowid
                conn.execute(
                    """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                       SELECT ?,path,sha256,size,content FROM archive_files WHERE version_id=?""",
                    (copy_id, version_id),
                )
                self._audit(conn, version["archive_id"], actor_id, "copy.create", {"copy_id": copy_id, "version_id": version_id, "location": location})
                return {"id": copy_id, "version_id": version_id, "location": location, "state": "healthy"}
            except sqlite3.IntegrityError:
                conn.rollback()
                raise BusinessError("该版本的副本位置已存在", 409, "copy_exists")
            except Exception:
                conn.rollback()
                raise

    def get_version(self, user_id: str, version_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not version:
                raise BusinessError("档案版本不存在", 404, "not_found")
            self._access(conn, version["archive_id"], user)
            files = conn.execute(
                "SELECT path,sha256,size FROM archive_files WHERE version_id=? ORDER BY path", (version_id,)
            ).fetchall()
            copies = conn.execute(
                "SELECT id,location,state,last_verified_at FROM copies WHERE version_id=? ORDER BY id", (version_id,)
            ).fetchall()
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (version["archive_id"],)).fetchone()
            return {"version": dict(version), "archive": dict(archive), "files": [dict(x) for x in files], "copies": [dict(x) for x in copies]}

    def verify_copy(self, user_id: str, copy_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
                if not copy:
                    raise BusinessError("副本不存在", 404, "not_found")
                version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
                self._access(conn, version["archive_id"], user)
                stored = conn.execute(
                    "SELECT path,sha256,size,content FROM copy_files WHERE copy_id=? ORDER BY path", (copy_id,)
                ).fetchall()
                corrupt_paths = [r["path"] for r in stored if hashlib.sha256(r["content"]).hexdigest() != r["sha256"] or len(r["content"]) != r["size"]]
                repaired = False
                if not corrupt_paths:
                    conn.execute("UPDATE copies SET state='healthy',last_verified_at=? WHERE id=?", (now(), copy_id))
                    result_state = "healthy"
                else:
                    conn.execute("UPDATE copies SET state='corrupt',last_verified_at=? WHERE id=?", (now(), copy_id))
                    healthy = conn.execute(
                        "SELECT id FROM copies WHERE version_id=? AND id<>? AND state='healthy' ORDER BY last_verified_at DESC LIMIT 1",
                        (copy["version_id"], copy_id),
                    ).fetchone()
                    result_state = "degraded"
                    if healthy:
                        donor = conn.execute(
                            "SELECT path,sha256,size,content FROM copy_files WHERE copy_id=? ORDER BY path", (healthy["id"],)
                        ).fetchall()
                        donor_by_path = {r["path"]: r for r in donor}
                        expected = {r["path"]: r for r in conn.execute(
                            "SELECT path,sha256,size FROM archive_files WHERE version_id=?", (copy["version_id"],)
                        ).fetchall()}
                        if set(donor_by_path) == set(expected) and all(
                            hashlib.sha256(donor_by_path[p]["content"]).hexdigest() == expected[p]["sha256"] for p in expected
                        ):
                            conn.execute("DELETE FROM copy_files WHERE copy_id=?", (copy_id,))
                            conn.execute(
                                """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                                   SELECT ?,path,sha256,size,content FROM copy_files WHERE copy_id=?""",
                                (copy_id, healthy["id"]),
                            )
                            conn.execute("UPDATE copies SET state='healthy',last_verified_at=? WHERE id=?", (now(), copy_id))
                            repaired, result_state = True, "healthy"
                    if result_state == "degraded":
                        conn.execute("UPDATE archive_versions SET state='degraded' WHERE id=?", (copy["version_id"],))
                if corrupt_paths:
                    # 任一副本验坏（即使随后自动修复），进行中的移交批次立即失效并列出阻塞项
                    invalidated = self._invalidate_pending_for_archive(
                        conn, version["archive_id"], "copy_corrupt",
                        [{"copy_id": copy_id, "corrupt_paths": corrupt_paths, "repaired": repaired}], user_id,
                    )
                else:
                    invalidated = []
                self._audit(
                    conn, version["archive_id"], user_id, "copy.verify",
                    {"copy_id": copy_id, "state": result_state, "corrupt_paths": corrupt_paths,
                     "repaired": repaired, "invalidated_batches": invalidated},
                )
                return {"copy_id": copy_id, "state": result_state, "corrupt_paths": corrupt_paths,
                        "repaired": repaired, "invalidated_batches": invalidated}
            except Exception:
                conn.rollback()
                raise

    def simulate_corruption(self, user_id: str, copy_id: int, path: str) -> dict:
        """仅用于演示和测试，在受控环境中模拟底层介质损坏。"""
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist"})
            copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
            if not copy:
                raise BusinessError("副本不存在", 404, "not_found")
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
            # 演示/审计工具：移交双方都可在冻结期内注入损坏以观察批次失效，但不属于版本/副本变更
            self._access(conn, version["archive_id"], user, require_write=True, freeze=False)
            row = conn.execute("SELECT content FROM copy_files WHERE copy_id=? AND path=?", (copy_id, path)).fetchone()
            if not row:
                raise BusinessError("副本文件不存在", 404, "not_found")
            damaged = bytes([row["content"][0] ^ 0xFF]) + row["content"][1:] if row["content"] else b"corrupt"
            conn.execute("UPDATE copy_files SET content=? WHERE copy_id=? AND path=?", (damaged, copy_id, path))
            conn.execute("UPDATE copies SET state='corrupt' WHERE id=?", (copy_id,))
            self._audit(conn, version["archive_id"], user_id, "copy.simulate_corruption", {"copy_id": copy_id, "path": path})
            return {"copy_id": copy_id, "path": path, "state": "corrupt"}

    def migrate(self, actor_id: str, version_id: int, source_path: str, target_path: str, target_format: str, content_b64: str) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            source_version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not source_version:
                raise BusinessError("源档案版本不存在", 404, "not_found")
            self._access(conn, source_version["archive_id"], actor, require_write=True)
            source = conn.execute(
                "SELECT * FROM archive_files WHERE version_id=? AND path=?", (version_id, source_path)
            ).fetchone()
            if not source:
                raise BusinessError("源文件不存在", 404, "source_not_found")
            converted = verify_manifest([{"path": target_path, "content_b64": content_b64}])[0]
            try:
                conn.execute("BEGIN IMMEDIATE")
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (source_version["archive_id"],)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (source_version["archive_id"], version_no, actor_id, now()),
                )
                target_version_id = cur.lastrowid
                conn.execute(
                    """INSERT INTO archive_files(version_id,path,sha256,size,content)
                       SELECT ?,path,sha256,size,content FROM archive_files
                       WHERE version_id=? AND path<>?""",
                    (target_version_id, version_id, source_path),
                )
                conn.execute(
                    "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                    (target_version_id, converted["path"], converted["sha256"], converted["size"], converted["content"]),
                )
                conn.execute(
                    "INSERT INTO migrations(source_version_id,target_version_id,source_path,target_path,target_format,actor_id,created_at) VALUES(?,?,?,?,?,?,?)",
                    (version_id, target_version_id, source_path, converted["path"], target_format.strip(), actor_id, now()),
                )
                self._audit(
                    conn, source_version["archive_id"], actor_id, "format.migrate",
                    {"source_version_id": version_id, "target_version_id": target_version_id,
                     "source_path": source_path, "target_path": converted["path"], "target_format": target_format.strip()},
                )
                return {"id": target_version_id, "version": version_no, "source_version_id": version_id, "target_path": converted["path"]}
            except Exception:
                conn.rollback()
                raise

    def list_institutions(self, user_id: str) -> list[dict]:
        with self.connect() as conn:
            self._user(conn, user_id)
            return [dict(r) for r in conn.execute("SELECT id,name FROM institutions ORDER BY id")]

    # ---------------- 机构间移交 ----------------

    def start_transfer(self, actor_id: str, archive_id: int, target_institution_id: str) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                # 两个提交抢同一档案：锁内先查进行中批次，保证后到者拿到稳定的冲突码
                archive = self._access(conn, archive_id, actor, require_write=True, freeze=False)
                if self._active_transfer(conn, archive_id):
                    raise BusinessError("该档案已有进行中的移交批次", 409, "transfer_conflict")
            except Exception:
                conn.rollback()
                raise
            source = archive["custodian_id"]
            target = conn.execute("SELECT id FROM institutions WHERE id=?", (target_institution_id,)).fetchone()
            if not target:
                conn.rollback()
                raise BusinessError("接收机构不存在", 404, "institution_not_found")
            if target["id"] == source:
                conn.rollback()
                raise BusinessError("接收机构不能是当前保管机构", 422, "same_institution")
            copies = conn.execute(
                """SELECT c.id,c.location FROM copies c
                   JOIN archive_versions v ON v.id=c.version_id
                   WHERE v.archive_id=? ORDER BY v.version,c.id""",
                (archive_id,),
            ).fetchall()
            if not copies:
                conn.rollback()
                raise BusinessError("档案还没有任何副本，无法移交", 422, "no_copies")
            if self._active_transfer(conn, archive_id):
                conn.rollback()
                raise BusinessError("该档案已有进行中的移交批次", 409, "transfer_conflict")
            ts = now()
            try:
                cur = conn.execute(
                    """INSERT INTO transfer_batches(
                           archive_id,kind,source_institution_id,target_institution_id,state,
                           retention_snapshot,blockers,created_by,created_at)
                       VALUES(?, 'transfer', ?,?, 'pending', ?, '[]', ?,?)""",
                    (archive_id, source, target["id"], archive["retention_until"], actor_id, ts),
                )
                batch_id = cur.lastrowid
                for seq, copy in enumerate(copies, 1):
                    conn.execute(
                        "INSERT INTO transfer_items(batch_id,copy_id,location,seq,state) VALUES(?,?,?,?, 'pending')",
                        (batch_id, copy["id"], copy["location"], seq),
                    )
                self._audit(
                    conn, archive_id, actor_id, "transfer.start",
                    {"batch_id": batch_id, "source": source, "target": target["id"], "copies": len(copies)},
                )
                return self._batch_dict(conn, batch_id)
            except sqlite3.IntegrityError:
                conn.rollback()
                raise BusinessError("该档案已有进行中的移交批次", 409, "transfer_conflict")
            except Exception:
                conn.rollback()
                raise

    def list_transfers(self, user_id: str, archive_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_legacy_batches(conn)
                self._access(conn, archive_id, user)
                batches = conn.execute(
                    "SELECT id FROM transfer_batches WHERE archive_id=? ORDER BY id", (archive_id,)
                ).fetchall()
                result = [self._batch_dict(conn, b["id"]) for b in batches]
                conn.commit()
                return {"archive_id": archive_id, "batches": result}
            except Exception:
                conn.rollback()
                raise

    def get_transfer(self, user_id: str, batch_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            batch = conn.execute("SELECT * FROM transfer_batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise BusinessError("移交批次不存在", 404, "not_found")
            self._access(conn, batch["archive_id"], user)
            return self._batch_dict(conn, batch_id)

    def _batch_dict(self, conn, batch_id: int) -> dict:
        batch = conn.execute("SELECT * FROM transfer_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise BusinessError("移交批次不存在", 404, "not_found")
        items = conn.execute(
            "SELECT id,copy_id,location,seq,state,written_at,confirmed_by,confirmed_at FROM transfer_items WHERE batch_id=? ORDER BY seq",
            (batch_id,),
        ).fetchall()
        d = {k: batch[k] for k in batch.keys()}
        d["blockers"] = json.loads(d["blockers"])
        d["items"] = [dict(i) for i in items]
        d["total"] = len(items)
        d["confirmed"] = sum(1 for i in items if i["state"] == "confirmed")
        d["written"] = sum(1 for i in items if i["state"] in ("written", "confirmed"))
        return d

    def write_transfer(
        self, actor_id: str, batch_id: int, item_id: int | None = None,
        fail_after: int | None = None, fail_item_id: int | None = None,
    ) -> dict:
        """接收机构把副本写入自己一侧。逐条提交形成断点；已写入/确认的条目重试时跳过。"""
        batch, archive, actor = self._load_write_batch(actor_id, batch_id)
        written, skipped, failed = [], [], None
        prefetch = self.connect()
        try:
            prefetch.execute("BEGIN IMMEDIATE")
            items = prefetch.execute(
                "SELECT * FROM transfer_items WHERE batch_id=? AND state='pending' ORDER BY seq", (batch_id,)
            ).fetchall()
            if item_id is not None:
                items = [i for i in items if i["id"] == item_id]
                if not items and not prefetch.execute(
                    "SELECT 1 FROM transfer_items WHERE batch_id=? AND id=?", (batch_id, item_id)
                ).fetchone():
                    raise BusinessError("批次中没有该条目", 404, "item_not_found")
            prefetch.commit()
        finally:
            prefetch.close()
        for index, item in enumerate(items):
            if fail_after is not None and index >= fail_after:
                failed = item["id"]
                break
            if fail_item_id is not None and item["id"] == fail_item_id:
                failed = item["id"]
                break
            conn = self.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                state = conn.execute("SELECT state FROM transfer_batches WHERE id=?", (batch_id,)).fetchone()["state"]
                if state != "pending":
                    raise BusinessError(f"批次已 {state}，不能继续写入", 409, "batch_not_pending")
                row = conn.execute(
                    "SELECT * FROM transfer_items WHERE id=?", (item["id"],)
                ).fetchone()
                if row["state"] != "pending":
                    skipped.append(item["id"])
                    conn.commit()
                    continue
                if row["copy_id"] is None or not conn.execute(
                    "SELECT 1 FROM copies WHERE id=?", (row["copy_id"],)
                ).fetchone():
                    self._invalidate_batch(
                        conn, batch, "source_copy_missing",
                        [{"item_id": item["id"], "copy_id": item["copy_id"], "reason": "源副本已不存在"}],
                        actor_id,
                    )
                    conn.commit()
                    failed = item["id"]
                    break
                conn.execute(
                    """INSERT INTO transfer_item_files(item_id,path,sha256,size,content)
                       SELECT ?,path,sha256,size,content FROM copy_files WHERE copy_id=?""",
                    (item["id"], item["copy_id"]),
                )
                ts = now()
                conn.execute(
                    "UPDATE transfer_items SET state='written',written_at=? WHERE id=?", (ts, item["id"])
                )
                self._audit(
                    conn, archive["id"], actor_id, "transfer.write",
                    {"batch_id": batch_id, "item_id": item["id"], "copy_id": item["copy_id"]},
                )
                conn.commit()
                written.append(item["id"])
            except BusinessError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                failed = item["id"]
                break
            finally:
                conn.close()
        result = self.get_transfer(actor_id, batch_id)
        result.update({"written": written, "skipped": skipped, "remaining_pending": result["total"] - result["written"]})
        if failed is not None:
            raise BusinessError(
                f"条目 {failed} 写入失败，断点已保留，可重试（已确认条目不重做）", 500, "transfer_write_failed"
            )
        return result

    def _load_write_batch(self, actor_id: str, batch_id: int):
        conn = self.connect()
        try:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            batch = conn.execute("SELECT * FROM transfer_batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise BusinessError("移交批次不存在", 404, "not_found")
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (batch["archive_id"],)).fetchone()
            if actor["institution_id"] != batch["target_institution_id"]:
                raise BusinessError("只有接收机构可以写入并核验移交副本", 403, "forbidden")
            if batch["state"] != "pending":
                raise BusinessError(f"批次已 {batch['state']}", 409, "batch_not_pending")
            return batch, archive, actor
        finally:
            conn.close()

    def confirm_transfer_item(self, actor_id: str, batch_id: int, item_id: int) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                batch = conn.execute("SELECT * FROM transfer_batches WHERE id=?", (batch_id,)).fetchone()
                if not batch:
                    raise BusinessError("移交批次不存在", 404, "not_found")
                archive = conn.execute("SELECT * FROM archives WHERE id=?", (batch["archive_id"],)).fetchone()
                if actor["institution_id"] != batch["target_institution_id"]:
                    raise BusinessError("只有接收机构可以确认移交副本", 403, "forbidden")
                if batch["state"] != "pending":
                    raise BusinessError(f"批次已 {batch['state']}", 409, "batch_not_pending")
                item = conn.execute("SELECT * FROM transfer_items WHERE id=? AND batch_id=?", (item_id, batch_id)).fetchone()
                if not item:
                    raise BusinessError("批次中没有该条目", 404, "item_not_found")
                if item["state"] == "confirmed":
                    result = self._batch_dict(conn, batch_id)
                    result["idempotent"] = True
                    conn.commit()
                    return result
                if item["state"] != "written":
                    raise BusinessError("该条目尚未写入接收侧，无法确认", 422, "item_not_written")
                # 在对方侧重新做 SHA-256 内容核验
                staged = conn.execute(
                    "SELECT path,sha256,size,content FROM transfer_item_files WHERE item_id=? ORDER BY path",
                    (item_id,),
                ).fetchall()
                expected = conn.execute(
                    """SELECT f.path,f.sha256,f.size FROM archive_files f
                       JOIN copies c ON c.version_id=f.version_id
                       WHERE c.id=? ORDER BY f.path""",
                    (item["copy_id"],),
                ).fetchall()
                blockers = []
                if len(staged) != len(expected) or {s["path"] for s in staged} != {e["path"] for e in expected}:
                    blockers.append({"item_id": item_id, "copy_id": item["copy_id"], "reason": "文件清单与源副本不一致"})
                else:
                    for s, e in zip(staged, expected):
                        if len(s["content"]) != s["size"] or hashlib.sha256(s["content"]).hexdigest() != s["sha256"]:
                            blockers.append({"item_id": item_id, "copy_id": item["copy_id"], "path": s["path"], "reason": "副本内容验坏"})
                        elif (s["sha256"], s["size"]) != (e["sha256"], e["size"]):
                            blockers.append({"item_id": item_id, "copy_id": item["copy_id"], "path": s["path"], "reason": "与档案正本哈希不一致"})
                if blockers:
                    self._invalidate_batch(conn, batch, "verify_failed", blockers, actor_id)
                    conn.commit()
                    raise BusinessError("核验未通过，批次已失效", 409, "verify_failed")
                ts = now()
                conn.execute(
                    "UPDATE transfer_items SET state='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?",
                    (actor_id, ts, item_id),
                )
                self._audit(
                    conn, archive["id"], actor_id, "transfer.confirm",
                    {"batch_id": batch_id, "item_id": item_id, "copy_id": item["copy_id"]},
                )
                pending = conn.execute(
                    "SELECT COUNT(*) AS n FROM transfer_items WHERE batch_id=? AND state<>'confirmed'", (batch_id,)
                ).fetchone()["n"]
                completed = None
                if pending == 0:
                    completed = self._complete_transfer(conn, batch, archive, actor)
                result = self._batch_dict(conn, batch_id)
                if completed:
                    result["completed"] = completed
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def _complete_transfer(self, conn, batch: sqlite3.Row, archive: sqlite3.Row, actor: sqlite3.Row) -> dict:
        """全部副本在对方侧验过后才更换保管权。"""
        target = batch["target_institution_id"]
        new_owner = conn.execute(
            "SELECT id FROM users WHERE institution_id=? AND role='owner' ORDER BY id LIMIT 1", (target,)
        ).fetchone()
        owner_id = new_owner["id"] if new_owner else actor["id"]
        conn.execute("UPDATE archives SET owner_id=?,custodian_id=? WHERE id=?", (owner_id, target, archive["id"]))
        conn.execute(
            """DELETE FROM archive_members WHERE archive_id=? AND user_id IN (
                   SELECT id FROM users WHERE institution_id=?)""",
            (archive["id"], batch["source_institution_id"]),
        )
        conn.execute(
            "UPDATE transfer_batches SET state='completed',ended_at=? WHERE id=?", (now(), batch["id"])
        )
        self._audit(
            conn, archive["id"], actor["id"], "transfer.complete",
            {"batch_id": batch["id"], "source": batch["source_institution_id"], "target": target,
             "new_owner": owner_id},
        )
        return {"custodian_id": target, "owner_id": owner_id}

    def _invalidate_batch(
        self, conn, batch: sqlite3.Row, reason: str, blockers: list[dict], actor_id: str
    ) -> None:
        for b in blockers:
            b.setdefault("batch_id", batch["id"])
        existing = json.loads(batch["blockers"])
        conn.execute(
            "UPDATE transfer_batches SET state='invalidated',blockers=? WHERE id=?",
            (json.dumps(existing + blockers, ensure_ascii=False, sort_keys=True), batch["id"]),
        )
        conn.execute(
            "UPDATE transfer_items SET state='blocked' WHERE batch_id=? AND state IN ('pending','written','failed')",
            (batch["id"],),
        )
        self._audit(
            conn, batch["archive_id"], actor_id, "transfer.invalidate",
            {"batch_id": batch["id"], "reason": reason, "blockers": blockers},
        )

    def _invalidate_pending_for_archive(self, conn, archive_id: int, reason: str, blockers: list[dict], actor_id: str) -> list[int]:
        batches = conn.execute(
            "SELECT * FROM transfer_batches WHERE archive_id=? AND state='pending'", (archive_id,)
        ).fetchall()
        ids = []
        for batch in batches:
            ids.append(batch["id"])
            self._invalidate_batch(conn, batch, reason, blockers, actor_id)
        return ids

    def update_retention(self, actor_id: str, archive_id: int, retention_until: str) -> dict:
        try:
            deadline = date.fromisoformat(retention_until)
        except ValueError:
            raise BusinessError("retention_until 必须是 YYYY-MM-DD", 422, "invalid_retention")
        if deadline < date.today():
            raise BusinessError("保留期限不能早于今天", 422, "retention_in_past")
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                archive = self._access(conn, archive_id, actor, require_write=True, freeze=False)
                old = archive["retention_until"]
                invalidated = []
                if old != retention_until:
                    conn.execute("UPDATE archives SET retention_until=? WHERE id=?", (retention_until, archive_id))
                    # 保留期限变化：未完成批次立即失效并列出阻塞项
                    invalidated = self._invalidate_pending_for_archive(
                        conn, archive_id, "retention_changed",
                        [{"old_retention_until": old, "new_retention_until": retention_until}], actor_id,
                    )
                    self._audit(
                        conn, archive_id, actor_id, "retention.update",
                        {"old": old, "new": retention_until, "invalidated_batches": invalidated},
                    )
                conn.commit()
                return {"archive_id": archive_id, "retention_until": retention_until,
                        "invalidated_batches": invalidated}
            except Exception:
                conn.rollback()
                raise

    def remove_copy(self, actor_id: str, copy_id: int) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
                if not copy:
                    raise BusinessError("副本不存在", 404, "not_found")
                version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
                self._access(conn, version["archive_id"], actor, require_write=True)  # 冻结期 409
                conn.execute("DELETE FROM transfer_item_files WHERE item_id IN (SELECT id FROM transfer_items WHERE copy_id=?)", (copy_id,))
                conn.execute("DELETE FROM copy_files WHERE copy_id=?", (copy_id,))
                conn.execute("DELETE FROM copies WHERE id=?", (copy_id,))
                self._audit(conn, version["archive_id"], actor_id, "copy.remove",
                            {"copy_id": copy_id, "version_id": copy["version_id"], "location": copy["location"]})
                conn.commit()
                return {"removed": copy_id, "location": copy["location"]}
            except Exception:
                conn.rollback()
                raise

    def archive_status(self, user_id: str, archive_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_legacy_batches(conn)
                self._access(conn, archive_id, user)
                archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
                versions = conn.execute("SELECT id,version,state,created_at FROM archive_versions WHERE archive_id=? ORDER BY version", (archive_id,)).fetchall()
                deadline = date.fromisoformat(archive["retention_until"])
                active = self._active_transfer(conn, archive_id)
                result = {
                    "archive": dict(archive),
                    "days_remaining": (deadline - date.today()).days,
                    "active_transfer": self._batch_dict(conn, active["id"]) if active else None,
                    "versions": [dict(v) | {"file_count": conn.execute("SELECT COUNT(*) FROM archive_files WHERE version_id=?", (v["id"],)).fetchone()[0],
                                             "copy_count": conn.execute("SELECT COUNT(*) FROM copies WHERE version_id=?", (v["id"],)).fetchone()[0]}
                                 for v in versions],
                    "audit": [dict(r) | {"detail": json.loads(r["detail"])} for r in conn.execute("SELECT * FROM audit_log WHERE archive_id=? ORDER BY id", (archive_id,)).fetchall()],
                }
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


class Handler(BaseHTTPRequestHandler):
    server_version = "Preservation/1.0"

    def _store(self):
        return self.server.store  # type: ignore[attr-defined]

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict):
            raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _dispatch(self, method: str) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        parts = [p for p in path.split("/") if p]
        user = self.headers.get("X-User-Id", "")
        if method == "GET" and path == "/":
            body = (BASE_DIR / "web" / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if method == "GET" and path == "/health":
            return self._send(200, {"ok": True})
        store = self._store()
        if method == "GET" and path == "/api/institutions":
            return self._send(200, {"institutions": store.list_institutions(user)})
        if parts == ["api", "archives"] and method == "POST":
            d = self._body()
            return self._send(201, store.create_archive(user, d.get("name", ""), d.get("retention_until", ""), bool(d.get("restricted", True))))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and method == "POST":
            archive_id = int(parts[2])
            if parts[3] == "versions":
                d = self._body()
                return self._send(201, store.ingest_version(user, archive_id, d.get("files")))
            if parts[3] == "members":
                d = self._body()
                return self._send(201, store.grant(user, archive_id, d.get("user_id", ""), d.get("permission", "")))
            if parts[3] == "transfers":
                d = self._body()
                return self._send(201, store.start_transfer(user, archive_id, d.get("target_institution_id", "")))
            if parts[3] == "retention":
                d = self._body()
                return self._send(200, store.update_retention(user, archive_id, d.get("retention_until", "")))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and parts[3] == "status" and method == "GET":
            return self._send(200, store.archive_status(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and parts[3] == "transfers" and method == "GET":
            return self._send(200, store.list_transfers(user, int(parts[2])))
        if len(parts) == 5 and parts[:2] == ["api", "archives"] and parts[3] == "transfers" and method == "GET":
            return self._send(200, store.get_transfer(user, int(parts[4])))
        if len(parts) == 3 and parts[:2] == ["api", "versions"] and method == "GET":
            return self._send(200, store.get_version(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "copies" and method == "POST":
            d = self._body()
            return self._send(201, store.add_copy(user, int(parts[2]), d.get("location", "")))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "migrate" and method == "POST":
            d = self._body()
            return self._send(201, store.migrate(user, int(parts[2]), d.get("source_path", ""), d.get("target_path", ""), d.get("target_format", ""), d.get("content_b64", "")))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "verify" and method == "POST":
            return self._send(200, store.verify_copy(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "simulate-corruption" and method == "POST":
            d = self._body()
            return self._send(200, store.simulate_corruption(user, int(parts[2]), d.get("path", "")))
        if len(parts) == 3 and parts[:2] == ["api", "copies"] and method == "DELETE":
            return self._send(200, store.remove_copy(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "write" and method == "POST":
            d = self._body()
            item_id = d.get("item_id")
            return self._send(200, store.write_transfer(
                user, int(parts[2]),
                int(item_id) if item_id is not None else None,
                d.get("_fail_after"), d.get("_fail_item_id"),
            ))
        if (len(parts) == 6 and parts[:2] == ["api", "transfers"] and parts[3] == "items"
                and parts[5] == "confirm" and method == "POST"):
            return self._send(200, store.confirm_transfer_item(user, int(parts[2]), int(parts[4])))
        raise BusinessError("接口不存在", 404, "not_found")

    def _handle(self, method: str) -> None:
        try:
            self._dispatch(method)
        except BusinessError as exc:
            self._send(exc.status, {"error": {"code": exc.code, "message": exc.message}})
        except (ValueError, TypeError):
            self._send(400, {"error": {"code": "invalid_path", "message": "路径参数格式错误"}})
        except Exception as exc:
            self._send(500, {"error": {"code": "internal_error", "message": str(exc)}})

    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def do_DELETE(self): self._handle("DELETE")
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class PreservationServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, store):
        self.store = store
        super().__init__(address, Handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="数字档案长期保存服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--port", type=int, default=8102)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    store = PreservationStore(args.db)
    store.init_schema()
    if args.seed:
        store.seed()
    if args.init or args.seed:
        print(f"数据库已初始化: {args.db}")
        return
    print(f"数字档案服务运行于 http://127.0.0.1:{args.port}")
    server = PreservationServer(("127.0.0.1", args.port), store)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
