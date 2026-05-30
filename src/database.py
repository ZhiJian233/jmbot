import datetime
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from jmcomic import JmAlbumDetail


class Database:
    def __init__(self, db_path: str = "data/jmbot.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    async def connect(self):
        with self._lock:
            if self.conn is None:
                self._connect_sync()

    async def close(self):
        with self._lock:
            if self.conn is not None:
                conn = self.conn
                self.conn = None
                conn.close()

    async def add_album(
        self,
        album_detail: JmAlbumDetail,
        download_time: str,
        downloader_id: str,
    ) -> bool:
        album_id = str(album_detail.id)
        if await self.is_album_exist(album_id):
            return False
        await self._run(self._add_album_sync, album_detail, download_time, downloader_id)
        return True

    async def add_download(self, album_id: str, downloader_id: str, download_time: str):
        await self._run(self._add_download_sync, album_id, downloader_id, download_time)

    async def add_user(self, user_id: str, username: str):
        await self._run(self._add_user_sync, user_id, username)

    async def record_download(
        self,
        album_detail: JmAlbumDetail,
        download_time: str,
        downloader_id: str,
        username: str,
    ) -> dict[str, Any]:
        return await self._run(
            self._record_download_sync,
            album_detail,
            download_time,
            downloader_id,
            username,
        )

    async def get_download_count(self, downloader_id: str) -> int:
        return await self._run(self._get_download_count_sync, downloader_id)

    async def is_album_exist(self, album_id: str) -> bool:
        return await self._run(self._is_album_exist_sync, album_id)

    async def get_all_albums(self) -> list[dict]:
        return await self.list_albums(limit=1000, offset=0)

    async def get_album_by_id(self, album_id: str) -> Optional[dict]:
        return await self._run(self._get_album_by_id_sync, album_id)

    async def get_album_detail_by_id(self, album_id: str) -> Optional[JmAlbumDetail]:
        detail = await self.get_album_detail_record(album_id)
        if not detail:
            return None
        return detail["album_detail"]

    async def get_album_detail_record(self, album_id: str) -> Optional[dict[str, Any]]:
        return await self._run(self._get_album_detail_record_sync, album_id)

    async def get_downloads_by_downloader(
        self,
        downloader_id: str,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> list[dict]:
        return await self._run(
            self._get_downloads_by_downloader_sync,
            downloader_id,
            limit,
            offset,
        )

    async def get_username(self, user_id: str) -> Optional[str]:
        return await self._run(self._get_username_sync, user_id)

    async def get_albums_by_tag(self, tag_name: str) -> list[dict]:
        return await self.list_albums(limit=1000, offset=0, tag=tag_name)

    async def query_albums(
        self,
        query_type: Literal["tag", "title", "author"],
        keyword: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return await self._run(self._query_albums_sync, query_type, keyword, limit, offset)

    async def add_tag_to_album(self, album_id: str, tag_name: str) -> bool:
        return await self._run(self._add_tag_to_album_sync, album_id, tag_name)

    async def remove_tag_from_album(self, album_id: str, tag_name: str) -> bool:
        return await self._run(self._remove_tag_from_album_sync, album_id, tag_name)

    async def get_tags_for_album(self, album_id: str) -> list[str]:
        return await self._run(self._get_tags_for_album_sync, album_id)

    async def get_tag_statistics(self, days: Optional[int] = None) -> list[dict]:
        return await self._run(self._get_tag_statistics_sync, days)

    async def get_overview_stats(self) -> dict[str, int]:
        return await self._run(self._get_overview_stats_sync)

    async def count_albums(self, q: Optional[str] = None, tag: Optional[str] = None) -> int:
        return await self._run(self._count_albums_sync, q, tag, None, None)

    async def list_albums(
        self,
        limit: int,
        offset: int,
        q: Optional[str] = None,
        tag: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        return await self._run(self._list_albums_sync, limit, offset, q, tag, title, author)

    async def list_users(self, limit: int, offset: int) -> tuple[int, list[dict[str, Any]]]:
        return await self._run(self._list_users_sync, limit, offset)

    async def get_user_summary(self, user_id: str) -> Optional[dict[str, Any]]:
        return await self._run(self._get_user_summary_sync, user_id)

    async def count_user_downloads(self, user_id: str) -> int:
        return await self._run(self._count_user_downloads_sync, user_id)

    async def _run(self, func: Callable[..., Any], *args):
        with self._lock:
            return func(*args)

    def _connect_sync(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_tables_sync()
        self._migrate_data_sync()

    def _require_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            raise ConnectionError("Database not connected.")
        return self.conn

    def _create_tables_sync(self):
        conn = self._require_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS albums (
                id TEXT NOT NULL UNIQUE,
                name TEXT,
                tags TEXT,
                first_download_time TEXT,
                first_downloader_id TEXT,
                detail_json TEXT,
                PRIMARY KEY (id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                album_id TEXT NOT NULL,
                downloader_id TEXT NOT NULL,
                download_time TEXT,
                FOREIGN KEY (album_id) REFERENCES albums (id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT NOT NULL UNIQUE,
                username TEXT,
                PRIMARY KEY (user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_time TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS album_tags (
                album_id TEXT NOT NULL,
                tag_id INTEGER NOT NULL,
                FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE,
                PRIMARY KEY (album_id, tag_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_album_tags_tag_id ON album_tags(tag_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloads_downloader_id ON downloads(downloader_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloads_album_id ON downloads(album_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_albums_first_download_time ON albums(first_download_time)"
        )
        conn.commit()

    def _migrate_data_sync(self):
        conn = self._require_conn()
        rows = conn.execute("SELECT id, detail_json FROM albums").fetchall()

        pending_tags: set[str] = set()
        pending_relations: set[tuple[str, str]] = set()
        for row in rows:
            detail_json = row["detail_json"]
            if not detail_json:
                continue
            try:
                detail = json.loads(detail_json)
            except json.JSONDecodeError:
                continue

            for tag_name in self._normalize_tags(detail.get("tags") or []):
                pending_tags.add(tag_name)
                pending_relations.add((row["id"], tag_name))

        if pending_tags:
            conn.executemany(
                "INSERT OR IGNORE INTO tags (name) VALUES (?)",
                [(tag_name,) for tag_name in sorted(pending_tags)],
            )

        if pending_relations:
            tag_rows = conn.execute("SELECT id, name FROM tags").fetchall()
            tag_map = {row["name"]: row["id"] for row in tag_rows}
            conn.executemany(
                "INSERT OR IGNORE INTO album_tags (album_id, tag_id) VALUES (?, ?)",
                [
                    (album_id, tag_map[tag_name])
                    for album_id, tag_name in sorted(pending_relations)
                    if tag_name in tag_map
                ],
            )

        conn.execute(
            """
            DELETE FROM tags
            WHERE id IN (
                SELECT t.id
                FROM tags t
                LEFT JOIN album_tags at ON at.tag_id = t.id
                WHERE at.tag_id IS NULL
            )
            """
        )
        conn.execute("DROP INDEX IF EXISTS idx_tags_name")
        conn.commit()

    def _record_download_sync(
        self,
        album_detail: JmAlbumDetail,
        download_time: str,
        downloader_id: str,
        username: str,
    ) -> dict[str, Any]:
        conn = self._require_conn()
        album_id = str(album_detail.id)
        existing_album = self._is_album_exist_sync(album_id)

        try:
            conn.execute("BEGIN")
            self._upsert_album_sync(album_detail, download_time, downloader_id)
            self._add_download_sync(album_id, downloader_id, download_time, commit=False)
            self._add_user_sync(downloader_id, username, commit=False)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return {"album_created": not existing_album, "album_id": album_id}

    def _add_album_sync(
        self,
        album_detail: JmAlbumDetail,
        download_time: str,
        downloader_id: str,
    ):
        conn = self._require_conn()
        try:
            conn.execute("BEGIN")
            self._upsert_album_sync(album_detail, download_time, downloader_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _add_download_sync(
        self,
        album_id: str,
        downloader_id: str,
        download_time: str,
        commit: bool = True,
    ):
        conn = self._require_conn()
        conn.execute(
            "INSERT INTO downloads (album_id, downloader_id, download_time) VALUES (?, ?, ?)",
            (album_id, downloader_id, download_time),
        )
        if commit:
            conn.commit()

    def _add_user_sync(self, user_id: str, username: str, commit: bool = True):
        conn = self._require_conn()
        conn.execute(
            """
            INSERT INTO users (user_id, username) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
            """,
            (user_id, username),
        )
        if commit:
            conn.commit()

    def _get_download_count_sync(self, downloader_id: str) -> int:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM downloads WHERE downloader_id = ?",
            (downloader_id,),
        ).fetchone()
        return int(row["count"]) if row else 0

    def _is_album_exist_sync(self, album_id: str) -> bool:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT 1 FROM albums WHERE id = ?",
            (album_id,),
        ).fetchone()
        return row is not None

    def _get_album_by_id_sync(self, album_id: str) -> Optional[dict]:
        conn = self._require_conn()
        row = conn.execute(
            """
            SELECT
                a.id,
                a.name,
                a.first_download_time,
                a.first_downloader_id,
                u.username AS first_downloader_name
            FROM albums a
            LEFT JOIN users u ON u.user_id = a.first_downloader_id
            WHERE a.id = ?
            """,
            (album_id,),
        ).fetchone()
        if not row:
            return None

        tags = self._get_tags_for_album_sync(album_id)
        return {
            "id": row["id"],
            "name": row["name"],
            "tags": ",".join(tags),
            "tag_list": tags,
            "first_download_time": row["first_download_time"],
            "first_downloader_id": row["first_downloader_id"],
            "first_downloader_name": row["first_downloader_name"],
        }

    def _get_album_detail_record_sync(self, album_id: str) -> Optional[dict[str, Any]]:
        conn = self._require_conn()
        row = conn.execute(
            """
            SELECT
                a.id,
                a.name,
                a.first_download_time,
                a.first_downloader_id,
                a.detail_json,
                u.username AS first_downloader_name
            FROM albums a
            LEFT JOIN users u ON u.user_id = a.first_downloader_id
            WHERE a.id = ?
            """,
            (album_id,),
        ).fetchone()
        if not row or not row["detail_json"]:
            return None

        detail_dict = json.loads(row["detail_json"])
        tags = self._get_tags_for_album_sync(album_id)
        return {
            "id": row["id"],
            "name": row["name"],
            "first_download_time": row["first_download_time"],
            "first_downloader_id": row["first_downloader_id"],
            "first_downloader_name": row["first_downloader_name"],
            "tags": tags,
            "detail_dict": detail_dict,
            "album_detail": self._build_album_detail(detail_dict, tags),
        }

    def _get_downloads_by_downloader_sync(
        self,
        downloader_id: str,
        limit: Optional[int],
        offset: int,
    ) -> list[dict]:
        conn = self._require_conn()
        sql = """
            SELECT
                d.album_id,
                d.download_time,
                a.name AS album_name,
                a.detail_json
            FROM downloads d
            LEFT JOIN albums a ON a.id = d.album_id
            WHERE d.downloader_id = ?
            ORDER BY d.download_time DESC, d.id DESC
        """
        params: list[Any] = [downloader_id]
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        rows = conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "album_id": row["album_id"],
                "album_name": row["album_name"],
                "download_time": row["download_time"],
                "detail_json": row["detail_json"],
            }
            for row in rows
        ]

    def _get_username_sync(self, user_id: str) -> Optional[str]:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT username FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["username"] if row else None

    def _add_tag_to_album_sync(self, album_id: str, tag_name: str) -> bool:
        conn = self._require_conn()
        if not self._is_album_exist_sync(album_id):
            return False

        try:
            conn.execute("BEGIN")
            tag_id = self._ensure_tag_sync(tag_name)
            cursor = conn.execute(
                "INSERT OR IGNORE INTO album_tags (album_id, tag_id) VALUES (?, ?)",
                (album_id, tag_id),
            )
            changed = cursor.rowcount > 0
            self._sync_album_detail_tags_sync(album_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return changed

    def _remove_tag_from_album_sync(self, album_id: str, tag_name: str) -> bool:
        conn = self._require_conn()
        tag_row = conn.execute(
            "SELECT id FROM tags WHERE name = ?",
            (tag_name,),
        ).fetchone()
        if not tag_row:
            return False

        try:
            conn.execute("BEGIN")
            cursor = conn.execute(
                "DELETE FROM album_tags WHERE album_id = ? AND tag_id = ?",
                (album_id, tag_row["id"]),
            )
            changed = cursor.rowcount > 0
            conn.execute(
                """
                DELETE FROM tags
                WHERE id = ?
                AND NOT EXISTS (
                    SELECT 1 FROM album_tags WHERE tag_id = ?
                )
                """,
                (tag_row["id"], tag_row["id"]),
            )
            self._sync_album_detail_tags_sync(album_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return changed

    def _get_tags_for_album_sync(self, album_id: str) -> list[str]:
        conn = self._require_conn()
        rows = conn.execute(
            """
            SELECT t.name
            FROM tags t
            JOIN album_tags at ON t.id = at.tag_id
            WHERE at.album_id = ?
            ORDER BY t.name
            """,
            (album_id,),
        ).fetchall()
        return [row["name"] for row in rows]

    def _get_tag_statistics_sync(self, days: Optional[int]) -> list[dict]:
        conn = self._require_conn()
        params: tuple[Any, ...] = ()
        sql = """
            SELECT t.name, COUNT(at.album_id) AS count
            FROM tags t
            LEFT JOIN album_tags at ON t.id = at.tag_id
            LEFT JOIN albums a ON at.album_id = a.id
        """
        if days is not None:
            sql += " WHERE a.first_download_time >= ?"
            params = (self._threshold_iso(days),)
        sql += """
            GROUP BY t.id, t.name
            ORDER BY count DESC, t.name ASC
        """
        rows = conn.execute(sql, params).fetchall()
        return [{"tag_name": row["name"], "count": row["count"]} for row in rows]

    def _get_overview_stats_sync(self) -> dict[str, int]:
        conn = self._require_conn()
        tables = ("albums", "downloads", "users", "tags", "album_tags")
        result: dict[str, int] = {}
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
            result[table] = int(row["count"]) if row else 0
        return result

    def _query_albums_sync(
        self,
        query_type: Literal["tag", "title", "author"],
        keyword: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        normalized = keyword.strip()
        if not normalized:
            return []

        if query_type == "tag":
            return self._list_albums_sync(limit, offset, None, normalized, None, None)
        if query_type == "title":
            return self._list_albums_sync(limit, offset, None, None, normalized, None)
        if query_type == "author":
            return self._list_albums_sync(limit, offset, None, None, None, normalized)
        raise ValueError(f"unsupported query type: {query_type}")

    def _count_albums_sync(
        self,
        q: Optional[str],
        tag: Optional[str],
        title: Optional[str],
        author: Optional[str],
    ) -> int:
        conn = self._require_conn()
        sql, params = self._album_filter_sql(
            base_sql="SELECT COUNT(DISTINCT a.id) AS count FROM albums a",
            q=q,
            tag=tag,
            title=title,
            author=author,
        )
        row = conn.execute(sql, params).fetchone()
        return int(row["count"]) if row else 0

    def _list_albums_sync(
        self,
        limit: int,
        offset: int,
        q: Optional[str],
        tag: Optional[str],
        title: Optional[str],
        author: Optional[str],
    ) -> list[dict[str, Any]]:
        conn = self._require_conn()
        sql, params = self._album_filter_sql(
            base_sql="""
                SELECT
                    a.id,
                    a.name,
                    a.first_download_time,
                    a.first_downloader_id,
                    a.detail_json,
                    u.username AS first_downloader_name,
                    GROUP_CONCAT(t.name, '||') AS joined_tags
                FROM albums a
                LEFT JOIN users u ON u.user_id = a.first_downloader_id
                LEFT JOIN album_tags at ON at.album_id = a.id
                LEFT JOIN tags t ON t.id = at.tag_id
            """,
            q=q,
            tag=tag,
            title=title,
            author=author,
            suffix="""
                GROUP BY a.id, a.name, a.first_download_time, a.first_downloader_id, a.detail_json, u.username
                ORDER BY a.first_download_time DESC, a.id DESC
                LIMIT ? OFFSET ?
            """,
        )
        rows = conn.execute(sql, (*params, limit, offset)).fetchall()
        return [self._album_summary_from_row(row) for row in rows]

    def _list_users_sync(self, limit: int, offset: int) -> tuple[int, list[dict[str, Any]]]:
        conn = self._require_conn()
        total_row = conn.execute("SELECT COUNT(*) AS count FROM users").fetchone()
        total = int(total_row["count"]) if total_row else 0
        rows = conn.execute(
            """
            SELECT
                u.user_id,
                u.username,
                COUNT(d.id) AS download_count,
                MAX(d.download_time) AS last_download_time
            FROM users u
            LEFT JOIN downloads d ON d.downloader_id = u.user_id
            GROUP BY u.user_id, u.username
            ORDER BY download_count DESC, u.user_id ASC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        return total, [
            {
                "user_id": row["user_id"],
                "username": row["username"],
                "download_count": int(row["download_count"]),
                "last_download_time": row["last_download_time"],
            }
            for row in rows
        ]

    def _get_user_summary_sync(self, user_id: str) -> Optional[dict[str, Any]]:
        conn = self._require_conn()
        row = conn.execute(
            """
            SELECT
                u.user_id,
                u.username,
                COUNT(d.id) AS download_count,
                MAX(d.download_time) AS last_download_time
            FROM users u
            LEFT JOIN downloads d ON d.downloader_id = u.user_id
            WHERE u.user_id = ?
            GROUP BY u.user_id, u.username
            """,
            (user_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "user_id": row["user_id"],
            "username": row["username"],
            "download_count": int(row["download_count"]),
            "last_download_time": row["last_download_time"],
        }

    def _count_user_downloads_sync(self, user_id: str) -> int:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM downloads WHERE downloader_id = ?",
            (user_id,),
        ).fetchone()
        return int(row["count"]) if row else 0

    def _upsert_album_sync(
        self,
        album_detail: JmAlbumDetail,
        download_time: str,
        downloader_id: str,
    ):
        conn = self._require_conn()
        album_id = str(album_detail.id)
        detail_json = json.dumps(
            self._album_detail_to_dict(album_detail),
            ensure_ascii=False,
        )
        conn.execute(
            """
            INSERT INTO albums (id, name, first_download_time, first_downloader_id, detail_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                detail_json = excluded.detail_json
            """,
            (album_id, album_detail.name, download_time, downloader_id, detail_json),
        )
        self._merge_album_tags_sync(album_id, album_detail.tags or [])
        self._sync_album_detail_tags_sync(album_id)

    def _merge_album_tags_sync(self, album_id: str, raw_tags: list[Any]):
        conn = self._require_conn()
        for tag_name in self._normalize_tags(raw_tags):
            tag_id = self._ensure_tag_sync(tag_name)
            conn.execute(
                "INSERT OR IGNORE INTO album_tags (album_id, tag_id) VALUES (?, ?)",
                (album_id, tag_id),
            )
        conn.execute(
            """
            DELETE FROM tags
            WHERE id IN (
                SELECT t.id
                FROM tags t
                LEFT JOIN album_tags at ON at.tag_id = t.id
                WHERE at.tag_id IS NULL
            )
            """
        )

    def _ensure_tag_sync(self, tag_name: str) -> int:
        conn = self._require_conn()
        normalized = tag_name.strip()
        conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (normalized,))
        row = conn.execute(
            "SELECT id FROM tags WHERE name = ?",
            (normalized,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"tag creation failed: {normalized}")
        return int(row["id"])

    def _sync_album_detail_tags_sync(self, album_id: str):
        conn = self._require_conn()
        row = conn.execute(
            "SELECT detail_json FROM albums WHERE id = ?",
            (album_id,),
        ).fetchone()
        if not row or not row["detail_json"]:
            return
        try:
            detail = json.loads(row["detail_json"])
        except json.JSONDecodeError:
            return
        detail["tags"] = self._get_tags_for_album_sync(album_id)
        conn.execute(
            "UPDATE albums SET detail_json = ? WHERE id = ?",
            (json.dumps(detail, ensure_ascii=False), album_id),
        )

    def _album_filter_sql(
        self,
        base_sql: str,
        q: Optional[str] = None,
        tag: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        suffix: str = "",
    ) -> tuple[str, tuple[Any, ...]]:
        joins = []
        conditions = []
        params: list[Any] = []

        if tag:
            joins.extend(
                [
                    "JOIN album_tags filter_at ON filter_at.album_id = a.id",
                    "JOIN tags filter_t ON filter_t.id = filter_at.tag_id",
                ]
            )
            conditions.append("filter_t.name = ?")
            params.append(tag)

        if q:
            keyword = f"%{q}%"
            conditions.append("(a.id LIKE ? OR a.name LIKE ?)")
            params.extend([keyword, keyword])

        if title:
            conditions.append("a.name LIKE ?")
            params.append(f"%{title}%")

        if author:
            conditions.append(
                """
                (
                    json_valid(a.detail_json)
                    AND EXISTS (
                        SELECT 1
                        FROM json_each(a.detail_json, '$.authors') AS author_item
                        WHERE author_item.value LIKE ?
                    )
                )
                """
            )
            params.append(f"%{author}%")

        sql = base_sql
        if joins:
            sql += "\n" + "\n".join(joins)
        if conditions:
            sql += "\nWHERE " + " AND ".join(conditions)
        if suffix:
            sql += "\n" + suffix.strip()
        return sql, tuple(params)

    def _album_summary_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        detail_dict = self._parse_detail_json(row["detail_json"])
        tags = sorted({item for item in (row["joined_tags"] or "").split("||") if item})
        return {
            "id": row["id"],
            "name": row["name"],
            "first_download_time": row["first_download_time"],
            "first_downloader_id": row["first_downloader_id"],
            "first_downloader_name": row["first_downloader_name"],
            "tags": tags,
            "page_count": detail_dict.get("page_count"),
            "authors": detail_dict.get("authors", []),
            "description": detail_dict.get("description", ""),
            "detail_json": row["detail_json"],
        }

    @staticmethod
    def _album_detail_to_dict(album_detail: JmAlbumDetail) -> dict[str, Any]:
        return {
            "album_id": album_detail.album_id,
            "scramble_id": album_detail.scramble_id,
            "name": album_detail.name,
            "episode_list": album_detail.episode_list,
            "page_count": album_detail.page_count,
            "pub_date": album_detail.pub_date,
            "update_date": album_detail.update_date,
            "likes": album_detail.likes,
            "views": album_detail.views,
            "comment_count": album_detail.comment_count,
            "works": album_detail.works,
            "actors": album_detail.actors,
            "authors": album_detail.authors,
            "tags": album_detail.tags,
            "related_list": album_detail.related_list,
            "description": album_detail.description,
        }

    @staticmethod
    def _parse_detail_json(detail_json: Optional[str]) -> dict[str, Any]:
        if not detail_json:
            return {}
        try:
            return json.loads(detail_json)
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _normalize_tags(raw_tags: list[Any]) -> list[str]:
        seen = set()
        tags = []
        for item in raw_tags:
            if item is None:
                continue
            tag = str(item).strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            tags.append(tag)
        return tags

    @staticmethod
    def _build_album_detail(detail_dict: dict[str, Any], tags: list[str]) -> JmAlbumDetail:
        return JmAlbumDetail(
            album_id=detail_dict.get("album_id"),
            scramble_id=detail_dict.get("scramble_id"),
            name=detail_dict.get("name"),
            episode_list=detail_dict.get("episode_list", []),
            page_count=detail_dict.get("page_count"),
            pub_date=detail_dict.get("pub_date"),
            update_date=detail_dict.get("update_date"),
            likes=detail_dict.get("likes"),
            views=detail_dict.get("views"),
            comment_count=detail_dict.get("comment_count"),
            works=detail_dict.get("works", []),
            actors=detail_dict.get("actors", []),
            authors=detail_dict.get("authors", []),
            tags=tags,
            related_list=detail_dict.get("related_list", []),
            description=detail_dict.get("description", ""),
        )

    @staticmethod
    def _threshold_iso(days: int) -> str:
        return (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
