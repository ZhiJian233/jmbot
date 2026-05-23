import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from api_server import create_api_app
from database import Database
from jmcomic import JmAlbumDetail


def run(coro):
    return asyncio.run(coro)


def make_album(
    album_id: str,
    name: str,
    tags: list[str],
    authors: list[str] | None = None,
) -> JmAlbumDetail:
    return JmAlbumDetail(
        album_id=album_id,
        scramble_id=f"scramble-{album_id}",
        name=name,
        episode_list=[],
        page_count=12,
        pub_date="2026-01-01",
        update_date="2026-01-02",
        likes="10",
        views="100",
        comment_count=3,
        works=[],
        actors=[],
        authors=authors or ["author"],
        tags=tags,
        related_list=[],
        description=f"desc-{album_id}",
    )


def with_db(tmp_path: Path, callback):
    db = Database(str(tmp_path / "test.db"))
    run(db.connect())
    try:
        return callback(db)
    finally:
        run(db.close())


def test_record_download_syncs_tag_relations_and_user(tmp_path: Path):
    def exercise(db: Database):
        album = make_album("1001", "album-1001", ["中文", "巨乳"])
        run(
            db.record_download(
                album_detail=album,
                download_time="2026-05-23T10:00:00",
                downloader_id="u1",
                username="user-1",
            )
        )

        album_info = run(db.get_album_by_id("1001"))
        assert album_info is not None
        assert album_info["tag_list"] == ["中文", "巨乳"]
        assert run(db.get_download_count("u1")) == 1
        assert run(db.get_username("u1")) == "user-1"

    with_db(tmp_path, exercise)


def test_migrate_recovers_album_tags_from_detail_json(tmp_path: Path):
    db_path = tmp_path / "migrate.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE albums (
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
        CREATE TABLE downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            album_id TEXT NOT NULL,
            downloader_id TEXT NOT NULL,
            download_time TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE users (
            user_id TEXT NOT NULL UNIQUE,
            username TEXT,
            PRIMARY KEY (user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_time TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE album_tags (
            album_id TEXT NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (album_id, tag_id)
        )
        """
    )
    detail_json = json.dumps(
        {
            "album_id": "2001",
            "scramble_id": "s-2001",
            "name": "legacy",
            "episode_list": [],
            "page_count": 1,
            "pub_date": "2026-01-01",
            "update_date": "2026-01-01",
            "likes": "1",
            "views": "2",
            "comment_count": 0,
            "works": [],
            "actors": [],
            "authors": ["legacy-author"],
            "tags": ["旧标签", "中文"],
            "related_list": [],
            "description": "legacy-desc",
        },
        ensure_ascii=False,
    )
    conn.execute(
        """
        INSERT INTO albums (id, name, first_download_time, first_downloader_id, detail_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("2001", "legacy", "2026-05-22T10:00:00", "u9", detail_json),
    )
    conn.execute("INSERT INTO tags (name) VALUES (?)", ("孤儿标签",))
    conn.commit()
    conn.close()

    db = Database(str(db_path))
    run(db.connect())
    try:
        tags = run(db.get_tags_for_album("2001"))
        stats = run(db.get_tag_statistics())
    finally:
        run(db.close())

    assert tags == ["中文", "旧标签"]
    assert {item["tag_name"] for item in stats} == {"中文", "旧标签"}


def test_add_and_remove_tag_syncs_detail_json(tmp_path: Path):
    def exercise(db: Database):
        album = make_album("3001", "album-3001", ["原始标签"])
        run(
            db.record_download(
                album_detail=album,
                download_time="2026-05-23T10:00:00",
                downloader_id="u1",
                username="user-1",
            )
        )

        added = run(db.add_tag_to_album("3001", "人工标签"))
        assert added is True
        detail = run(db.get_album_detail_record("3001"))
        assert detail is not None
        assert detail["tags"] == ["人工标签", "原始标签"]

        removed = run(db.remove_tag_from_album("3001", "原始标签"))
        assert removed is True
        detail = run(db.get_album_detail_record("3001"))
        assert detail is not None
        assert detail["tags"] == ["人工标签"]

    with_db(tmp_path, exercise)


def test_api_exposes_album_and_user_queries(tmp_path: Path):
    os.environ["JMBOT_PATH"] = str(tmp_path)
    (tmp_path / "albums" / "4001").mkdir(parents=True, exist_ok=True)
    (tmp_path / "albums" / "4001" / "00001.jpg").write_bytes(b"cover")

    db = Database(str(tmp_path / "api.db"))
    run(db.connect())
    try:
        run(
            db.record_download(
                album_detail=make_album(
                    "4001",
                    "api album",
                    ["中文", "剧情向"],
                    authors=["api-author"],
                ),
                download_time="2026-05-23T10:00:00",
                downloader_id="u-api",
                username="api-user",
            )
        )

        app = create_api_app(
            db,
            {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 8787,
                "cors_origins": ["*"],
                "base_path": "/api/v1",
            },
            tmp_path / "albums",
        )

        async def exercise():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                overview = await client.get("/api/v1/overview")
                assert overview.status_code == 200
                assert overview.json()["stats"]["albums"] == 1

                albums = await client.get("/api/v1/albums", params={"tag": "中文"})
                assert albums.status_code == 200
                assert albums.json()["total"] == 1
                assert (
                    albums.json()["items"][0]["cover_url"]
                    == "/media/albums/4001/00001.jpg"
                )

                detail = await client.get("/api/v1/albums/4001")
                assert detail.status_code == 200
                assert detail.json()["tags"] == ["中文", "剧情向"]
                assert detail.json()["page_files"] == ["00001.jpg"]

                users = await client.get("/api/v1/users")
                assert users.status_code == 200
                assert users.json()["items"][0]["user_id"] == "u-api"

                downloads = await client.get("/api/v1/users/u-api/downloads")
                assert downloads.status_code == 200
                assert downloads.json()["total"] == 1
                assert downloads.json()["items"][0]["album_id"] == "4001"

        run(exercise())
    finally:
        run(db.close())
