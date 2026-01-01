import aiosqlite
import asyncio
from pathlib import Path
from typing import Optional
import datetime
import json
from jmcomic import JmAlbumDetail

class Database:
    def __init__(self, db_path: str = 'data/jmbot.db'):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        if self.conn is None:
            self.conn = await aiosqlite.connect(self.db_path)
            await self._create_tables()

    async def close(self):
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def _create_tables(self):
        if not self.conn:
            raise ConnectionError("Database not connected.")

        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                id TEXT NOT NULL UNIQUE,
                name TEXT,
                tags TEXT,
                first_download_time TEXT,
                first_downloader_id TEXT,
                detail_json TEXT,
                PRIMARY KEY (id)
            )
        """)
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                album_id TEXT NOT NULL,
                downloader_id TEXT NOT NULL,
                download_time TEXT,
                FOREIGN KEY (album_id) REFERENCES albums (id)
            )
        """)
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT NOT NULL UNIQUE,
                username TEXT,
                PRIMARY KEY (user_id)
            )
        """)
        await self.conn.commit()

    async def add_album(self, album_detail: JmAlbumDetail, download_time: str, downloader_id: str):
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        album_id = album_detail.id
        name = album_detail.name
        tags = ','.join(album_detail.tags) if album_detail.tags else ''
        # Manually create a dictionary from JmAlbumDetail attributes for serialization
        album_detail_dict = {
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
        detail_json = json.dumps(album_detail_dict) # Serialize JmAlbumDetail to JSON string

        # 检查专辑是否已存在
        cursor = await self.conn.execute("SELECT id FROM albums WHERE id = ?", (album_id,))
        existing_album = await cursor.fetchone()
        await cursor.close()

        if not existing_album:
            await self.conn.execute(
                "INSERT INTO albums (id, name, tags, first_download_time, first_downloader_id, detail_json) VALUES (?, ?, ?, ?, ?, ?)",
                (album_id, name, tags, download_time, downloader_id, detail_json)
            )
            await self.conn.commit()
            return True
        return False # 专辑已存在

    async def add_download(self, album_id: str, downloader_id: str, download_time: str):
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        await self.conn.execute(
            "INSERT INTO downloads (album_id, downloader_id, download_time) VALUES (?, ?, ?)",
            (album_id, downloader_id, download_time)
        )
        await self.conn.commit()

    async def get_download_count(self, downloader_id: str) -> int:
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM downloads WHERE downloader_id = ?", (downloader_id,)
        )
        result = await cursor.fetchone()
        await cursor.close()
        return result[0] if result else 0

    async def is_album_exist(self, album_id: str) -> bool:
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        cursor = await self.conn.execute("SELECT id FROM albums WHERE id = ?", (album_id,))
        result = await cursor.fetchone()
        await cursor.close()
        return result is not None

    async def get_all_albums(self) -> list[dict]:
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        cursor = await self.conn.execute("SELECT id, name, tags, first_download_time, first_downloader_id FROM albums")
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            {"id": row[0], "name": row[1], "tags": row[2], "first_download_time": row[3], "first_downloader_id": row[4]}
            for row in rows
        ]

    async def get_album_by_id(self, album_id: str) -> Optional[dict]:
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        cursor = await self.conn.execute(
            "SELECT id, name, tags, first_download_time, first_downloader_id FROM albums WHERE id = ?", (album_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            return {"id": row[0], "name": row[1], "tags": row[2], "first_download_time": row[3], "first_downloader_id": row[4]}
        return None

    async def get_album_detail_by_id(self, album_id: str) -> Optional[JmAlbumDetail]:
        if not self.conn:
            raise ConnectionError("Database not connected.")
        
        cursor = await self.conn.execute(
            "SELECT detail_json FROM albums WHERE id = ?", (album_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row and row[0]:
            detail_dict = json.loads(row[0])
            # Reconstruct JmAlbumDetail object from dictionary
            return JmAlbumDetail(
                album_id=detail_dict.get('album_id'),
                scramble_id=detail_dict.get('scramble_id'),
                name=detail_dict.get('name'),
                episode_list=detail_dict.get('episode_list', []),
                page_count=detail_dict.get('page_count'),
                pub_date=detail_dict.get('pub_date'),
                update_date=detail_dict.get('update_date'),
                likes=detail_dict.get('likes'),
                views=detail_dict.get('views'),
                comment_count=detail_dict.get('comment_count'),
                works=detail_dict.get('works', []),
                actors=detail_dict.get('actors', []),
                authors=detail_dict.get('authors', []),
                tags=detail_dict.get('tags', []),
                related_list=detail_dict.get('related_list', []),
                description=detail_dict.get('description', '')
            )
        return None

    async def get_downloads_by_downloader(self, downloader_id: str) -> list[dict]:
        if not self.conn:
            raise ConnectionError("Database not connected.")

        cursor = await self.conn.execute(
            "SELECT album_id, download_time FROM downloads WHERE downloader_id = ?", (downloader_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            {"album_id": row[0], "download_time": row[1]}
            for row in rows
        ]

    async def add_user(self, user_id: str, username: str):
        if not self.conn:
            raise ConnectionError("Database not connected.")

        await self.conn.execute(
            "INSERT OR REPLACE INTO users (user_id, username) VALUES (?, ?)",
            (user_id, username)
        )
        await self.conn.commit()

    async def get_username(self, user_id: str) -> Optional[str]:
        if not self.conn:
            raise ConnectionError("Database not connected.")

        cursor = await self.conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        await cursor.close()
        return result[0] if result else None


# 示例用法 (仅用于测试，实际应用中应通过其他模块调用)
async def main():
    db = Database()
    await db.connect()

    # Create mock JmAlbumDetail objects for testing
    mock_album_detail_1 = JmAlbumDetail(
        album_id='12345',
        name='Test Album 1',
        tags=['tagA', 'tagB'],
        authors=['Author 1'],
        page_count=10,
        episode_list=[], # Assuming empty for mock
        pub_date='2023-01-01',
        update_date='2023-01-01',
        likes='100', # Likes and views are strings in JmAlbumDetail
        views='1000',
        comment_count=10,
        works=[], actors=[],
        scramble_id='scramble1',
        description='Description 1'
    )
    mock_album_detail_2 = JmAlbumDetail(
        album_id='67890',
        name='Test Album 2',
        tags=['tagC'],
        authors=['Author 2'],
        page_count=20,
        episode_list=[], # Assuming empty for mock
        pub_date='2023-02-01',
        update_date='2023-02-01',
        likes='200',
        views='2000',
        comment_count=20,
        works=[], actors=[],
        scramble_id='scramble2',
        description='Description 2'
    )

    print("Adding album '12345'...")
    await db.add_album(mock_album_detail_1, datetime.datetime.now().isoformat(), 'user1')
    print("Adding album '67890'...")
    await db.add_album(mock_album_detail_2, datetime.datetime.now().isoformat(), 'user2')

    print("Adding downloads...")
    await db.add_download('12345', 'user1', datetime.datetime.now().isoformat())
    await db.add_download('12345', 'user3', datetime.datetime.now().isoformat())
    await db.add_download('67890', 'user1', datetime.datetime.now().isoformat())
    await db.add_download('67890', 'user2', datetime.datetime.now().isoformat())

    print(f"User1 download count: {await db.get_download_count('user1')}")
    print(f"User2 download count: {await db.get_download_count('user2')}")
    print(f"User3 download count: {await db.get_download_count('user3')}")

    print("\nAll albums:")
    for album in await db.get_all_albums():
        print(album)

    print("\nDownloads by user1:")
    for download in await db.get_downloads_by_downloader('user1'):
        print(download)

    await db.close()

if __name__ == '__main__':
    asyncio.run(main())
