import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import uvicorn
import yaml
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from database import Database

logger = logging.getLogger(__name__)


class ApiServer:
    def __init__(self, database: Database, config_path: str = "options/api_option.yml"):
        self.database = database
        self.config = load_api_config(config_path)
        self.media_root = Path(os.environ.get("JMBOT_PATH", ".")).joinpath("albums")
        self.app = create_api_app(self.database, self.config, self.media_root)
        self.server: Optional[uvicorn.Server] = None
        self.server_task: Optional[asyncio.Task] = None

    async def start(self):
        if not self.config["enabled"]:
            logger.info("API 服务已禁用")
            return

        uvicorn_config = uvicorn.Config(
            self.app,
            host=self.config["host"],
            port=self.config["port"],
            log_level="info",
        )
        self.server = uvicorn.Server(uvicorn_config)
        self.server_task = asyncio.create_task(self.server.serve())

        while not self.server.started and not self.server_task.done():
            await asyncio.sleep(0.05)

        logger.info(
            "API 服务已启动: http://%s:%s%s",
            self.config["host"],
            self.config["port"],
            self.config["base_path"],
        )

    async def stop(self):
        if not self.server:
            return

        self.server.should_exit = True
        if self.server_task:
            await self.server_task
        self.server = None
        self.server_task = None


def load_api_config(config_path: str) -> dict[str, Any]:
    default_config = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 8787,
        "cors_origins": ["*"],
        "base_path": "/api/v1",
    }

    path = Path(config_path)
    if not path.exists():
        return default_config

    with path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}

    config = default_config.copy()
    config.update(loaded)
    config["base_path"] = normalize_base_path(config.get("base_path", "/api/v1"))
    config["port"] = int(config["port"])
    config["enabled"] = bool(config["enabled"])
    config["cors_origins"] = list(config.get("cors_origins") or ["*"])
    return config


def create_api_app(database: Database, config: dict[str, Any], media_root: Path) -> Starlette:
    base_path = normalize_base_path(config["base_path"])
    media_mount_path = "/media/albums"

    async def health(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    async def overview(_: Request) -> JSONResponse:
        stats = await database.get_overview_stats()
        return JSONResponse({"stats": stats})

    async def list_albums(request: Request) -> JSONResponse:
        limit = get_int_query(request, "limit", default=20, minimum=1, maximum=100)
        offset = get_int_query(request, "offset", default=0, minimum=0)
        q = optional_query(request, "q")
        tag = optional_query(request, "tag")

        total = await database.count_albums(q=q, tag=tag)
        items = await database.list_albums(limit=limit, offset=offset, q=q, tag=tag)
        serialized = [
            serialize_album_summary(item, media_mount_path=media_mount_path)
            for item in items
        ]
        return JSONResponse(
            {
                "items": serialized,
                "total": total,
                "limit": limit,
                "offset": offset,
                "q": q,
                "tag": tag,
            }
        )

    async def get_album(request: Request) -> JSONResponse:
        album_id = request.path_params["album_id"]
        record = await database.get_album_detail_record(album_id)
        if not record:
            return JSONResponse({"error": "album not found"}, status_code=404)

        return JSONResponse(
            serialize_album_detail(
                record,
                media_root=media_root,
                media_mount_path=media_mount_path,
            )
        )

    async def list_tags(request: Request) -> JSONResponse:
        limit = get_int_query(request, "limit", default=100, minimum=1, maximum=500)
        days = optional_int_query(request, "days", minimum=1)
        stats = await database.get_tag_statistics(days=days)
        return JSONResponse(
            {
                "items": stats[:limit],
                "total": len(stats),
                "limit": limit,
                "days": days,
            }
        )

    async def list_users(request: Request) -> JSONResponse:
        limit = get_int_query(request, "limit", default=20, minimum=1, maximum=100)
        offset = get_int_query(request, "offset", default=0, minimum=0)
        total, items = await database.list_users(limit=limit, offset=offset)
        return JSONResponse(
            {"items": items, "total": total, "limit": limit, "offset": offset}
        )

    async def get_user_downloads(request: Request) -> JSONResponse:
        user_id = request.path_params["user_id"]
        limit = get_int_query(request, "limit", default=20, minimum=1, maximum=100)
        offset = get_int_query(request, "offset", default=0, minimum=0)

        user = await database.get_user_summary(user_id)
        if not user:
            return JSONResponse({"error": "user not found"}, status_code=404)

        total = await database.count_user_downloads(user_id)
        items = await database.get_downloads_by_downloader(
            user_id,
            limit=limit,
            offset=offset,
        )
        return JSONResponse(
            {
                "user": user,
                "items": [
                    serialize_download_record(item, media_mount_path=media_mount_path)
                    for item in items
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        )

    routes = [
        Route(join_path(base_path, "/health"), health),
        Route(join_path(base_path, "/overview"), overview),
        Route(join_path(base_path, "/albums"), list_albums),
        Route(join_path(base_path, "/albums/{album_id:str}"), get_album),
        Route(join_path(base_path, "/tags"), list_tags),
        Route(join_path(base_path, "/users"), list_users),
        Route(join_path(base_path, "/users/{user_id:str}/downloads"), get_user_downloads),
        Mount(
            media_mount_path,
            app=StaticFiles(directory=str(media_root), check_dir=False),
        ),
    ]
    app = Starlette(routes=routes)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config["cors_origins"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.add_exception_handler(ValueError, handle_value_error)
    return app


async def handle_value_error(_: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse({"error": str(exc)}, status_code=400)


def serialize_album_summary(item: dict[str, Any], media_mount_path: str) -> dict[str, Any]:
    cover_path = album_cover_url(item["id"], media_mount_path)
    return {
        "id": item["id"],
        "name": item["name"],
        "tags": item["tags"],
        "authors": item["authors"],
        "page_count": item["page_count"],
        "description": item["description"],
        "first_download_time": item["first_download_time"],
        "first_downloader": {
            "user_id": item["first_downloader_id"],
            "username": item["first_downloader_name"],
        },
        "cover_url": cover_path,
    }


def serialize_album_detail(
    record: dict[str, Any],
    media_root: Path,
    media_mount_path: str,
) -> dict[str, Any]:
    detail = record["detail_dict"]
    page_files = list_album_files(record["id"], media_root)
    return {
        "id": record["id"],
        "name": record["name"],
        "description": detail.get("description", ""),
        "tags": record["tags"],
        "authors": detail.get("authors", []),
        "actors": detail.get("actors", []),
        "works": detail.get("works", []),
        "page_count": detail.get("page_count"),
        "likes": detail.get("likes"),
        "views": detail.get("views"),
        "comment_count": detail.get("comment_count"),
        "pub_date": detail.get("pub_date"),
        "update_date": detail.get("update_date"),
        "episode_list": detail.get("episode_list", []),
        "related_list": detail.get("related_list", []),
        "first_download_time": record["first_download_time"],
        "first_downloader": {
            "user_id": record["first_downloader_id"],
            "username": record["first_downloader_name"],
        },
        "cover_url": album_cover_url(record["id"], media_mount_path),
        "media_base_url": f"{media_mount_path}/{record['id']}",
        "page_files": page_files,
    }


def serialize_download_record(item: dict[str, Any], media_mount_path: str) -> dict[str, Any]:
    detail = parse_detail_json(item.get("detail_json"))
    return {
        "album_id": item["album_id"],
        "album_name": item["album_name"],
        "download_time": item["download_time"],
        "authors": detail.get("authors", []),
        "tags": detail.get("tags", []),
        "cover_url": album_cover_url(item["album_id"], media_mount_path),
    }


def list_album_files(album_id: str, media_root: Path) -> list[str]:
    album_dir = media_root / str(album_id)
    if not album_dir.exists():
        return []

    return sorted(
        [
            path.name
            for path in album_dir.iterdir()
            if path.is_file()
        ]
    )


def album_cover_url(album_id: str, media_mount_path: str) -> Optional[str]:
    album_dir = Path(os.environ.get("JMBOT_PATH", ".")).joinpath("albums", str(album_id))
    cover = album_dir / "00001.jpg"
    if not cover.exists():
        return None
    return f"{media_mount_path}/{album_id}/00001.jpg"


def parse_detail_json(detail_json: Optional[str]) -> dict[str, Any]:
    if not detail_json:
        return {}
    try:
        return json.loads(detail_json)
    except json.JSONDecodeError:
        return {}


def get_int_query(
    request: Request,
    key: str,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw_value = request.query_params.get(key)
    if raw_value is None or raw_value == "":
        value = default
    else:
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise ValueError(f"{key} must be an integer") from exc

    if minimum is not None and value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{key} must be <= {maximum}")
    return value


def optional_int_query(
    request: Request,
    key: str,
    minimum: Optional[int] = None,
) -> Optional[int]:
    raw_value = request.query_params.get(key)
    if raw_value is None or raw_value == "":
        return None
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


def optional_query(request: Request, key: str) -> Optional[str]:
    value = request.query_params.get(key)
    if value is None:
        return None
    value = value.strip()
    return value or None


def normalize_base_path(path: str) -> str:
    path = "/" + path.strip("/")
    return "" if path == "/" else path


def join_path(base: str, suffix: str) -> str:
    if not base:
        return suffix
    return f"{base}{suffix}"
