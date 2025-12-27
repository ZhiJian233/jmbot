from __future__ import annotations

import asyncio
import logging
from typing import Any, Tuple, Callable, Awaitable, Optional
from jmcomic import JmAlbumDetail
from jm import JmDownloader
from qq import QQBot
from event_core import Event, EventQueue, EventHandler, DowdnloadFinishedEvent

logger = logging.getLogger(__name__)

class DownloadFinishedEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, bot: QQBot) -> None:
        self.event_queue = event_queue
        self.bot = bot
        super().__init__(event_queue, "DOWNLOAD_FINISHED_EVENT")
        
    async def handle_event(self, event: DowdnloadFinishedEvent) -> None:
        logger.debug(f"DownloadFinishedEventHandler received event.data: {event.data}")
        
        # Ensure event.data is a tuple and contains expected elements
        if not isinstance(event.data, tuple) or len(event.data) != 2:
            logger.error(f"Invalid event.data format for DownloadFinishedEvent: {event.data}")
            await self.bot.send_group_message(f"下载完成事件处理失败：事件数据格式不正确。", str(event.data[1]) if isinstance(event.data, tuple) and len(event.data) > 1 else "未知群组")
            return

        album_detail, group_id = event.data
        
        if album_detail: # Check if album_detail is not None
            logger.debug(f"Album detail is not None. album_detail.authoroname: {album_detail.authoroname}, album_detail.id: {album_detail.id}")
            await self.bot.send_group_message(f"专辑：{album_detail.authoroname} (ID: {album_detail.id})下载完成", str(group_id))
            await self.bot.send_forward_photos(str(group_id), album_detail)
        else:
            logger.warning(f"Album detail is None for DownloadFinishedEvent. group_id: {group_id}")
            await self.bot.send_group_message(f"下载专辑失败，无法获取专辑详情。", str(group_id))

class DownloadRequestEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, bot: QQBot) -> None:
        self.event_queue = event_queue
        self.bot = bot
        super().__init__(event_queue, "DOWNLOAD_REQUEST_EVENT")

    def can_handle(self, event: Event) -> bool:
        message = event.data
        if QQBot.is_group_message(message) and QQBot.is_pure_text_message(message):
            text = QQBot.get_message_text_content(message)
            if text and text.strip().isdigit():
                return True
        return False

    async def handle_event(self, event: Event) -> None:
        message = event.data
        album_id_content = QQBot.get_message_text_content(message)
        album_id = album_id_content.strip() if album_id_content else ""
        group_id = message.group_id
        
        logger.info(f"DownloadRequestEventHandler received request for album_id: {album_id} in group: {group_id}")
        await self.bot.send_group_message(f"开始下载专辑：{album_id}", str(group_id))
        
        album_detail = None # Initialize album_detail
        try: 
            album_detail = await JmDownloader().download_album(album_id)
            logger.debug(f"JmDownloader().download_album({album_id}) returned: {album_detail}")
        except Exception as e:
            logger.error(f"下载专辑 {album_id} 失败：{str(e)}", exc_info=True)
            await self.bot.send_group_message(f"下载专辑 {album_id} 失败：{str(e)}", str(group_id))
            return
        
        # Only create and put the event if album_detail was successfully obtained
        if album_detail:
            event_data = DowdnloadFinishedEvent("DOWNLOAD_FINISHED_EVENT", (album_detail, group_id))
            await self.event_queue.put_event(event_data)
        else:
            logger.warning(f"JmDownloader().download_album({album_id}) returned None. Sending failure message.")
            await self.bot.send_group_message(f"下载专辑 {album_id} 失败：未获取到专辑详情。", str(group_id))
