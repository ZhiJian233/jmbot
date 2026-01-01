from __future__ import annotations

import asyncio
import datetime
from email import message
import logging
from tkinter import E
from tokenize import group
from turtle import down
from typing import Any, Tuple, Callable, Awaitable, Optional
from jmcomic import JmAlbumDetail
import database
from jm import JmDownloader
from qq import GroupMessage, QQBot, Message
from event_core import Event, EventQueue, EventHandler, DownloadFinishedEvent
from database import Database

logger = logging.getLogger(__name__)

class MessageEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, bot: QQBot ) -> None:
        self.event_queue = event_queue
        self.bot = bot
        super().__init__(event_queue, "MESSAGE_EVENT")

    async def handle_event(self, event: Event) -> None:
        try:
            message = QQBot.parse_message(event.data)
        except Exception as e:
            logger.error(f"非消息事件: {e}")
            return
        if QQBot.is_group_message(message):
            await self.event_queue.put_event(Event("GROUP_MESSAGE_EVENT", message))
        elif QQBot.is_private_message(message):
            await self.event_queue.put_event(Event("PRIVATE_MESSAGE_EVENT", message))


class DownloadFinishedEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, bot: QQBot, database: Database) -> None:
        self.event_queue = event_queue
        self.bot = bot
        self.database = database
        super().__init__(event_queue, "DOWNLOAD_FINISHED_EVENT")
        
    async def handle_event(self, event: DownloadFinishedEvent) -> None:
        logger.debug(f"DownloadFinishedEventHandler received event.data: {event.data}")
        
        # Ensure event.data is a tuple and contains expected elements
        if not isinstance(event.data, Tuple) or len(event.data) != 2:
            logger.error(f"Invalid event.data format for DownloadFinishedEvent: {event.data}")
            await self.bot.send_group_message(f"下载完成事件处理失败：事件数据格式不正确。", str(event.data[1]) if isinstance(event.data, tuple) and len(event.data) > 1 else "未知群组")
            return

        album_detail, message = event.data
        group_id = message.group_id
        if album_detail: # Check if album_detail is not None
            logger.debug(f"Album detail is not None. album_detail.authoroname: {album_detail.authoroname}, album_detail.id: {album_detail.id}")
            await self.bot.send_group_message(f"专辑：{album_detail.authoroname} (ID: {album_detail.id})下载完成", str(group_id))
            await self.bot.send_forward_photos(str(group_id), album_detail)
        else:
            album = await self.database.get_album_detail_by_id(str(group_id))
            if album:
                logger.debug(f"Album detail is None. Fetched album from database: {album.name}, id: {album.id}")
                await self.bot.send_group_message(f"专辑：{album.name} (ID: {album.id})下载完成", str(group_id))
                await self.bot.send_forward_photos(str(group_id), album)
                # Here we would need to reconstruct a JmAlbumDetail or similar object to send photos
                # This part is left as a placeholder since we don't have the full context
                # await self.bot.send_forward_photos(str(group_id), reconstructed_album_detail)


class DownloadRequestEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, bot: QQBot, database: Database) -> None:
        self.event_queue = event_queue
        self.bot = bot
        self.database = database
        super().__init__(event_queue, "GROUP_MESSAGE_EVENT")

    def can_handle(self, event: Event) -> bool:
        message: GroupMessage = event.data
        if QQBot.is_pure_text_message(message):
            text = QQBot.get_message_text_content(message)
            if text and text.strip().isdigit():
                return True
        return False

    async def handle_event(self, event: Event) -> None:
        message: GroupMessage = event.data
        album_id_content = QQBot.get_message_text_content(message)
        album_id = album_id_content.strip() if album_id_content else ""
        group_id = message.group_id
        downloader = JmDownloader()

        logger.info(f"DownloadRequestEventHandler received request for album_id: {album_id} in group: {group_id}")
        await self.bot.send_group_message(f"开始下载专辑：{album_id}", str(group_id))

        #获得本子详情
        # if not await self.database.is_album_exist(album_id):
        #     logger.info(f"专辑 {album_id} 不在数据库中，准备获取详情。")
        #     await self.bot.send_group_message(f"专辑 {album_id} 不在数据库中，准备获取详情。", str(group_id))
        #     album_detail = await downloader.get_album_detail(album_id)
        #     logger.debug(f"JmDownloader().get_album_detail({album_id}) returned: {album_detail}")
        #     if album_detail:
        #         logger.info(f"专辑 {album_id} 详情获取成功: {album_detail.name}")
        #         #发送记录下载事件
        #         await self.event_queue.put_event(Event("RECORD_DOWNLOAD_EVENT", (album_detail, message.sender)))
        #         await self.bot.send_group_message(f"{album_detail.name}/n共{album_detail.page_count}页/ntag：{album_detail.tags}", str(group_id))
        #     else:
        #         logger.warning(f"专辑 {album_id} 详情获取失败或不存在.")
        #         await self.bot.send_group_message(f"专辑 {album_id} 不存在或无法获取详情。", str(group_id))
        #         return
        # else:
        #     logger.info(f"专辑 {album_id} 已存在于数据库中，跳过获取详情步骤。")
        #     await self.bot.send_group_message(f"专辑 {album_id} 已存在于数据库中。", str(group_id))
        #     album_detail = await self.database.get_album_detail_by_id(album_id)
        try:
            album_detail = await self.database.get_album_detail_by_id(album_id)
        except ConnectionError as e:
            logger.error(f"数据库连接中断: {e}")
            await self.bot.send_group_message(f"数据库连接中断!", str(group_id))
            album_detail = None
        if album_detail is not None:
            logger.info(f"专辑 {album_id} 详情已存在于数据库中: {album_detail.name}")
            try:
                album_info = await self.database.get_album_by_id(album_id)
            except ConnectionError as e:
                logger.error(f"数据库连接中断: {e}")
                await self.bot.send_group_message(f"数据库连接中断!", str(group_id))
                album_info = None
            try:
                user_name = await self.database.get_username(album_info['first_downloader_id']) if album_info else '未知'
            except ConnectionError as e:
                logger.error(f"数据库连接中断: {e}")
                await self.bot.send_group_message(f"数据库连接中断!", str(group_id))
            await self.bot.send_group_message(f"{album_detail.name}\n共{album_detail.page_count}页\ntag：{album_detail.tags}\n由{user_name}下载", str(group_id))
            #发送下载完成事件
            event_data = DownloadFinishedEvent("DOWNLOAD_FINISHED_EVENT", (album_detail, message))
            await self.event_queue.put_event(event_data)
        else:
            logger.info(f"专辑 {album_id} 详情不在数据库中，准备获取详情。")
            await self.bot.send_group_message(f"专辑 {album_id} 详情不在数据库中，准备获取详情。", str(group_id))
            try:
                album_detail = await downloader.get_album_detail(album_id)
            except Exception as e:
                logger.error(f"获取专辑 {album_id} 详情时发生错误：{e}", exc_info=True)
                await self.bot.send_group_message(f"获取专辑 {album_id} 详情失败：{str(e)}", str(group_id))
                return
            logger.debug(f"JmDownloader().get_album_detail({album_id}) returned: {album_detail}")            
            logger.info(f"专辑 {album_id} 详情获取成功: {album_detail.name}")
            #发送记录下载事件
            logger.info("发送记录下载事件")
            await self.event_queue.put_event(Event("RECORD_DOWNLOAD_EVENT", (album_detail, message)))
            await self.bot.send_group_message(f"{album_detail.name}\n共{album_detail.page_count}页\ntag：{album_detail.tags}", str(group_id))
            #下载本子
            try:
                await downloader.download_album(album_id)
            except Exception as e:
                logger.error(f"下载专辑 {album_id} 失败: {str(e)}", exc_info=True)
                await self.bot.send_group_message(f"下载专辑 {album_id} 失败：{str(e)}", str(group_id))
            logger.info(f"专辑 {album_id} 下载完成.")
            event_data = DownloadFinishedEvent("DOWNLOAD_FINISHED_EVENT", (album_detail, message))
            await self.event_queue.put_event(event_data)

        
        # # Only create and put the event if album_detail was successfully obtained
        # if album_detail:
        #     await downloader.download_album(album_id)
        #     #event_data = DowdnloadFinishedEvent("DOWNLOAD_FINISHED_EVENT", (album_detail, group_id))
        #     #await self.event_queue.put_event(event_data)
        # else:
        #     logger.warning(f"JmDownloader().download_album({album_id}) returned None. Sending failure message.")
        #     await self.bot.send_group_message(f"下载专辑 {album_id} 失败", str(group_id))

class RecordDownloadEventHandler(EventHandler):
    def __init__(self, event_queue: EventQueue, database: Database) -> None:
        self.event_queue = event_queue
        self.database = database

        super().__init__(event_queue, "RECORD_DOWNLOAD_EVENT")

    async def handle_event(self, event: Event) -> None:
        album_detail, message = event.data
        message: GroupMessage
        album_detail: JmAlbumDetail
        logger.info(f"Recording download for album: {album_detail.name} requested by {message.sender.nickname} (ID: {message.sender.user_id})")

        # Extract album information
        album_id = str(album_detail.id)
        album_name = album_detail.name
        # album_tags = ','.join(album_detail.tags) if album_detail.tags else ''
        downloader_id = str(message.sender.user_id)
        download_time = datetime.datetime.fromtimestamp(message.time).isoformat()

        # Record album information (if not already exists)
        await self.database.add_album(album_detail, download_time, downloader_id)

        # Record the download event
        await self.database.add_download(album_id, downloader_id, download_time)

        await self.database.add_user(downloader_id, message.sender.nickname)
        logger.info(f"Successfully recorded download for album '{album_name}' by user {downloader_id}")
