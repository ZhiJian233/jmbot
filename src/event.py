"""事件处理模块

该模块包含各种事件处理器，用于处理消息事件、下载请求和下载完成事件。
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import re
from typing import Any, Tuple, Callable, Awaitable, Optional

from jmcomic import JmAlbumDetail
from jm import JmDownloader
from qq import GroupMessage, QQBot, Message
from event_core import Event, EventQueue, EventHandler, DownloadFinishedEvent
from database import Database

logger = logging.getLogger(__name__)

# 事件类型常量
EVENT_TYPE_MESSAGE = "MESSAGE_EVENT"
EVENT_TYPE_GROUP_MESSAGE = "GROUP_MESSAGE_EVENT"
EVENT_TYPE_PRIVATE_MESSAGE = "PRIVATE_MESSAGE_EVENT"
EVENT_TYPE_DOWNLOAD_FINISHED = "DOWNLOAD_FINISHED_EVENT"
EVENT_TYPE_RECORD_DOWNLOAD = "RECORD_DOWNLOAD_EVENT"


class MessageEventHandler(EventHandler):
    """消息事件处理器
    
    负责解析原始消息事件并将其分发到相应的群组或私聊消息处理器。
    """
    
    def __init__(self, event_queue: EventQueue, bot: QQBot) -> None:
        """初始化消息事件处理器
        
        Args:
            event_queue: 事件队列
            bot: QQ机器人实例
        """
        self.event_queue = event_queue
        self.bot = bot
        super().__init__(event_queue, EVENT_TYPE_MESSAGE)

    async def handle_event(self, event: Event) -> None:
        """处理消息事件
        
        Args:
            event: 待处理的事件
        """
        try:
            message = QQBot.parse_message(event.data)
        except Exception as e:
            logger.error(f"解析消息失败，非消息事件: {e}")
            return
            
        if QQBot.is_group_message(message):
            await self.event_queue.put_event(Event(EVENT_TYPE_GROUP_MESSAGE, message))
        elif QQBot.is_private_message(message):
            await self.event_queue.put_event(Event(EVENT_TYPE_PRIVATE_MESSAGE, message))


class DownloadFinishedEventHandler(EventHandler):
    """下载完成事件处理器
    
    负责处理专辑下载完成后的通知和照片发送。
    """
    
    def __init__(self, event_queue: EventQueue, bot: QQBot, database: Database) -> None:
        """初始化下载完成事件处理器
        
        Args:
            event_queue: 事件队列
            bot: QQ机器人实例
            database: 数据库实例
        """
        self.event_queue = event_queue
        self.bot = bot
        self.database = database
        super().__init__(event_queue, EVENT_TYPE_DOWNLOAD_FINISHED)
        
    async def handle_event(self, event: DownloadFinishedEvent) -> None:
        """处理下载完成事件
        
        Args:
            event: 下载完成事件
        """
        logger.debug(f"收到下载完成事件，数据: {event.data}")
        
        # 验证事件数据格式
        if not isinstance(event.data, tuple) or len(event.data) != 2:
            logger.error(f"下载完成事件数据格式无效: {event.data}")
            return

        album_detail, message = event.data
        group_id = str(message.group_id)
        
        if album_detail:
            await self._send_album_completion_message(album_detail, group_id)
        else:
            await self._handle_missing_album_detail(group_id)
    
    async def _send_album_completion_message(
        self, 
        album_detail: JmAlbumDetail, 
        group_id: str
    ) -> None:
        """发送专辑下载完成消息
        
        Args:
            album_detail: 专辑详情
            group_id: 群组ID
        """
        logger.debug(f"专辑详情存在: {album_detail.authoroname}, ID: {album_detail.id}")
        await self.bot.send_group_message(
            f"专辑：{album_detail.authoroname} (ID: {album_detail.id})下载完成", 
            str(group_id)
        )
        await self.bot.send_forward_photos(str(group_id), album_detail)
    
    async def _handle_missing_album_detail(self, group_id: str) -> None:
        """处理专辑详情缺失的情况
        
        Args:
            group_id: 群组ID
        """
        try:
            album = await self.database.get_album_detail_by_id(str(group_id))
            if album:
                logger.debug(f"从数据库获取专辑详情 - 名称: {album.name}, ID: {album.id}")
                await self.bot.send_group_message(
                    f"专辑：{album.name} (ID: {album.id})下载完成", 
                    str(group_id)
                )
                await self.bot.send_forward_photos(str(group_id), album)
            else:
                logger.warning(f"无法从数据库获取群组 {group_id} 的专辑详情")
        except Exception as e:
            logger.error(f"获取专辑详情失败: {e}", exc_info=True)


class DownloadRequestEventHandler(EventHandler):
    """下载请求事件处理器
    
    负责处理用户的专辑下载请求，包括验证、下载和通知。
    """
    
    def __init__(self, event_queue: EventQueue, bot: QQBot, database: Database) -> None:
        """初始化下载请求事件处理器
        
        Args:
            event_queue: 事件队列
            bot: QQ机器人实例
            database: 数据库实例
        """
        self.event_queue = event_queue
        self.bot = bot
        self.database = database
        super().__init__(event_queue, EVENT_TYPE_GROUP_MESSAGE)

    def can_handle(self, event: Event) -> bool:
        """判断是否可以处理该事件
        
        Args:
            event: 待判断的事件
            
        Returns:
            如果消息是纯数字文本则返回True，否则返回False
        """
        message: GroupMessage = event.data
        if QQBot.is_pure_text_message(message):
            text = QQBot.get_message_text_content(message)
            if text and text.strip().isdigit():
                return True
        return False

    async def handle_event(self, event: Event) -> None:
        """处理下载请求事件
        
        Args:
            event: 下载请求事件
        """
        message: GroupMessage = event.data
        album_id_content = QQBot.get_message_text_content(message)
        album_id = album_id_content.strip() if album_id_content else ""
        group_id = str(message.group_id)

        logger.info(f"收到下载请求 - 专辑ID: {album_id}, 群组: {group_id}")
        await self.bot.send_group_message(f"开始下载专辑：{album_id}", group_id)

        # 尝试从数据库获取专辑详情
        album_detail = await self._get_album_from_database(album_id, group_id)
        
        if album_detail is not None:
            await self._handle_existing_album(album_detail, album_id, message, group_id)
        else:
            await self._handle_new_album(album_id, message, group_id)

    async def _get_album_from_database(
        self, 
        album_id: str, 
        group_id: str
    ) -> Optional[JmAlbumDetail]:
        """从数据库获取专辑详情
        
        Args:
            album_id: 专辑ID
            group_id: 群组ID
            
        Returns:
            专辑详情对象，如果不存在或发生错误则返回None
        """
        try:
            return await self.database.get_album_detail_by_id(album_id)
        except ConnectionError as e:
            logger.error(f"数据库连接中断: {e}")
            await self.bot.send_group_message("数据库连接中断！", group_id)
            return None
        except Exception as e:
            logger.error(f"获取专辑详情时发生错误: {e}", exc_info=True)
            return None

    async def _handle_existing_album(
        self,
        album_detail: JmAlbumDetail,
        album_id: str,
        message: GroupMessage,
        group_id: str
    ) -> None:
        """处理已存在的专辑
        
        Args:
            album_detail: 专辑详情
            album_id: 专辑ID
            message: 群组消息
            group_id: 群组ID
        """
        logger.info(f"专辑 {album_id} 已存在于数据库中: {album_detail.name}")
        
        # 获取下载者信息
        user_name = await self._get_downloader_name(album_id, group_id)
        
        # 发送专辑信息
        await self.bot.send_group_message(
            f"{album_detail.name}\n共{album_detail.page_count}页\ntag：{album_detail.tags}\n由{user_name}下载",
            group_id
        )
        
        # 发送下载完成事件
        event_data = DownloadFinishedEvent(
            EVENT_TYPE_DOWNLOAD_FINISHED, 
            (album_detail, message)
        )
        await self.event_queue.put_event(event_data)

    async def _get_downloader_name(self, album_id: str, group_id: str) -> str:
        """获取下载者名称
        
        Args:
            album_id: 专辑ID
            group_id: 群组ID
            
        Returns:
            下载者名称，如果获取失败则返回"未知"
        """
        try:
            album_info = await self.database.get_album_by_id(album_id)
            if album_info:
                username = await self.database.get_username(album_info['first_downloader_id'])
                return username if username else '未知'
            return '未知'
        except ConnectionError as e:
            logger.error(f"数据库连接中断: {e}")
            await self.bot.send_group_message("数据库连接中断！", group_id)
            return '未知'
        except Exception as e:
            logger.error(f"获取下载者信息失败: {e}", exc_info=True)
            return '未知'

    async def _handle_new_album(
        self,
        album_id: str,
        message: GroupMessage,
        group_id: str
    ) -> None:
        """处理新专辑的下载
        
        Args:
            album_id: 专辑ID
            message: 群组消息
            group_id: 群组ID
        """
        logger.info(f"专辑 {album_id} 不在数据库中，准备获取详情")
        await self.bot.send_group_message(
            f"专辑 {album_id} 不在数据库中，准备获取详情。",
            group_id
        )
        
        # 获取专辑详情
        downloader = JmDownloader()
        try:
            album_detail = await downloader.get_album_detail(album_id)
        except Exception as e:
            logger.error(f"获取专辑 {album_id} 详情时发生错误: {e}", exc_info=True)
            await self.bot.send_group_message(
                f"获取专辑 {album_id} 详情失败：{str(e)}",
                group_id
            )
            return
        
        logger.info(f"专辑 {album_id} 详情获取成功: {album_detail.name}")
        
        # 记录下载事件
        await self.event_queue.put_event(
            Event(EVENT_TYPE_RECORD_DOWNLOAD, (album_detail, message))
        )
        
        # 发送专辑信息
        await self.bot.send_group_message(
            f"{album_detail.name}\n共{album_detail.page_count}页\ntag：{album_detail.tags}",
            group_id
        )
        
        # 下载专辑
        await self._download_album(downloader, album_id, group_id)
        
        # 发送下载完成事件
        event_data = DownloadFinishedEvent(
            EVENT_TYPE_DOWNLOAD_FINISHED, 
            (album_detail, message)
        )
        await self.event_queue.put_event(event_data)

    async def _download_album(
        self,
        downloader: JmDownloader,
        album_id: str,
        group_id: str
    ) -> None:
        """下载专辑
        
        Args:
            downloader: 下载器实例
            album_id: 专辑ID
            group_id: 群组ID
        """
        try:
            await downloader.download_album(album_id)
            logger.info(f"专辑 {album_id} 下载完成")
        except Exception as e:
            logger.error(f"下载专辑 {album_id} 失败: {str(e)}", exc_info=True)
            await self.bot.send_group_message(
                f"下载专辑 {album_id} 失败：{str(e)}",
                group_id
            )


class RecordDownloadEventHandler(EventHandler):
    """记录下载事件处理器
    
    负责将下载记录保存到数据库中。
    """
    
    def __init__(self, event_queue: EventQueue, database: Database) -> None:
        """初始化记录下载事件处理器
        
        Args:
            event_queue: 事件队列
            database: 数据库实例
        """
        self.event_queue = event_queue
        self.database = database
        super().__init__(event_queue, EVENT_TYPE_RECORD_DOWNLOAD)

    async def handle_event(self, event: Event) -> None:
        """处理记录下载事件
        
        Args:
            event: 记录下载事件
        """
        album_detail: JmAlbumDetail
        message: GroupMessage
        album_detail, message = event.data
        
        logger.info(
            f"记录下载 - 专辑: {album_detail.name}, "
            f"请求者: {message.sender.nickname} (ID: {message.sender.user_id})"
        )

        # 提取专辑信息
        album_id = str(album_detail.id)
        album_name = album_detail.name
        downloader_id = str(message.sender.user_id)
        download_time = datetime.datetime.fromtimestamp(message.time).isoformat()

        try:
            # 记录专辑信息
            await self.database.add_album(album_detail, download_time, downloader_id)
            
            # 记录下载事件
            await self.database.add_download(album_id, downloader_id, download_time)
            
            # 记录用户信息
            await self.database.add_user(downloader_id, message.sender.nickname)
            
            logger.info(f"成功记录专辑 '{album_name}' 的下载，下载者: {downloader_id}")
        except Exception as e:
            logger.error(f"记录下载失败: {e}", exc_info=True)


class CommandHandler(EventHandler):
    """命令处理器

    负责处理群聊中的命令式消息，如tag查询等。
    """

    def __init__(self, event_queue: EventQueue, bot: QQBot, database: Database) -> None:
        """初始化命令处理器

        Args:
            event_queue: 事件队列
            bot: QQ机器人实例
            database: 数据库实例
        """
        self.event_queue = event_queue
        self.bot = bot
        self.database = database
        self.commands = {
            re.compile(r"^tag$"): self._handle_tag_all,
            re.compile(r"^tag (.+)$"): self._handle_tag_specific,
        }
        super().__init__(event_queue, EVENT_TYPE_GROUP_MESSAGE)

    def can_handle(self, event: Event) -> bool:
        """判断是否可以处理该事件

        Args:
            event: 待判断的事件

        Returns:
            如果消息是纯文本且匹配命令则返回True，否则返回False
        """
        message: GroupMessage = event.data
        if QQBot.is_pure_text_message(message):
            text = QQBot.get_message_text_content(message)
            if text:
                for pattern in self.commands.keys():
                    if pattern.match(text.strip()):
                        return True
        return False

    async def handle_event(self, event: Event) -> None:
        """处理命令事件

        Args:
            event: 命令事件
        """
        message: GroupMessage = event.data
        text_content = QQBot.get_message_text_content(message)
        text = text_content.strip() if text_content else ""
        group_id = str(message.group_id)

        for pattern, handler in self.commands.items():
            match = pattern.match(text)
            if match:
                await handler(match, group_id)
                break

    async def _handle_tag_all(self, match: re.Match, group_id: str) -> None:
        """处理显示所有tag统计的命令

        Args:
            match: 正则匹配对象
            group_id: 群组ID
        """
        try:
            stats = await self.database.get_tag_statistics()
            if not stats:
                await self.bot.send_group_message("暂无tag统计数据", group_id)
                return

            message_lines = ["所有tag统计："]
            for stat in stats[:20]:  # 限制显示前20个
                message_lines.append(f"{stat['tag_name']}: {stat['count']} 个专辑")

            if len(stats) > 20:
                message_lines.append(f"... 还有 {len(stats) - 20} 个tag")

            await self.bot.send_group_message("\n".join(message_lines), group_id)
        except Exception as e:
            logger.error(f"获取tag统计失败: {e}", exc_info=True)
            await self.bot.send_group_message("获取tag统计失败", group_id)

    async def _handle_tag_specific(self, match: re.Match, group_id: str) -> None:
        """处理显示特定tag专辑的命令

        Args:
            match: 正则匹配对象
            group_id: 群组ID
        """
        tag_name = match.group(1)
        try:
            albums = await self.database.get_albums_by_tag(tag_name)
            if not albums:
                await self.bot.send_group_message(f"tag '{tag_name}' 下暂无专辑", group_id)
                return

            # 获取tag统计
            stats = await self.database.get_tag_statistics()
            tag_count = next((stat['count'] for stat in stats if stat['tag_name'] == tag_name), 0)

            message_lines = [f"tag '{tag_name}' 共有 {tag_count} 个专辑："]
            for album in albums[:10]:  # 限制显示前10个
                message_lines.append(f"ID: {album['id']} - {album['name']}")

            if len(albums) > 10:
                message_lines.append(f"... 还有 {len(albums) - 10} 个专辑")

            await self.bot.send_group_message("\n".join(message_lines), group_id)
        except Exception as e:
            logger.error(f"获取tag '{tag_name}' 专辑失败: {e}", exc_info=True)
            await self.bot.send_group_message(f"获取tag '{tag_name}' 专辑失败", group_id)
