from email import message
import logging
from event_core import EventQueue
import event
from calendar import c
import jmcomic
import requests
import json
import websockets
import asyncio
import yaml
import jm
from qq import *
from typing import Union
from asyncio import Queue
from database import Database

# class MessageHandler(event.EventHandler):
#     def __init__(self, event_queue: event.EventQueue, queue: asyncio.Queue, jmoption: jmcomic.JmOption, bot: QQBot, loop: asyncio.AbstractEventLoop):
#         super().__init__("MESSAGE_EVENT", event_queue)
#         self.queue = queue
#         self.jmoption = jmoption
#         self.jmclient: Union[jmcomic.JmHtmlClient, jmcomic.JmApiClient] = jmoption.build_jm_client()
#         self.bot = bot
#         self.loop = loop

#     async def handle_event(self, event: event.Event):
#         message = event.data
#         if QQBot.is_group_message(message) and QQBot.is_pure_text_message(message):
#             text = QQBot.get_message_text_content(message)
#             if text and text.strip().isdigit():
#                 album_id = int(text.strip())

#                 def callback(album: jmcomic.JmAlbumDetail, downloader):
#                     logger.info(f"专辑{album.name}下载完成")
#                     asyncio.run_coroutine_threadsafe(self.queue.put((album, album_id, message.group_id)), self.loop)
#                 try:
#                     album_detail: JmAlbumDetail = self.jmclient.get_album_detail(album_id)
#                     await self.bot.send_group_message(f"开始下载专辑 {album_id}[{album_detail.name}]{album_detail.tags}", str(message.group_id))
                
#                     await asyncio.get_event_loop().run_in_executor(None,
#                     lambda: self.jmoption.download_photo(str(album_id), callback=callback))
#                 except Exception as e:
#                     logger.error(e)
#                     await self.bot.send_group_message(
#                         f"下载专辑 {album_id} 失败：{str(e)}",
#                         str(message.group_id))


# async def process_queue(queue: asyncio.Queue, bot: QQBot):
#     while True:
#         album: jmcomic.JmAlbumDetail
#         album, album_id, group_id = await queue.get()
#         await bot.send_group_message(f"专辑：{album.authoroname} (ID: {album_id})下载完成", str(group_id))
#         await bot.send_forward_photos(group_id, album)



async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s[%(name)s]-[%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('jm_bot.log', encoding='utf-8')]
    )
    logger = logging.getLogger(__name__)
    
    jmoption: jmcomic.JmOption = jm.jmcomic_create_option_by_file()
    jmclient: Union[jmcomic.JmHtmlClient, jmcomic.JmApiClient] =  jmoption.build_jm_client()
    with open('options/qq_option.yml', 'r') as f:
        config = yaml.safe_load(f)
    uri = config.get('url', 'ws://localhost:3001/')
    
    async with websockets.connect(uri) as ws:
        logger.info("websocket已连接") 
        logger.info("开始初始化")

        # queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        main_event_queue = EventQueue(loop)
        bot = QQBot(ws, main_event_queue, loop)

        # Initialize database
        db = Database()
        await db.connect()
        
        event_router = event.EventRouter(main_event_queue, bot)
        heartbeat_handler = event.HeartbeatEventHandler(main_event_queue)
        message_dispatcher = event.MessageDispatcher(main_event_queue)
        download_request_handler = event.DownloadRequestEventHandler(main_event_queue, bot, db)
        download_finished_handler = event.DownloadFinishedEventHandler(main_event_queue, bot, db)
        record_download_handler = event.RecordDownloadEventHandler(main_event_queue, db)
        command_handler = event.CommandHandler(main_event_queue, bot, db)
        
        logger.info("初始化完成 开始运行")
        try:
            # Keep the connection alive
            await asyncio.Future()  # Run forever
        finally:
            await db.close()

asyncio.run(main())
