from calendar import c
from re import L
import jmcomic
import requests
import json
import websockets
import asyncio
import qq
import yaml
import jm
from qq import *
from typing import Union
from asyncio import Queue

async def process_queue(queue: asyncio.Queue, bot: QQBot):
    while True:
        album: jmcomic.JmAlbumDetail
        album, album_id, group_id = await queue.get()
        await bot.send_group_message(f"专辑：{album.authoroname} (ID: {album_id})下载完成", str(group_id))
        await qq.send_forward_photos(bot.ws, group_id, album)
async def main():
    jmoption: jmcomic.JmOption = jm.jmcomic_create_option_by_file()
    jmclient: Union[jmcomic.JmHtmlClient, jmcomic.JmApiClient] =  jmoption.build_jm_client()
    albums_id_to_name_map = {}
    with open('options/qq_option.yml', 'r') as f:
        config = yaml.safe_load(f)
    uri = config.get('url', 'ws://localhost:3001/')

    async with websockets.connect(uri) as ws:
        bot = QQBot(ws)
        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        asyncio.create_task(process_queue(queue, bot))

        while True:
            msg = await ws.recv(decode=True)
            msg_text = qq.get_group_message_text(msg)

            try:
                message = QQBot.parse_message(msg)
                if isinstance(message, GroupMessageEvent):
                    group_message = message
                    if QQBot.is_pure_text_message(group_message):
                        text = QQBot.get_message_text_content(group_message)
                        if text != None and text.strip().isdigit():
                            album_id = int(text.strip())

                            def callback(album: jmcomic.JmAlbumDetail, downloader):
                                asyncio.run_coroutine_threadsafe(queue.put((album, album_id, group_message.group_id)), loop)

                            await bot.send_group_message(text, str(group_message.group_id))
                            try:
                                await asyncio.get_event_loop().run_in_executor(None,
                                lambda: jmoption.download_photo(str(album_id),callback=callback))
                            except Exception as e:
                                print(e)
                                await bot.send_group_message(
                                    f"下载专辑 {album_id} 失败：{str(e)}",
                                    str(group_message.group_id))

                        # Handle group message here
                        # Example: await bot.send_group_message("Response message", str(message_event.group_id))
                        pass
            except ValueError as e:
                print(f"Failed to parse message: {e}")


            if msg_text is not None and False:
                # qq.send_forward_photos(ws,json.loads(msg).get("group_id"),"[MANA] 神里绫华 1 (原神) [中国语] [无修正]")
                if msg_text.strip().isdigit():
                    album_id = int(msg_text.strip())
                    try:
                        qq.send_group_message(
                                ws,
                                f"{album_id}",
                                json.loads(msg).get("group_id")
                            )
                        def callback(album, downloader):
                            albums_id_to_name_map[album_id] = album.title
                            qq.send_group_message(
                                ws,
                                f"开始下载专辑：{album.title} (ID: {album_id})",
                                json.loads(msg).get("group_id")
                            )

                        jm.jmcomic.download_album(album_id, option=jmoption, callback=callback)
                        qq.send_group_message(
                            ws,
                            f"专辑 {album_id} 下载完成！",
                            json.loads(msg).get("group_id")
                        )
                        await qq.send_forward_photos(ws,json.loads(msg).get("group_id"),f"{albums_id_to_name_map[album_id]}")
                    except Exception as e:
                        qq.send_group_message(
                            ws,
                            f"下载专辑 {album_id} 失败：{str(e)}",
                            json.loads(msg).get("group_id")
                        )
                # else:
                #     qq.send_group_message(
                #         ws,
                #         msg_text,
                #         json.loads(msg).get("group_id")
                #     )
asyncio.run(main())

# jmoption=jm.jmcomic_create_option_by_file()
# qq.photos_send_test()
