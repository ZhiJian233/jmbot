from re import L
import requests
import json
import websockets
import asyncio
import qq
import yaml
import jm
from qq import *
albums_id_to_name_map = {}
async def main():
    jmoption=jm.jmcomic_create_option_by_file()
    with open('options/qq_option.yml', 'r') as f:
        config = yaml.safe_load(f)
    uri = config.get('url', 'ws://localhost:3001/')
    
    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv(decode=True)
            msg_text = qq.get_group_message_text(msg)

            bot = QQBot(ws)

            message_event = bot.ifis_group_message(bot.parse_message(msg))
            
            bot.send_group_message()
            
            
            if msg_text is not None:
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
