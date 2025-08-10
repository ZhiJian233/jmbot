import requests
import json
import websockets
import asyncio
import qq
import yaml
import jm

async def main():
    jmoption=jm.jmcomic_create_option_by_file()
    with open('options/qq_option.yml', 'r') as f:
        config = yaml.safe_load(f)
    uri = config.get('url', 'ws://localhost:3001/')
    
    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv(decode=True)
            msg_text = qq.get_group_message_text(msg)
            
            if msg_text is not None:
                if msg_text.strip().isdigit():
                    album_id = int(msg_text.strip())
                    try:
                        def callback(album, downloader):
                            title = album.title
                            qq.send_group_message(
                                ws,
                                f"开始下载专辑：{title} (ID: {album_id})",
                                json.loads(msg).get("group_id")
                            )
                        
                        jm.jmcomic.download_album(album_id, option=jmoption, callback=callback)
                        qq.send_group_message(
                            ws,
                            f"专辑 {album_id} 下载完成！",
                            json.loads(msg).get("group_id")
                        )
                    except Exception as e:
                        qq.send_group_message(
                            ws,
                            f"下载专辑 {album_id} 失败：{str(e)}",
                            json.loads(msg).get("group_id")
                        )
                else:
                    qq.send_group_message(
                        ws,
                        msg_text,
                        json.loads(msg).get("group_id")
                    )

asyncio.run(main())
