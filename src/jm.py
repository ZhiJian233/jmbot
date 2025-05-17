from typing import Dict
import os
import pathlib
import websockets
#from jmcomic  import *
import jmcomic

from qq import get_group_message
def jmcomic_create_option_by_file() -> jmcomic.JmOption:
    jmbot_path = pathlib.Path(__file__).parent.parent.absolute()
    print(jmbot_path)
    os.environ['JMBOT_PATH'] = f'{jmbot_path}'
    return jmcomic.create_option_by_file(f'{jmbot_path}/options/jmcomic_option.yml')
def creat_dowmload_album_callback(ws:websockets.connect,group_message:Dict)  -> None:
    def album_callback(album:jmcomic.JmAlbumDetail,dler:jmcomic.JmDownloader):
            group_message
            group_id = group_message['group_id']
            req = {
                "action": "send_group_msg",
                "params": {"group_id": f"{group_id}", "message": "hello  world"},
                "echo": "test"
            }
