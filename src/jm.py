import functools
import re
from typing import Callable, Any, Optional
from typing import Dict
import os
import pathlib
import websockets
import jmcomic
import aiosqlite
from qq import QQBot
import logging
from jmcomic import JmAlbumDetail
import asyncio

logger = logging.getLogger(__name__)

def jmcomic_create_option_by_file() -> jmcomic.JmOption:
    jmbot_path = pathlib.Path(__file__).parent.parent.absolute()
    print(jmbot_path)
    os.environ['JMBOT_PATH'] = f'{jmbot_path}'
    return jmcomic.create_option_by_file(f'{jmbot_path}/options/jmcomic_option.yml')

class JmDownloader:
    def __init__(self) -> None:
        self.jm_option = jmcomic_create_option_by_file()
        self.jm_client = self.jm_option.build_jm_client()


    async def download_album(self, album_id: str) -> Optional[JmAlbumDetail]:    
        try:
            # First, try to get album details to verify album_id and get album_detail object
            album_detail = await self.get_album_detail(int(album_id))
            if album_detail is None:
                logger.warning(f"获取专辑 {album_id} 详情失败，无法下载。")
                return None

            # If album_detail is successfully obtained, proceed with download
            await asyncio.to_thread(self.jm_option.download_photo, str(album_id))
            logger.debug(f"jm_option.download_photo({album_id}) called for download.")
            
            # After download, return the album_detail obtained earlier
            return album_detail
        except Exception as e:
            logger.error(f"下载专辑 {album_id} 失败: {str(e)}", exc_info=True)
            return None # Return None on exception

    async def get_album_detail(self, album_id: int) -> Optional[JmAlbumDetail]:
        try: 
            album_detail = await asyncio.to_thread(self.jm_client.get_album_detail, album_id)
            logger.debug(f"jm_client.get_album_detail({album_id}) returned: {album_detail}")
            return album_detail
        except Exception as e:
            logger.error(f"获取专辑 {album_id} 详情失败: {str(e)}", exc_info=True)
            return None
