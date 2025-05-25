import functools
from turtle import down
from typing import Callable, Any
from typing import Dict
import os
import pathlib
import websockets
#from jmcomic  import *
import jmcomic

from qq import get_group_message,send_group_message

def jmcomic_create_option_by_file() -> jmcomic.JmOption:
    jmbot_path = pathlib.Path(__file__).parent.parent.absolute()
    print(jmbot_path)
    os.environ['JMBOT_PATH'] = f'{jmbot_path}'
    return jmcomic.create_option_by_file(f'{jmbot_path}/options/jmcomic_option.yml')    

def jmdownloader_callback_factory(
    func: Callable,
    *fixed_args: Any,
    **fixed_kwargs: Any
) -> Callable[[Any, Any], Any]:
    """
    函数工厂，生成只接收前两个参数的回调函数
    
    :param func: 原始函数，需至少接收两个位置参数
    :param fixed_args: 需要固定的位置参数
    :param fixed_kwargs: 需要固定的关键字参数
    :return: 接收两个参数的回调函数
    """
    @functools.wraps(func)
    def wrapped_callback(album: jmcomic.JmAlbumDetail, downloader: jmcomic.JmDownloader) -> Any:
        return func(album, downloader, *fixed_args, **fixed_kwargs)
    return wrapped_callback
