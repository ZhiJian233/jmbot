import functools
from typing import Callable, Any
from typing import Dict
import os
import pathlib
import websockets
import jmcomic
import aiosqlite


def jmcomic_create_option_by_file() -> jmcomic.JmOption:
    jmbot_path = pathlib.Path(__file__).parent.parent.absolute()
    print(jmbot_path)
    os.environ['JMBOT_PATH'] = f'{jmbot_path}'
    return jmcomic.create_option_by_file(f'{jmbot_path}/options/jmcomic_option.yml')

class JmDownloader:
    def __init__(self) -> None:
        client = jmcomic_create_option_by_file().build_jm_client()
        pass




