#from email import message
#from email.contentmanager import ContentManager
import os
import asyncio
import re
import websockets
import json
from typing import LiteralString, Union, Dict, List, Optional, Literal, Annotated, Any
from event_core import EventQueue, Event
import event
import file_utils
from pydantic import BaseModel, Field
from jmcomic import JmAlbumDetail
import time
import datetime

import logging


logger: logging.Logger = logging.getLogger(__name__)

class Sender(BaseModel):
    user_id : int
    nickname : str

class GroupSender(Sender):
    card: str
    role: str


class BaseEvent(BaseModel):
    post_type: str
    time: int

class MessageSegment(BaseModel):
    type: str
    data: Dict[str, Any]

class MessageEvent(BaseEvent):
    post_type: Literal["message"]
    user_id: int
    message: List[MessageSegment]
    raw_message: str
                             
class GroupMessageEvent(MessageEvent):
    message_type: Literal["group"]
    group_id: int
    sender: GroupSender

class PrivateMessageEvent(MessageEvent):
    message_type: Literal["private"]
    sender: Sender

Message = Annotated[Union[PrivateMessageEvent,GroupMessageEvent],Field(discriminator='message_type')]

class QQBot:
    def __init__(self, ws:websockets.ClientConnection, event_queue: EventQueue, loop :asyncio.AbstractEventLoop):
        self.ws = ws
        self.event_queue = event_queue
        self.loop = loop
        asyncio.create_task(self.receive_message())

        
    @staticmethod
    def parse_message(json_str: str) -> Message:
        try:
            data = json.loads(json_str)
            if data.get("message_type") == "group":
                return GroupMessageEvent.model_validate(data)
            elif data.get("message_type") == "private":
                return PrivateMessageEvent.model_validate(data)
            else:
                raise ValueError(f"Unknown message type: {json_str}")
        except Exception as e:
            raise ValueError(f"无效信息 JSON: {json_str}")
        
    @staticmethod
    def is_group_message(message: Message) -> bool:
        if isinstance(message, GroupMessageEvent):
            return True
        else:
            return False
        
    @staticmethod
    def is_private_message(message: Message) -> bool:
        if isinstance(message, PrivateMessageEvent):
            return True
        else:
            return False
        
    @staticmethod
    def is_pure_text_message(message: Message) -> bool:
        for message_content in message.message:
            if message_content.type != "text":
                return False
        return True
    @staticmethod
    def get_message_text_content(message: Message) -> Optional[str]:
        content :str =""
        for message_content in message.message:
            if message_content.type == "text":
                content = content + message_content.data["text"]
        return content



    async def send_group_message(self, message_to_send: str, group_id: str) -> None:
        if message_to_send is None:
            return
        req = {
            "action": "send_group_msg",
            "params": {"group_id": f"{group_id}", "message": f"{message_to_send}"}
        }
        await self.ws.send(json.dumps(req))

    async def send_forward_msg(self, group_id: str,content: list, title: str, subtitle: str) -> None:
        data = {
        "group_id": f"{group_id}",
        "messages": [
            {
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": f"麦麦",
                    "content": content,
                }
            }
        ],
        "news": [
            {
                "text": subtitle
            }
        ],
        "prompt": title,
        "summary": title,
        "source": title
    }
        req = {
            "action": "send_forward_msg",
            "params": data
        }
        await self.ws.send(json.dumps(req))

    def __make_forward_msg_content(self, content: list, title: str, subtitle: str) -> Dict:
        data = {
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": "麦麦",
                    "content": content,
                },
                "news":[{
                    "text": subtitle
                }],
                "prompt": title,
                "summary": title,
                "source": title}
        return data
        
    async def send_forward_photos(self, group_id:str,albums: JmAlbumDetail) -> None:
        content = []
        jmpath = os.environ['JMBOT_PATH']
        photos = file_utils.list_files_iter(f"{jmpath}/albums/{albums.id}")
        count = 0
        vol = 1
        async for photo in photos :
            count+=1
            content.append(
                {
                    "type" : "image" ,
                    "data" :
                    {
                        "file" : f"{photo}"
                    }
                }
            )
            logger.info(f"添加图片 vol.{vol} {count}")
            if count == 99:
                content.append(
                    {
                        "type" : "text" ,
                        "data" :
                        {
                            "text" : f"{int(time.time())}"
                        }
                    }
                )
                logger.info(f"向{group_id}发送:{albums.name} part vol {vol}")
                #forward_msg_list.append(self.__make_forward_msg_content(content, f"vol {vol} {albums.name}", albums.author))
                asyncio.create_task(self.send_forward_msg(group_id, content, f"vol.{vol} {albums.name}", f"{albums.author}@{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))
                logger.info(f"{vol} {count}")
                count = 0
                vol += 1
                content = []

        logger.info(f"向{group_id}发送:{albums.name} part vol {vol}")
        #forward_msg_list.append(self.__make_forward_msg_content(content, f"vol {vol} {albums.name}", albums.author))
        asyncio.create_task(self.send_forward_msg(group_id, content, f"vol.{vol} {albums.name}", f"{albums.author}@{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))
        
    async def receive_message(self) -> None:
        while True:
            message = None  # Initialize message to None
            try:
                msg = await self.ws.recv(decode=True)
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed")
                await asyncio.sleep(30)
                continue # Continue the loop to try reconnecting

            try:
                message = self.parse_message(msg)
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
                continue # Skip this message if parsing fails
            
            if message: # Only process if message was successfully parsed
                event_data = Event("MESSAGE_EVENT", message)
                logger.info(message)
                await self.event_queue.put_event(event_data)
