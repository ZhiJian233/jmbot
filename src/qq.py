from email import message
from email.contentmanager import ContentManager
import os
import asyncio
import websockets
import json
from typing import LiteralString, Union, Dict, List, Optional, Literal, Annotated, Any
import event
import file_utils
from pydantic import BaseModel, Field
from jmcomic import *
# def get_group_message(json_data: str) -> Optional[Dict[str, Union[List[Optional[Dict[str,Union[str,Dict]]]], str, None]]]: 
#     if json_data is None:
#         print("json_data为空")
#         return None
#     group_message = {
#         "messages": [],
#         "group_id": None,
#         "user_id": None
#     }
#     try:
#         print(json_data)
#         json_data = json.loads(json_data)
#         if (isinstance(json_data, dict) and 
#             json_data.get("post_type") == "message" and 
#             json_data.get("message_type") == "group"):
#             group_message["group_id"] = json_data.get("group_id")
#             group_message["user_id"] = json_data.get("user_id")
#             messages = json_data.get("message", [])
#             if isinstance(messages, list) and messages:
#                 group_message["messages"] = messages
#                 print("群消息解析成功")
#                 return group_message
#     except json.JSONDecodeError:
#         print("json_data解析错误")
#         pass
#     return None

# def send_group_message(ws:websockets.ClientConnection,message_to_send:str,group_id:str)  -> None:
#     if message_to_send is None:
#         return
#     req = {
#         "action": "send_group_msg",
#         "params": {"group_id": f"{group_id}", "message": f"{message_to_send}"}
#     }
    
#     loop = asyncio.get_event_loop()
#     future = asyncio.run_coroutine_threadsafe(ws.send(json.dumps(req)), loop)


# def get_group_message_text(json_data: str) -> Optional[str]:
#     """
#     获取群消息的文本
#     :param json_data:
#     :return:
#     :rtype:
#     """
#     text = ""
#     group_message = get_group_message(json_data)
#     if group_message is None:
#         print("群消息为空")
#         return None
#     messages = group_message.get("messages", [])
#     if not isinstance(messages, list):
#         print("群消息不是列表")
#         return None
#     for message in messages:
        
#         if not isinstance(message, dict) or message.get("type") != "text":
#             continue
            
#         text_data = message.get("data", {})
#         if not isinstance(text_data, dict):
#             continue
            
#         text_content = text_data.get("text", "")
#         if isinstance(text_content, str):
#             print(text_content)
#             text += text_content
#     return text if text else None
import logging
logger = logging.getLogger(__name__)

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
    def __init__(self, ws:websockets.ClientConnection, event_queue: event.EventQueue, loop :asyncio.AbstractEventLoop):
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
                raise ValueError("Unknown message type")
        except Exception as e:
            raise ValueError(f"无效信息 JSON: {e}")
        
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
                    "nickname": "麦麦",
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
        
    async def send_forward_photos(self, group_id:str,albums: JmAlbumDetail) -> None:
        content = []
        jmpath = os.environ['JMBOT_PATH']
        photos = file_utils.list_files_iter(f"{jmpath}/albums/{albums.title}")
        for photo in photos :
            content.append(
                {
                    "type" : "image" ,
                    "data" : 
                    {
                        "file" : f"{photo}"
                    }
                }
            )
            print(content)
        await self.send_forward_msg(group_id,content, albums.name, albums.author)

    async def receive_message(self) -> None:
        while True:
            try:
                msg = await self.ws.recv(decode=True)
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed")
                asyncio.sleep(3)
                continue
            try:
                message = self.parse_message(msg)
            except Exception as e:
                logger.error(e) 
                continue
            event_data : event.Event = event.Event("MESSAGE_EVENT", message)
            logger.info(message)
            await self.event_queue.put_event(event_data)
