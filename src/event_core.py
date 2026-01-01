import asyncio
from dataclasses import dataclass
from email import message
from math import e
from typing import Any, Tuple, Callable, Awaitable, Optional
from jmcomic import JmAlbumDetail
from message_types import Message, GroupMessage, PrivateMessage
import logging

logger = logging.getLogger(__name__)

@dataclass
class Event:
    type: str
    data: Any

@dataclass
class DownloadRequestEvent(Event):
    type = "DOWNLOAD_REQUEST_EVENT"
    data: Tuple[int, int]  # album_id, group_id

@dataclass
class DownloadFinishedEvent(Event):
    type = "DOWNLOAD_FINISHED_EVENT"
    data: Tuple[JmAlbumDetail, GroupMessage] # Changed JmAlbumDetail to Any to avoid circular import with jm

@dataclass
class MessageEvent(Event):
    type = "MESSAGE_EVENT"
    data: Message

@dataclass
class GroupMessageEvent(Event):
    type = "GROUP_MESSAGE_EVENT"
    data: GroupMessage

@dataclass
class PrivateMessageEvent(Event):
    type = "PRIVATE_MESSAGE_EVENT"
    data: PrivateMessage

@dataclass
class RecordDownloadEvent(Event):
    type = "RECORD_DOWNLOAD_EVENT"
    data: Tuple[JmAlbumDetail, Message]

class EventQueue:
    def __init__(self, loop :asyncio.AbstractEventLoop) -> None:
        self.queue = asyncio.Queue()
        self.loop = loop
        self.handler_list = []
        self.loop.create_task(self.process_events())

    async def put_event(self, event: Event) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def register_handler(self, handler: 'EventHandler') -> None: # Forward reference for EventHandler
        self.handler_list.append(handler)

    async def process_events(self) -> None:
        while True:
            event: Event = await self.queue.get()
            logger.info(event.type)
            for handler in self.handler_list:
                handler(event)
                # if handler.can_handle(event):
                #     asyncio.create_task(handler.handle_event(event))

class EventHandler:
    def __init__(self, event_queue: EventQueue, event_type: str, handle_func: Optional[Callable[[Event], Awaitable[None]]] = None, can_handle_func: Optional[Callable[[Event], bool]] = None) -> None:
        self.event_type = event_type
        self.event_queue = event_queue
        self.handle_func = handle_func
        self.can_handle_func = can_handle_func
        self.event_queue.register_handler(self)

    def __call__(self, event: Event) -> Any:
        if event.type == self.event_type and self.can_handle(event):
            asyncio.create_task(self.handle_event(event))

    def can_handle(self, event: Event) -> bool:
        if self.can_handle_func:
            return self.can_handle_func(event)
        return True

    async def handle_event(self, event: Event) -> None:
        if self.handle_func:
            await self.handle_func(event)
