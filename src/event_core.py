import asyncio
from dataclasses import dataclass
from typing import Any, Tuple, Callable, Awaitable, Optional

@dataclass
class Event:
    type: str
    data: Any

class DowdnloadFinishedEvent(Event):
    data: Tuple[Any, int] # Changed JmAlbumDetail to Any to avoid circular import with jm

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
            event = await self.queue.get()
            for handler in self.handler_list:
                if handler.can_handle(event):
                    asyncio.create_task(handler.handle_event(event))

class EventHandler:
    def __init__(self, event_queue: EventQueue, event_type: str, handle_func: Optional[Callable[[Event], Awaitable[None]]] = None, can_handle_func: Optional[Callable[[Event], bool]] = None) -> None:
        self.event_type = event_type
        self.event_queue = event_queue
        self.handle_func = handle_func
        self.can_handle_func = can_handle_func
        self.event_queue.register_handler(self)

    def can_handle(self, event: Event) -> bool:
        if self.can_handle_func:
            return self.can_handle_func(event)
        return event.type == self.event_type

    async def handle_event(self, event: Event) -> None:
        if self.handle_func:
            await self.handle_func(event)
