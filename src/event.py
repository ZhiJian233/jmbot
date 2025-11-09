from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

@dataclass
class Event:
    type: str
    data: Any

class EventQueue:
    def __init__(self, loop :asyncio.AbstractEventLoop) -> None:
        self.queue = asyncio.Queue(5)
        self.loop = loop
        self.handler_list = []
        self.loop.create_task(self.process_events())

    async def put_event(self, event: Event) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def register_handler(self, handler: EventHandler) -> None:
        self.handler_list.append(handler)

    async def process_events(self) -> None:
        while True:
            event = await self.queue.get()
            for handler in self.handler_list:
                if handler.can_handle(event):
                    await handler.handle_event(event)


class EventHandler:
    def __init__(self, event_type: str, event_queue: EventQueue) -> None:
        self.event_type = event_type
        self.event_queue = event_queue

    def can_handle(self, event: Event) -> bool:
        return event.type == self.event_type
    
    async def handle_event(self, event: Event) -> None:
        pass
