from typing import LiteralString, Union, Dict, List, Optional, Literal, Annotated, Any
from pydantic import BaseModel, Field

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

class GroupMessage(MessageEvent):
    message_type: Literal["group"]
    group_id: int
    sender: GroupSender

class PrivateMessage(MessageEvent):
    message_type: Literal["private"]
    sender: Sender

Message = Annotated[Union[PrivateMessage,GroupMessage],Field(discriminator='message_type')]
