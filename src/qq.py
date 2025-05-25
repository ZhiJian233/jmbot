import asyncio
import websockets
import json
from typing import Union, Dict, List, Optional

def get_group_message(json_data: str) -> Optional[Dict[str, Union[List[Optional[Dict[str,Union[str,Dict]]]], str, None]]]: 
    if json_data is None:
        print("json_data为空")
        return None
    group_message = {
        "messages": [],
        "group_id": None,
        "user_id": None
    }
    try:
        json_data = json.loads(json_data)
        if (isinstance(json_data, dict) and 
            json_data.get("post_type") == "message" and 
            json_data.get("message_type") == "group"):
            group_message["group_id"] = json_data.get("group_id")
            group_message["user_id"] = json_data.get("user_id")
            messages = json_data.get("message", [])
            if isinstance(messages, list) and messages:
                group_message["messages"] = messages
                print("群消息解析成功")
                return group_message
    except json.JSONDecodeError:
        print("json_data解析错误")
        pass
    return None

def send_group_message(ws:websockets.ClientConnection,message_to_send:str,group_id:str)  -> None:
    if message_to_send is None:
        return
    req = {
        "action": "send_group_msg",
        "params": {"group_id": f"{group_id}", "message": f"{message_to_send}"}
    }
    loop = asyncio.get_event_loop()
    future = asyncio.run_coroutine_threadsafe(ws.send(json.dumps(req)), loop)


def get_group_message_text(json_data: str) -> Optional[str]:
    """
    获取群消息的文本
    :param json_data:
    :return:
    :rtype:
    """
    text = ""
    print(json_data)
    group_message = get_group_message(json_data)
    print(group_message)
    if group_message is None:
        print("群消息为空")
        return None
    messages = group_message.get("messages", [])
    if not isinstance(messages, list):
        print("群消息不是列表")
        return None
    for message in messages:
        
        if not isinstance(message, dict) or message.get("type") != "text":
            continue
            
        text_data = message.get("data", {})
        if not isinstance(text_data, dict):
            continue
            
        text_content = text_data.get("text", "")
        if isinstance(text_content, str):
            print(text_content)
            text += text_content
    return text if text else None
