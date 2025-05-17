import json
from typing import Union, Dict, List, Optional

def get_group_message(json_data: str) -> Optional[Dict[str, Union[List, str, None]]]:
    if json_data is None:
        return None
    group_message = {
        "message": [],
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
                group_message["message"] = messages
                return group_message
    except json.JSONDecodeError:
        pass
    return None
