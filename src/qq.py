import json
def get_message_text(json_data : str) -> str:
    json_data = json.loads(json_data)
    if isinstance(json_data, dict) and "message" in json_data:
        messages = json_data["message"]
        if messages and isinstance(messages, list) and len(messages) > 0:
            first_msg = messages[0]
            if "data" in first_msg and "text" in first_msg["data"]:
                return first_msg["data"]["text"]
    return None

