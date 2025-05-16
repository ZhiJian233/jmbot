import requests
import json
import websockets
import asyncio
import json

async def main():
    uri = "ws://localhost:3001/"
    async with websockets.connect(uri) as ws:
        # 发送请求 v
        req = {
            "action": "send_group_msg",
            "params": {"group_id": 150508453, "message": "hello  world"},
            "echo": "test"
        }
        #await ws.send(json.dumps(req))
        # 接收响应或事件
        while True:
            msg  = await ws.recv()
            print(msg)
            msg  = json.loads(msg)
            if msg["post_type"] == "message" :
                if msg["message"][0]["type"] == "text":
                    print("收到消息：", msg["message"][0]["data"]["text"])
asyncio.run(main())
