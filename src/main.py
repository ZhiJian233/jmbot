import requests
import json
import websockets
import asyncio
import qq
async def main():
    uri = "ws://localhost:3001/"
    async with websockets.connect(uri) as ws:
        # 发送请求 v
        req = {
            "action": "send_group_msg",
            "params": {"group_id": 150508453, "message": "测试"},
            "echo": "test"
        }
        #await ws.send(json.dumps(req))
        # 接收响应或事件
        while True:
            msg  = await ws.recv(decode=True)
            print(msg)
            print(qq.get_group_message_text(msg))
            if qq.get_group_message_text(msg) is  not None:
                print("群消息")
                msg_text = qq.get_group_message_text(msg)

                print(msg_text)
                if msg_text:
                    print("测试成功")
                    qq.send_group_message(ws, msg_text, json.loads(msg).get("group_id"))
            else:
                print("非群消息")



asyncio.run(main())
