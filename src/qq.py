#from email import message
#from email.contentmanager import ContentManager
import os
import asyncio
import re
import random
import tempfile
import concurrent.futures
import websockets
import json
from typing import LiteralString, Union, Dict, List, Optional, Literal, Annotated, Any
from event_core import EventQueue, Event
import event
import file_utils
from message_types import Message, GroupMessage, PrivateMessage
from jmcomic import JmAlbumDetail
from PIL import Image
import time
import datetime

import logging


logger: logging.Logger = logging.getLogger(__name__)

class QQBot:
    def __init__(self, ws:websockets.ClientConnection, event_queue: EventQueue, loop :asyncio.AbstractEventLoop):
        self.ws = ws
        self.event_queue = event_queue
        self.loop = loop
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._request_counter = 0
        asyncio.create_task(self.receive_message())

        
    @staticmethod
    def parse_message(json_str: str) -> Message:
        try:
            data = json.loads(json_str)
            if data.get("message_type") == "group":
                return GroupMessage.model_validate(data)
            elif data.get("message_type") == "private":
                return PrivateMessage.model_validate(data)
            else:
                raise ValueError(f"Unknown message type: {json_str}")
        except Exception as e:
            raise ValueError(f"{e}.无效信息 JSON: {json_str}")
        
    @staticmethod
    def is_group_message(message: Message) -> bool:
        if isinstance(message, GroupMessage):
            return True
        else:
            return False
        
    @staticmethod
    def is_private_message(message: Message) -> bool:
        if isinstance(message, PrivateMessage):
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

    @staticmethod
    def get_at_targets(message: Message) -> list[str]:
        """提取消息中所有 @ 目标的 QQ 号"""
        targets = []
        for segment in message.message:
            if segment.type == "at":
                qq = segment.data.get("qq")
                if qq:
                    targets.append(str(qq))
        return targets

    async def send_forward_text(self, group_id: str, text: str, title: str) -> None:
        """将多行文本包装为合并转发消息，长文本自动分多个节点"""
        # 按行分割，每个节点最多 4000 字符
        lines = text.split('\n')
        nodes = []
        chunk = []
        chunk_len = 0
        for line in lines:
            if chunk and chunk_len + len(line) > 4000:
                nodes.append([{"type": "text", "data": {"text": '\n'.join(chunk)}}])
                chunk = []
                chunk_len = 0
            chunk.append(line)
            chunk_len += len(line) + 1
        if chunk:
            nodes.append([{"type": "text", "data": {"text": '\n'.join(chunk)}}])

        data = {
            "group_id": f"{group_id}",
            "message": [
                {
                    "type": "node",
                    "data": {
                        "user_id": 2377284392,
                        "nickname": "麦麦",
                        "content": node_content,
                    }
                }
                for node_content in nodes
            ],
            "news": [{"text": title}],
            "prompt": title,
            "summary": title,
            "source": title
        }
        await self.call_api("send_forward_msg", data, timeout=300)

    async def send_group_message(self, message_to_send: str, group_id: str) -> None:
        if message_to_send is None:
            return
        req = {
            "action": "send_group_msg",
            "params": {"group_id": f"{group_id}", "message": f"{message_to_send}"}
        }
        await self.ws.send(json.dumps(req))

    async def send_forward_msg(self, group_id: str,content: list, title: str, subtitle: str) -> dict:
        data = {
            "group_id": f"{group_id}",
            "message": [
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
        return await self.call_api("send_forward_msg", data, timeout=300)

    async def test_forward_single_node(self, group_id: str) -> dict:
        """测试：所有文本塞进一个 node（当前行为）"""
        texts = [f"单节点消息 #{i}" for i in range(1, 6)]
        node_content = [{"type": "text", "data": {"text": t}} for t in texts]
        data = {
            "group_id": f"{group_id}",
            "message": [
                {
                    "type": "node",
                    "data": {
                        "user_id": 2377284392,
                        "nickname": "麦麦",
                        "content": node_content,
                    }
                }
            ],
            "news": [{"text": "单节点测试"}],
            "prompt": "单节点测试",
            "summary": "单节点测试",
            "source": "单节点测试",
        }
        return await self.call_api("send_forward_msg", data, timeout=300)

    async def test_forward_multi_nodes(self, group_id: str) -> dict:
        """测试：每条文本一个独立 node"""
        texts = [f"多节点消息 #{i}" for i in range(1, 6)]
        nodes = []
        for t in texts:
            nodes.append({
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": "麦麦",
                    "content": [{"type": "text", "data": {"text": t}}],
                }
            })
        data = {
            "group_id": f"{group_id}",
            "message": nodes,
            "news": [{"text": "多节点测试"}],
            "prompt": "多节点测试",
            "summary": "多节点测试",
            "source": "多节点测试",
        }
        return await self.call_api("send_forward_msg", data, timeout=300)

    async def test_forward_grouped_nodes(self, group_id: str, per_node: int = 2) -> dict:
        """测试：每 per_node 条文本一组，每组一个 node"""
        texts = [f"分组节点消息 #{i}" for i in range(1, 7)]
        nodes = []
        for i in range(0, len(texts), per_node):
            group = texts[i:i + per_node]
            group_content = [{"type": "text", "data": {"text": t}} for t in group]
            nodes.append({
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": "麦麦",
                    "content": group_content,
                }
            })
        data = {
            "group_id": f"{group_id}",
            "message": nodes,
            "news": [{"text": "分组节点测试"}],
            "prompt": "分组节点测试",
            "summary": "分组节点测试",
            "source": "分组节点测试",
        }
        return await self.call_api("send_forward_msg", data, timeout=300)

    async def send_forward_albums(self, group_id: str, albums: list[dict], tag_name: str) -> None:
        """将专辑列表作为合并转发消息发送，每个专辑一个气泡"""
        jmpath = os.environ['JMBOT_PATH']
        nodes = []
        for album in albums:
            # 解析 detail_json
            authors = ""
            tags = ""
            if album.get("detail_json"):
                try:
                    detail = json.loads(album["detail_json"])
                    author_list = detail.get("authors", [])
                    authors = ", ".join(author_list) if author_list else "未知"
                    tag_list = detail.get("tags", [])
                    tags = ", ".join(tag_list) if tag_list else ""
                except (json.JSONDecodeError, TypeError):
                    pass

            text_parts = [f"名称: {album['name']}", f"作者: {authors}"]
            if tags:
                text_parts.append(f"标签: {tags}")

            content = [
                {"type": "text", "data": {"text": "\n".join(text_parts)}}
            ]

            # 添加封面图
            cover_path = f"{jmpath}/albums/{album['id']}/00001.jpg"
            if os.path.exists(cover_path):
                content.append(
                    {"type": "image", "data": {"file": f"file:///{cover_path}"}}
                )

            nodes.append({
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": "麦麦",
                    "content": content,
                }
            })

        data = {
            "group_id": f"{group_id}",
            "message": nodes,
            "news": [{"text": tag_name}],
            "prompt": tag_name,
            "summary": tag_name,
            "source": tag_name,
        }
        return await self.call_api("send_forward_msg", data, timeout=300)

    def _make_forward_msg_content(self, content: list, title: str, subtitle: str) -> Dict:
        data = {
                "type": "node",
                "data": {
                    "user_id": 2377284392,
                    "nickname": "麦麦",
                    "content": content,
                },
                "news":[{
                    "text": subtitle
                }],
                "prompt": title,
                "summary": title,
                "source": title}
        return data
        
    
    async def send_forward_photos(self, group_id:str, albums: JmAlbumDetail, reprocess: bool = False) -> None:
        # 收集所有图片路径
        jmpath = os.environ['JMBOT_PATH']
        photo_paths = []
        async for photo in file_utils.list_files_iter(f"{jmpath}/albums/{albums.id}"):
            photo_paths.append(photo)

        # 反去重：并行重编码所有图片
        temp_paths = None
        if reprocess:
            temp_paths = await self._reprocess_images(photo_paths)
            photo_paths = temp_paths

        flip_temp_paths = []
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            content = []
            count = 0
            vol = 1
            send_tasks = []  # list of (vol, content, coroutine)
            for photo in photo_paths:
                count += 1
                content.append(
                    {"type": "image", "data": {"file": f"file:///{photo}"}}
                )
                if count == 99:
                    content.append(
                        {"type": "text", "data": {"text": f"{int(time.time())}"}}
                    )
                    logger.info(f"向{group_id}发送:{albums.name} part vol {vol}")
                    subtitle = f"{albums.author}@{timestamp}"
                    send_tasks.append((vol, list(content),
                        self.send_forward_msg(group_id, content, f"vol.{vol} {albums.name}", subtitle)))
                    count = 0
                    vol += 1
                    content = []

            logger.info(f"向{group_id}发送:{albums.name} part vol {vol}")
            subtitle = f"{albums.author}@{timestamp}"
            send_tasks.append((vol, list(content),
                self.send_forward_msg(group_id, content, f"vol.{vol} {albums.name}", subtitle)))

            # 并发等待所有发送
            results = await asyncio.gather(
                *(task for _, _, task in send_tasks),
                return_exceptions=True
            )

            for (v, batch_content, _), result in zip(send_tasks, results):
                failed = isinstance(result, Exception) or (
                    isinstance(result, dict) and result.get("status") != "ok"
                )
                if failed:
                    logger.warning(f"发送合并转发 vol.{v} 失败，尝试翻转图片重试: {result}")
                    retry_result, retry_temps = await self._retry_with_flip(
                        group_id, batch_content, f"vol.{v} {albums.name}", subtitle
                    )
                    flip_temp_paths.extend(retry_temps)
                    if isinstance(retry_result, Exception) or (
                        isinstance(retry_result, dict) and retry_result.get("status") != "ok"
                    ):
                        logger.error(f"翻转重试 vol.{v} 依然失败: {retry_result}")
                        await self.send_group_message(f"发送 vol.{v} 失败", group_id)
                    else:
                        logger.info(f"翻转重试 vol.{v} 成功")
        finally:
            # 清理临时文件
            if temp_paths:
                for p in temp_paths:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
            for p in flip_temp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    async def _retry_with_flip(self, group_id: str, content: list, title: str, subtitle: str) -> tuple:
        """翻转图片后重试发送，返回 (result, flip_temp_paths)"""
        flip_paths = []
        flipped_content = []
        try:
            for item in content:
                if item["type"] == "image":
                    file_path = item["data"]["file"].replace("file:///", "")
                    flipped_path = await asyncio.to_thread(self._flip_single, file_path)
                    flip_paths.append(flipped_path)
                    flipped_content.append(
                        {"type": "image", "data": {"file": f"file:///{flipped_path}"}}
                    )
                else:
                    flipped_content.append(item)

            result = await self.send_forward_msg(group_id, flipped_content, f"[颠倒] {title}", subtitle)
            return result, flip_paths
        except Exception as e:
            return e, flip_paths

    @staticmethod
    def _flip_single(path: str) -> str:
        """同步：翻转单张图片到临时文件"""
        img = Image.open(path)
        img.load()
        flipped = img.transpose(Image.ROTATE_180)
        tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        flipped.save(tmp, format='JPEG', quality=95, optimize=False)
        tmp.close()
        return tmp.name

    async def _reprocess_images(self, paths: list[str]) -> list[str]:
        """并行重编码图片，改变 hash 绕过 QQ 重复图片拦截"""
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            tasks = [loop.run_in_executor(pool, self._reprocess_single, p) for p in paths]
            return await asyncio.gather(*tasks)

    @staticmethod
    def _reprocess_single(path: str) -> str:
        """同步：随机抖动 + 重编码单张 JPEG 到临时文件

        随机修改前16个像素的值（±2），改变 JPEG DCT 系数，
        从而改变文件 hash 绕过 QQ 的重复图片检测。
        抖动幅度 ±2 肉眼完全不可见。
        """
        img = Image.open(path)
        img.load()

        # 随机抖动第一行像素，改变 JPEG 编码结果（hash）
        pixels = img.load()
        w = img.size[0]
        for x in range(min(16, w)):
            r, g, b = pixels[x, 0][:3]
            pixels[x, 0] = (
                (r + random.randint(-2, 2)) & 0xFF,
                (g + random.randint(-2, 2)) & 0xFF,
                (b + random.randint(-2, 2)) & 0xFF,
            )

        tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        try:
            img.save(tmp, format='JPEG', quality=95, optimize=False)
            tmp.close()
            return tmp.name
        except Exception:
            tmp.close()
            os.unlink(tmp.name)
            raise


    async def call_api(self, action: str, params: dict, timeout: float = 30) -> dict:
        """发送 API 请求并等待 websocket 返回响应

        通过 echo 字段将请求与响应配对，支持并发调用。
        """
        self._request_counter += 1
        echo = str(self._request_counter)

        req = {
            "action": action,
            "params": params,
            "echo": echo
        }

        future = self.loop.create_future()
        self._pending_requests[echo] = future

        await self.ws.send(json.dumps(req))

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending_requests.pop(echo, None)

    async def get_group_member_list(self, group_id: str) -> list[dict]:
        """获取群成员列表"""
        response = await self.call_api("get_group_member_list", {"group_id": group_id})
        return response.get("data", [])
        

    async def receive_message(self) -> None:
        while True:
            try:
                msg = await self.ws.recv(decode=True)
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed")
                await asyncio.sleep(30)
                continue

            if not msg:
                continue

            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from message: {msg}")
                continue

            # 检查是否为 API 响应（通过 echo 字段配对）
            echo = data.get("echo")
            if echo is not None:
                future = self._pending_requests.get(str(echo))
                if future and not future.done():
                    if data.get("status") == "ok":
                        future.set_result(data)
                    else:
                        future.set_exception(
                            Exception(f"API error: {data.get('message', data.get('wording', 'unknown'))}")
                        )
                    continue

            # 否则作为事件处理
            logger.info(msg)
            await self.event_queue.put_event(Event("MESSAGE_EVENT", data))
