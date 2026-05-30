import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from event import AlbumQueryCommand
from message_types import GroupMessage


def make_group_message(text: str) -> GroupMessage:
    return GroupMessage.model_validate(
        {
            "post_type": "message",
            "time": 1710000000,
            "user_id": 10001,
            "raw_message": text,
            "message": [{"type": "text", "data": {"text": text}}],
            "message_type": "group",
            "group_id": 20001,
            "sender": {
                "user_id": 10001,
                "nickname": "tester",
                "card": "",
                "role": "member",
            },
        }
    )


class StubBot:
    def __init__(self) -> None:
        self.group_messages: list[tuple[str, str]] = []
        self.forward_messages: list[tuple[str, list[dict], str]] = []

    async def send_group_message(self, message: str, group_id: str) -> None:
        self.group_messages.append((group_id, message))

    async def send_forward_albums(self, group_id: str, albums: list[dict], title: str) -> None:
        self.forward_messages.append((group_id, albums, title))


class StubDatabase:
    def __init__(self, result: list[dict] | None = None) -> None:
        self.result = result or []
        self.calls: list[tuple[str, str]] = []

    async def query_albums(self, query_type: str, keyword: str) -> list[dict]:
        self.calls.append((query_type, keyword))
        return self.result


def run(coro):
    return asyncio.run(coro)


def test_album_query_command_routes_query_types():
    bot = StubBot()
    db = StubDatabase(result=[{"id": "1", "name": "纯爱夏日物语"}])
    command = AlbumQueryCommand(bot, db)

    for text, expected_type, expected_keyword, expected_title in [
        ("query tag 纯爱", "tag", "纯爱", "tag '纯爱' 共有 1 个专辑"),
        ("query title 夏日", "title", "夏日", "标题 '夏日' 共有 1 个专辑"),
        ("query author 空老", "author", "空老", "作者 '空老' 共有 1 个专辑"),
    ]:
        message = make_group_message(text)
        assert command.can_handle(text) is True
        run(command.execute(message, str(message.group_id)))
        assert db.calls[-1] == (expected_type, expected_keyword)
        assert bot.forward_messages[-1][2] == expected_title


def test_album_query_command_reports_empty_result():
    bot = StubBot()
    db = StubDatabase(result=[])
    command = AlbumQueryCommand(bot, db)
    message = make_group_message("query author 不存在")

    run(command.execute(message, str(message.group_id)))

    assert db.calls == [("author", "不存在")]
    assert bot.group_messages == [(str(message.group_id), "作者 '不存在' 下暂无专辑")]
    assert bot.forward_messages == []
