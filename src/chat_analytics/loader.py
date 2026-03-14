from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import ChatExport, TelegramMessage


def resolve_export_path(path: str | Path) -> Path:
    source_path = Path(path)
    if source_path.is_dir():
        candidate = source_path / "result.json"
        if not candidate.exists():
            raise FileNotFoundError(f"Could not find result.json inside export directory: {source_path}")
        return candidate

    if not source_path.exists():
        raise FileNotFoundError(f"Export path does not exist: {source_path}")

    return source_path


def flatten_text(raw_text: str | list[object]) -> str:
    if isinstance(raw_text, str):
        return raw_text

    parts: list[str] = []
    for item in raw_text:
        if isinstance(item, str):
            parts.append(item)
            continue

        if isinstance(item, dict):
            parts.append(str(item.get("text", "")))
            continue

        parts.append(str(item))

    return "".join(parts)


def parse_message(raw_message: dict[str, Any]) -> TelegramMessage:
    raw_text = raw_message.get("text", "")
    return TelegramMessage(
        id=int(raw_message["id"]),
        message_type=str(raw_message.get("type", "")),
        date=datetime.fromisoformat(str(raw_message["date"])),
        from_name=raw_message.get("from"),
        from_id=raw_message.get("from_id"),
        text=flatten_text(raw_text),
        raw_text=raw_text,
        is_edited="edited" in raw_message,
        reply_to_message_id=int(raw_message["reply_to_message_id"])
        if raw_message.get("reply_to_message_id") is not None
        else None,
        action=raw_message.get("action"),
        actor=raw_message.get("actor"),
        actor_id=raw_message.get("actor_id"),
        title=raw_message.get("title"),
        new_title=raw_message.get("new_title"),
    )


def load_chat_export(path: str | Path) -> ChatExport:
    source_path = resolve_export_path(path)
    with source_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    messages = tuple(parse_message(item) for item in payload.get("messages", []))

    return ChatExport(
        chat_id=payload.get("id"),
        chat_name=str(payload.get("name", source_path.stem)),
        chat_type=str(payload.get("type", "unknown")),
        source_path=source_path,
        messages=messages,
    )
