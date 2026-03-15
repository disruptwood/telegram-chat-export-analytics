from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Reaction:
    emoji: str
    count: int


@dataclass(frozen=True, slots=True)
class TelegramMessage:
    id: int
    message_type: str
    date: datetime
    from_name: str | None
    from_id: str | None
    text: str
    raw_text: str | list[object]
    is_edited: bool
    reply_to_message_id: int | None
    action: str | None
    actor: str | None
    actor_id: str | None
    title: str | None
    new_title: str | None
    reactions: tuple[Reaction, ...] = ()

    @property
    def is_user_message(self) -> bool:
        return self.message_type == "message"

    @property
    def total_reactions(self) -> int:
        return sum(r.count for r in self.reactions)


@dataclass(frozen=True, slots=True)
class ChatExport:
    chat_id: int | None
    chat_name: str
    chat_type: str
    source_path: Path
    messages: tuple[TelegramMessage, ...]


@dataclass(frozen=True, slots=True)
class CountBucket:
    period: str
    label: str
    start_date: date
    end_date: date
    message_count: int
