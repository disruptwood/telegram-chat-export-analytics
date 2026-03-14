from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from .aggregation import Period, SUPPORTED_PERIODS, format_bucket_label, period_end, period_start
from .models import TelegramMessage

DEFAULT_CORE_THRESHOLDS: dict[Period, int] = {
    "day": 1,
    "week": 3,
    "month": 5,
    "year": 20,
}


@dataclass(frozen=True, slots=True)
class SenderProfile:
    sender_id: str
    display_name: str
    sender_kind: str
    is_human_like: bool


@dataclass(frozen=True, slots=True)
class SenderDirectoryEntry:
    sender_id: str
    display_name: str
    sender_kind: str
    is_human_like: bool
    total_messages: int
    first_message_at: datetime
    last_message_at: datetime


@dataclass(frozen=True, slots=True)
class ParticipantPeriodSummary:
    period: str
    label: str
    start_date: date
    end_date: date
    total_messages: int
    active_senders: int
    first_time_senders: int
    first_time_sender_share: float
    core_threshold: int
    core_senders: int
    new_core_senders: int
    retained_core_senders: int
    reactivated_core_senders: int
    top3_share: float
    top10_share: float
    messages_per_active_sender: float
    unique_senders_per_100_messages: float
    effective_senders: float
    breadth_ratio: float
    concentration_hhi: float


@dataclass(frozen=True, slots=True)
class TopSenderPeriodRow:
    period: str
    label: str
    start_date: date
    end_date: date
    rank: int
    sender_id: str
    display_name: str
    sender_kind: str
    message_count: int
    share_of_messages: float
    is_first_time_sender: bool
    is_core_sender: bool


def sender_kind(sender_id: str | None) -> str:
    if sender_id is None:
        return "unknown"
    if sender_id.startswith("user"):
        return "user"
    if sender_id.startswith("channel"):
        return "channel"
    if sender_id.startswith("chat"):
        return "chat"
    return "other"


def canonical_sender_id(message: TelegramMessage) -> str:
    if message.from_id:
        return message.from_id
    if message.from_name:
        return f"name:{message.from_name}"
    return f"unknown:{message.id}"


def is_human_like_sender(message: TelegramMessage) -> bool:
    return sender_kind(message.from_id) == "user"


def filter_messages_for_participants(
    messages: Iterable[TelegramMessage],
    *,
    include_non_human: bool = False,
) -> list[TelegramMessage]:
    filtered = [message for message in messages if message.is_user_message]
    if include_non_human:
        return filtered
    return [message for message in filtered if is_human_like_sender(message)]


def collect_sender_profiles(messages: Iterable[TelegramMessage]) -> dict[str, SenderProfile]:
    profiles: dict[str, SenderProfile] = {}
    for message in sorted(messages, key=lambda item: (item.date, item.id)):
        sender_id_value = canonical_sender_id(message)
        current = profiles.get(sender_id_value)
        display_name = message.from_name or (current.display_name if current else sender_id_value)
        kind = sender_kind(message.from_id)
        profiles[sender_id_value] = SenderProfile(
            sender_id=sender_id_value,
            display_name=display_name,
            sender_kind=kind,
            is_human_like=kind == "user",
        )
    return profiles


def build_sender_directory(
    messages: Iterable[TelegramMessage],
    *,
    include_non_human: bool = True,
) -> list[SenderDirectoryEntry]:
    filtered_messages = filter_messages_for_participants(
        messages,
        include_non_human=include_non_human,
    )
    profiles = collect_sender_profiles(filtered_messages)
    message_groups: dict[str, list[TelegramMessage]] = defaultdict(list)
    for message in filtered_messages:
        message_groups[canonical_sender_id(message)].append(message)

    rows = [
        SenderDirectoryEntry(
            sender_id=sender_id_value,
            display_name=profiles[sender_id_value].display_name,
            sender_kind=profiles[sender_id_value].sender_kind,
            is_human_like=profiles[sender_id_value].is_human_like,
            total_messages=len(sender_messages),
            first_message_at=min(item.date for item in sender_messages),
            last_message_at=max(item.date for item in sender_messages),
        )
        for sender_id_value, sender_messages in message_groups.items()
    ]
    return sorted(rows, key=lambda row: (-row.total_messages, row.display_name.casefold(), row.sender_id))


def summarize_participants_by_period(
    messages: Iterable[TelegramMessage],
    period: Period,
    *,
    include_non_human: bool = False,
    top_n: int = 10,
    core_threshold: int | None = None,
) -> tuple[list[ParticipantPeriodSummary], list[TopSenderPeriodRow]]:
    if period not in SUPPORTED_PERIODS:
        raise ValueError(f"Unsupported period: {period}")

    filtered_messages = filter_messages_for_participants(
        messages,
        include_non_human=include_non_human,
    )
    if not filtered_messages:
        return [], []

    profiles = collect_sender_profiles(filtered_messages)
    counts_by_period: dict[date, Counter[str]] = defaultdict(Counter)
    first_period_by_sender: dict[str, date] = {}
    core_threshold_value = core_threshold or DEFAULT_CORE_THRESHOLDS[period]

    for message in sorted(filtered_messages, key=lambda item: (item.date, item.id)):
        sender_id_value = canonical_sender_id(message)
        bucket_start = period_start(message.date.date(), period)
        counts_by_period[bucket_start][sender_id_value] += 1
        first_period_by_sender.setdefault(sender_id_value, bucket_start)

    summaries: list[ParticipantPeriodSummary] = []
    top_rows: list[TopSenderPeriodRow] = []
    previous_core_senders: set[str] = set()
    ever_core_senders: set[str] = set()

    for bucket_start in sorted(counts_by_period):
        sender_counts = counts_by_period[bucket_start]
        total_messages = sum(sender_counts.values())
        active_senders = len(sender_counts)
        first_time_senders = {
            sender_id_value
            for sender_id_value in sender_counts
            if first_period_by_sender[sender_id_value] == bucket_start
        }
        core_senders = {
            sender_id_value
            for sender_id_value, message_count in sender_counts.items()
            if message_count >= core_threshold_value
        }
        new_core_senders = {
            sender_id_value
            for sender_id_value in core_senders
            if sender_id_value not in ever_core_senders
        }
        retained_core_senders = core_senders & previous_core_senders
        reactivated_core_senders = {
            sender_id_value
            for sender_id_value in core_senders
            if sender_id_value in ever_core_senders and sender_id_value not in previous_core_senders
        }

        sorted_senders = sorted(
            sender_counts.items(),
            key=lambda item: (
                -item[1],
                profiles[item[0]].display_name.casefold(),
                item[0],
            ),
        )
        top3_share = sum(message_count for _, message_count in sorted_senders[:3]) / total_messages
        top10_share = sum(message_count for _, message_count in sorted_senders[:10]) / total_messages
        concentration_hhi = sum((message_count / total_messages) ** 2 for _, message_count in sorted_senders)
        effective_senders = 1 / concentration_hhi if concentration_hhi else 0.0
        breadth_ratio = effective_senders / active_senders if active_senders else 0.0

        summaries.append(
            ParticipantPeriodSummary(
                period=period,
                label=format_bucket_label(bucket_start, period),
                start_date=bucket_start,
                end_date=period_end(bucket_start, period),
                total_messages=total_messages,
                active_senders=active_senders,
                first_time_senders=len(first_time_senders),
                first_time_sender_share=len(first_time_senders) / active_senders if active_senders else 0.0,
                core_threshold=core_threshold_value,
                core_senders=len(core_senders),
                new_core_senders=len(new_core_senders),
                retained_core_senders=len(retained_core_senders),
                reactivated_core_senders=len(reactivated_core_senders),
                top3_share=top3_share,
                top10_share=top10_share,
                messages_per_active_sender=total_messages / active_senders if active_senders else 0.0,
                unique_senders_per_100_messages=active_senders / total_messages * 100 if total_messages else 0.0,
                effective_senders=effective_senders,
                breadth_ratio=breadth_ratio,
                concentration_hhi=concentration_hhi,
            )
        )

        for rank, (sender_id_value, message_count) in enumerate(sorted_senders[:top_n], start=1):
            top_rows.append(
                TopSenderPeriodRow(
                    period=period,
                    label=format_bucket_label(bucket_start, period),
                    start_date=bucket_start,
                    end_date=period_end(bucket_start, period),
                    rank=rank,
                    sender_id=sender_id_value,
                    display_name=profiles[sender_id_value].display_name,
                    sender_kind=profiles[sender_id_value].sender_kind,
                    message_count=message_count,
                    share_of_messages=message_count / total_messages if total_messages else 0.0,
                    is_first_time_sender=sender_id_value in first_time_senders,
                    is_core_sender=sender_id_value in core_senders,
                )
            )

        previous_core_senders = core_senders
        ever_core_senders.update(core_senders)

    return summaries, top_rows
