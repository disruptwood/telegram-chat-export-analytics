from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Iterable, Literal

from .models import CountBucket, TelegramMessage

Period = Literal["day", "week", "month", "year"]
SUPPORTED_PERIODS: tuple[Period, ...] = ("day", "week", "month", "year")


def filter_messages(
    messages: Iterable[TelegramMessage],
    *,
    include_service: bool = False,
) -> list[TelegramMessage]:
    if include_service:
        return list(messages)

    return [message for message in messages if message.is_user_message]


def period_start(value: date, period: Period) -> date:
    if period == "day":
        return value
    if period == "week":
        return value - timedelta(days=value.weekday())
    if period == "month":
        return value.replace(day=1)
    if period == "year":
        return value.replace(month=1, day=1)
    raise ValueError(f"Unsupported period: {period}")


def period_end(start: date, period: Period) -> date:
    if period == "day":
        return start
    if period == "week":
        return start + timedelta(days=6)
    if period == "month":
        if start.month == 12:
            return date(start.year + 1, 1, 1) - timedelta(days=1)
        return date(start.year, start.month + 1, 1) - timedelta(days=1)
    if period == "year":
        return date(start.year, 12, 31)
    raise ValueError(f"Unsupported period: {period}")


def format_bucket_label(start: date, period: Period) -> str:
    if period == "day":
        return start.isoformat()
    if period == "week":
        iso_year, iso_week, _ = start.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    if period == "month":
        return f"{start.year:04d}-{start.month:02d}"
    if period == "year":
        return f"{start.year:04d}"
    raise ValueError(f"Unsupported period: {period}")


def count_messages_by_period(
    messages: Iterable[TelegramMessage],
    period: Period,
    *,
    include_service: bool = False,
) -> list[CountBucket]:
    if period not in SUPPORTED_PERIODS:
        raise ValueError(f"Unsupported period: {period}")

    filtered_messages = filter_messages(
        messages,
        include_service=include_service,
    )
    counts = Counter(period_start(message.date.date(), period) for message in filtered_messages)

    return [
        CountBucket(
            period=period,
            label=format_bucket_label(start, period),
            start_date=start,
            end_date=period_end(start, period),
            message_count=counts[start],
        )
        for start in sorted(counts)
    ]
