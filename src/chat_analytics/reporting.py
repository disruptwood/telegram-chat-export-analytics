from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from .aggregation import SUPPORTED_PERIODS, count_messages_by_period
from .models import ChatExport, CountBucket
from .participants import (
    ParticipantPeriodSummary,
    SenderDirectoryEntry,
    TopSenderPeriodRow,
    build_sender_directory,
    summarize_participants_by_period,
)


def write_counts_csv(rows: list[CountBucket], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["period", "label", "start_date", "end_date", "message_count"])
        for row in rows:
            writer.writerow(
                [
                    row.period,
                    row.label,
                    row.start_date.isoformat(),
                    row.end_date.isoformat(),
                    row.message_count,
                ]
            )

    return destination_path


def build_summary_markdown(chat_export: ChatExport, *, include_service: bool = False) -> str:
    total_messages = len(chat_export.messages)
    user_messages = [message for message in chat_export.messages if message.is_user_message]
    service_messages = total_messages - len(user_messages)
    unique_senders = len({message.from_id for message in user_messages if message.from_id})

    if user_messages:
        first_message = min(user_messages, key=lambda item: item.date)
        last_message = max(user_messages, key=lambda item: item.date)
    else:
        first_message = None
        last_message = None

    month_rows = count_messages_by_period(
        chat_export.messages,
        "month",
        include_service=include_service,
    )
    year_rows = count_messages_by_period(
        chat_export.messages,
        "year",
        include_service=include_service,
    )
    top_months = sorted(month_rows, key=lambda row: row.message_count, reverse=True)[:5]

    lines = [
        "# Chat Summary",
        "",
        f"- Chat: {chat_export.chat_name}",
        f"- Source: {chat_export.source_path.name}",
        f"- Total events in export: {total_messages}",
        f"- User messages: {len(user_messages)}",
        f"- Service events: {service_messages}",
        f"- Unique senders: {unique_senders}",
        f"- Counting mode: {'all events' if include_service else 'user messages only'}",
    ]

    if first_message and last_message:
        lines.extend(
            [
                f"- First user message: {first_message.date.isoformat(sep=' ')}",
                f"- Last user message: {last_message.date.isoformat(sep=' ')}",
            ]
        )

    lines.extend(["", "## Messages by Year", "", "| Year | Messages |", "| --- | ---: |"])
    for row in year_rows:
        lines.append(f"| {row.label} | {row.message_count} |")

    lines.extend(["", "## Top 5 Months", "", "| Month | Messages |", "| --- | ---: |"])
    for row in top_months:
        lines.append(f"| {row.label} | {row.message_count} |")

    lines.extend(["", "## Generated Files", ""])
    for period in SUPPORTED_PERIODS:
        lines.append(f"- `message_counts_{period}.csv`")

    return "\n".join(lines) + "\n"


def write_summary_markdown(
    chat_export: ChatExport,
    destination: str | Path,
    *,
    include_service: bool = False,
) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(
        build_summary_markdown(chat_export, include_service=include_service),
        encoding="utf-8",
    )
    return destination_path


def write_participant_summary_csv(rows: list[ParticipantPeriodSummary], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "period",
                "label",
                "start_date",
                "end_date",
                "total_messages",
                "active_senders",
                "first_time_senders",
                "first_time_sender_share",
                "core_threshold",
                "core_senders",
                "new_core_senders",
                "retained_core_senders",
                "reactivated_core_senders",
                "top3_share",
                "top10_share",
                "messages_per_active_sender",
                "unique_senders_per_100_messages",
                "effective_senders",
                "breadth_ratio",
                "concentration_hhi",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.period,
                    row.label,
                    row.start_date.isoformat(),
                    row.end_date.isoformat(),
                    row.total_messages,
                    row.active_senders,
                    row.first_time_senders,
                    f"{row.first_time_sender_share:.6f}",
                    row.core_threshold,
                    row.core_senders,
                    row.new_core_senders,
                    row.retained_core_senders,
                    row.reactivated_core_senders,
                    f"{row.top3_share:.6f}",
                    f"{row.top10_share:.6f}",
                    f"{row.messages_per_active_sender:.6f}",
                    f"{row.unique_senders_per_100_messages:.6f}",
                    f"{row.effective_senders:.6f}",
                    f"{row.breadth_ratio:.6f}",
                    f"{row.concentration_hhi:.6f}",
                ]
            )
    return destination_path


def write_top_sender_rows_csv(rows: list[TopSenderPeriodRow], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "period",
                "label",
                "start_date",
                "end_date",
                "rank",
                "sender_id",
                "display_name",
                "sender_kind",
                "message_count",
                "share_of_messages",
                "is_first_time_sender",
                "is_core_sender",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.period,
                    row.label,
                    row.start_date.isoformat(),
                    row.end_date.isoformat(),
                    row.rank,
                    row.sender_id,
                    row.display_name,
                    row.sender_kind,
                    row.message_count,
                    f"{row.share_of_messages:.6f}",
                    int(row.is_first_time_sender),
                    int(row.is_core_sender),
                ]
            )
    return destination_path


def write_top_sender_rows_json(
    summaries: list[ParticipantPeriodSummary],
    rows: list[TopSenderPeriodRow],
    destination: str | Path,
) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    rows_by_label: dict[str, list[TopSenderPeriodRow]] = {}
    for row in rows:
        rows_by_label.setdefault(row.label, []).append(row)

    payload = [
        {
            "period": summary.period,
            "label": summary.label,
            "start_date": summary.start_date.isoformat(),
            "end_date": summary.end_date.isoformat(),
            "total_messages": summary.total_messages,
            "active_senders": summary.active_senders,
            "core_threshold": summary.core_threshold,
            "core_senders": summary.core_senders,
            "first_time_senders": summary.first_time_senders,
            "top_senders": [
                {
                    "rank": row.rank,
                    "sender_id": row.sender_id,
                    "display_name": row.display_name,
                    "sender_kind": row.sender_kind,
                    "message_count": row.message_count,
                    "share_of_messages": row.share_of_messages,
                    "is_first_time_sender": row.is_first_time_sender,
                    "is_core_sender": row.is_core_sender,
                }
                for row in sorted(rows_by_label.get(summary.label, []), key=lambda item: item.rank)
            ],
        }
        for summary in summaries
    ]
    destination_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return destination_path


def write_sender_directory_csv(rows: list[SenderDirectoryEntry], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "sender_id",
                "display_name",
                "sender_kind",
                "is_human_like",
                "total_messages",
                "first_message_at",
                "last_message_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.sender_id,
                    row.display_name,
                    row.sender_kind,
                    int(row.is_human_like),
                    row.total_messages,
                    row.first_message_at.isoformat(sep=" "),
                    row.last_message_at.isoformat(sep=" "),
                ]
            )
    return destination_path


def build_participant_report_markdown(
    chat_export: ChatExport,
    *,
    include_non_human: bool = False,
    top_n: int = 10,
) -> str:
    human_directory = build_sender_directory(chat_export.messages, include_non_human=False)
    all_directory = build_sender_directory(chat_export.messages, include_non_human=True)
    excluded_senders = [row for row in all_directory if not row.is_human_like]
    excluded_messages = sum(row.total_messages for row in excluded_senders)
    human_messages = sum(row.total_messages for row in human_directory)

    year_summaries, _ = summarize_participants_by_period(
        chat_export.messages,
        "year",
        include_non_human=include_non_human,
        top_n=top_n,
    )
    month_summaries, _ = summarize_participants_by_period(
        chat_export.messages,
        "month",
        include_non_human=include_non_human,
        top_n=top_n,
    )
    top_all_time = human_directory[:10]
    widest_months = sorted(month_summaries, key=lambda row: row.breadth_ratio, reverse=True)[:5]
    deepest_months = sorted(month_summaries, key=lambda row: row.breadth_ratio)[:5]
    newest_core_months = sorted(month_summaries, key=lambda row: row.new_core_senders, reverse=True)[:5]

    lines = [
        "# Participant Report",
        "",
        "## Scope",
        "",
        f"- Included messages: {human_messages} human-like messages from senders with `from_id` starting with `user`.",
        f"- Excluded senders by default: {len(excluded_senders)} non-user senders with {excluded_messages} messages total.",
        "- Important limitation: Telegram export here does not expose a reliable bot flag for user accounts.",
        "- Because of that, the default filter excludes channels and other non-user senders, but may still keep user accounts that are actually bots.",
        "",
        "## Yearly Overview",
        "",
        "| Year | Messages | Active senders | First-time senders | Core senders | New core senders | Top10 share | Breadth ratio |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in year_summaries:
        lines.append(
            f"| {row.label} | {row.total_messages} | {row.active_senders} | {row.first_time_senders} | "
            f"{row.core_senders} | {row.new_core_senders} | {row.top10_share * 100:.1f}% | {row.breadth_ratio:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Top 10 All-Time Human-Like Senders",
            "",
            "| Rank | Sender | Messages |",
            "| --- | --- | ---: |",
        ]
    )
    for rank, row in enumerate(top_all_time, start=1):
        lines.append(f"| {rank} | {row.display_name} | {row.total_messages} |")

    lines.extend(
        [
            "",
            "## Widest Months",
            "",
            "| Month | Messages | Active senders | Top10 share | Breadth ratio |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in widest_months:
        lines.append(
            f"| {row.label} | {row.total_messages} | {row.active_senders} | {row.top10_share * 100:.1f}% | {row.breadth_ratio:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Deepest / Most Concentrated Months",
            "",
            "| Month | Messages | Active senders | Top10 share | Breadth ratio |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in deepest_months:
        lines.append(
            f"| {row.label} | {row.total_messages} | {row.active_senders} | {row.top10_share * 100:.1f}% | {row.breadth_ratio:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Months With Most New Core Senders",
            "",
            "| Month | New core senders | Core senders | First-time senders |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for row in newest_core_months:
        lines.append(
            f"| {row.label} | {row.new_core_senders} | {row.core_senders} | {row.first_time_senders} |"
        )

    lines.extend(
        [
            "",
            "## Metric Notes",
            "",
            "- `active_senders`: distinct included senders with at least one message in the period.",
            "- `first_time_senders`: included senders whose first-ever message in the export falls in the period.",
            "- `core_senders`: senders above a fixed threshold (`day=1`, `week=3`, `month=5`, `year=20`).",
            "- `new_core_senders`: senders who reach the core threshold for the first time ever in that period.",
            "- `top10_share`: share of messages written by the top 10 senders in the period.",
            "- `breadth_ratio`: effective number of senders divided by active senders; closer to 1 means broader participation, closer to 0 means concentration in a few people.",
        ]
    )

    return "\n".join(lines) + "\n"


def write_participant_report_markdown(
    chat_export: ChatExport,
    destination: str | Path,
    *,
    include_non_human: bool = False,
    top_n: int = 10,
) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(
        build_participant_report_markdown(
            chat_export,
            include_non_human=include_non_human,
            top_n=top_n,
        ),
        encoding="utf-8",
    )
    return destination_path
