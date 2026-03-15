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
from .reactions import (
    DistributionStats,
    ReactionStabilityProfile,
    SenderReactionPeriodRow,
    SenderReactionProfile,
    TopReactedMessage,
    compute_message_distribution,
    compute_reaction_stability,
    compute_sender_reaction_by_period,
    compute_sender_reaction_profiles,
    filter_senders_by_percentile,
    filter_senders_top_n,
    get_top_reacted_messages,
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


# ---------------------------------------------------------------------------
# Reaction report writers
# ---------------------------------------------------------------------------


def write_reaction_profiles_csv(rows: list[SenderReactionProfile], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "sender_id", "display_name", "total_messages", "total_chars",
            "total_reactions", "reactions_per_message", "reactions_per_1k_chars",
            "median_reactions", "messages_with_reactions", "messages_with_reactions_share",
        ])
        for row in rows:
            writer.writerow([
                row.sender_id, row.display_name, row.total_messages, row.total_chars,
                row.total_reactions, f"{row.reactions_per_message:.4f}",
                f"{row.reactions_per_1k_chars:.4f}", f"{row.median_reactions:.1f}",
                row.messages_with_reactions, f"{row.messages_with_reactions_share:.4f}",
            ])
    return destination_path


def write_reaction_by_period_csv(rows: list[SenderReactionPeriodRow], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "period", "label", "start_date", "end_date",
            "sender_id", "display_name", "messages", "total_reactions",
            "reactions_per_message", "reactions_per_1k_chars", "messages_with_reactions",
        ])
        for row in rows:
            writer.writerow([
                row.period, row.label, row.start_date.isoformat(), row.end_date.isoformat(),
                row.sender_id, row.display_name, row.messages, row.total_reactions,
                f"{row.reactions_per_message:.4f}", f"{row.reactions_per_1k_chars:.4f}",
                row.messages_with_reactions,
            ])
    return destination_path


def write_reaction_stability_csv(rows: list[ReactionStabilityProfile], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "sender_id", "display_name", "periods_active", "periods_with_reactions",
            "mean_reactions_per_message", "std_reactions_per_message", "cv_reactions_per_message",
            "min_reactions_per_message", "max_reactions_per_message",
            "median_reactions_per_message", "consistency_score",
        ])
        for row in rows:
            writer.writerow([
                row.sender_id, row.display_name, row.periods_active, row.periods_with_reactions,
                f"{row.mean_reactions_per_message:.4f}", f"{row.std_reactions_per_message:.4f}",
                f"{row.cv_reactions_per_message:.4f}", f"{row.min_reactions_per_message:.4f}",
                f"{row.max_reactions_per_message:.4f}", f"{row.median_reactions_per_message:.4f}",
                f"{row.consistency_score:.4f}",
            ])
    return destination_path


def write_top_reacted_messages_csv(rows: list[TopReactedMessage], destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "message_id", "date", "sender_id", "display_name",
            "text_preview", "total_reactions", "reaction_breakdown",
        ])
        for row in rows:
            breakdown_str = " ".join(f"{emoji}:{count}" for emoji, count in row.reaction_breakdown)
            writer.writerow([
                row.message_id, row.date, row.sender_id, row.display_name,
                row.text_preview, row.total_reactions, breakdown_str,
            ])
    return destination_path


def build_reaction_report_markdown(
    chat_export: ChatExport,
    *,
    include_non_human: bool = False,
    top_n_senders: int = 30,
    top_n_messages: int = 30,
    stability_period: str = "month",
    min_percentile: float = 0.0,
    min_periods: int = 3,
) -> str:
    msgs = list(chat_export.messages)

    # Determine allowed senders
    allowed = filter_senders_top_n(
        [m for m in msgs if m.is_user_message],
        top_n_senders,
    )

    dist = compute_message_distribution(
        [m for m in msgs if m.is_user_message],
    )

    profiles = compute_sender_reaction_profiles(
        msgs, include_non_human=include_non_human, allowed_senders=allowed,
    )

    stability = compute_reaction_stability(
        msgs, stability_period,
        include_non_human=include_non_human, allowed_senders=allowed,
        min_periods=min_periods,
    )

    top_messages = get_top_reacted_messages(
        msgs, include_non_human=include_non_human, top_n=top_n_messages,
    )

    lines = [
        "# Reaction Analytics Report",
        "",
        f"- Chat: {chat_export.chat_name}",
        f"- Senders analyzed: top {top_n_senders} by message count",
        f"- Stability period: {stability_period} (min {min_periods} periods)",
        "",
        "## Message Count Distribution (all senders)",
        "",
        f"- Senders: {dist.count}",
        f"- Mean messages: {dist.mean:.1f}",
        f"- Std: {dist.std:.1f}",
        f"- Min: {dist.min:.0f}, P25: {dist.p25:.0f}, Median: {dist.median:.0f}, P75: {dist.p75:.0f}, P90: {dist.p90:.0f}, P95: {dist.p95:.0f}, Max: {dist.max:.0f}",
        "",
        "## Top Senders by Reactions per Message",
        "",
        "| Rank | Sender | Messages | Reactions | R/Msg | R/1kChars | Median R | % With R |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for rank, p in enumerate(profiles[:20], start=1):
        lines.append(
            f"| {rank} | {p.display_name} | {p.total_messages} | {p.total_reactions} | "
            f"{p.reactions_per_message:.2f} | {p.reactions_per_1k_chars:.2f} | "
            f"{p.median_reactions:.1f} | {p.messages_with_reactions_share * 100:.1f}% |"
        )

    if stability:
        lines.extend([
            "",
            f"## Reaction Stability ({stability_period}ly, min {min_periods} periods)",
            "",
            "| Rank | Sender | Periods | Mean R/Msg | Std | CV | Min | Max | Consistency |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for rank, s in enumerate(stability[:20], start=1):
            lines.append(
                f"| {rank} | {s.display_name} | {s.periods_active} | "
                f"{s.mean_reactions_per_message:.2f} | {s.std_reactions_per_message:.2f} | "
                f"{s.cv_reactions_per_message:.2f} | {s.min_reactions_per_message:.2f} | "
                f"{s.max_reactions_per_message:.2f} | {s.consistency_score:.3f} |"
            )

    if top_messages:
        lines.extend([
            "",
            f"## Top {min(len(top_messages), top_n_messages)} Most Reacted Messages",
            "",
            "| # | Date | Sender | Reactions | Breakdown | Text |",
            "| ---: | --- | --- | ---: | --- | --- |",
        ])
        for rank, m in enumerate(top_messages[:top_n_messages], start=1):
            breakdown = " ".join(f"{e}x{c}" for e, c in m.reaction_breakdown[:5])
            preview = m.text_preview[:80].replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {rank} | {m.date[:10]} | {m.display_name} | {m.total_reactions} | {breakdown} | {preview} |"
            )

    lines.extend([
        "",
        "## Metric Notes",
        "",
        "- `R/Msg`: total reactions divided by total messages for the sender.",
        "- `R/1kChars`: total reactions per 1,000 characters of text written.",
        "- `Median R`: median number of reactions per message.",
        "- `% With R`: share of messages that received at least one reaction.",
        "- `CV`: coefficient of variation (std / mean); lower means more consistent.",
        "- `Consistency`: `mean / (1 + CV)`; rewards high reaction rates with low variance.",
        "- Only senders in the top N by message count are included to filter out low-activity outliers.",
    ])

    return "\n".join(lines) + "\n"


def write_reaction_report_markdown(
    chat_export: ChatExport,
    destination: str | Path,
    *,
    include_non_human: bool = False,
    top_n_senders: int = 30,
    top_n_messages: int = 30,
    stability_period: str = "month",
    min_percentile: float = 0.0,
    min_periods: int = 3,
) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(
        build_reaction_report_markdown(
            chat_export,
            include_non_human=include_non_human,
            top_n_senders=top_n_senders,
            top_n_messages=top_n_messages,
            stability_period=stability_period,
            min_percentile=min_percentile,
            min_periods=min_periods,
        ),
        encoding="utf-8",
    )
    return destination_path
