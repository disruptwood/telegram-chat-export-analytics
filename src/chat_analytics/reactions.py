from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Literal

from .aggregation import Period, SUPPORTED_PERIODS, format_bucket_label, period_end, period_start
from .models import TelegramMessage
from .participants import (
    canonical_sender_id,
    collect_sender_profiles,
    filter_messages_for_participants,
    SenderProfile,
)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

SenderFilter = Literal["top_n", "percentile"]


@dataclass(frozen=True, slots=True)
class SenderReactionProfile:
    sender_id: str
    display_name: str
    total_messages: int
    total_chars: int
    total_reactions: int
    reactions_per_message: float
    reactions_per_1k_chars: float
    median_reactions: float
    messages_with_reactions: int
    messages_with_reactions_share: float


@dataclass(frozen=True, slots=True)
class SenderReactionPeriodRow:
    period: str
    label: str
    start_date: date
    end_date: date
    sender_id: str
    display_name: str
    messages: int
    total_reactions: int
    reactions_per_message: float
    reactions_per_1k_chars: float
    messages_with_reactions: int


@dataclass(frozen=True, slots=True)
class ReactionStabilityProfile:
    sender_id: str
    display_name: str
    periods_active: int
    periods_with_reactions: int
    mean_reactions_per_message: float
    std_reactions_per_message: float
    cv_reactions_per_message: float
    min_reactions_per_message: float
    max_reactions_per_message: float
    median_reactions_per_message: float
    consistency_score: float


@dataclass(frozen=True, slots=True)
class TopReactedMessage:
    message_id: int
    date: str
    sender_id: str
    display_name: str
    text_preview: str
    total_reactions: int
    reaction_breakdown: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class DistributionStats:
    count: int
    mean: float
    std: float
    min: float
    p25: float
    median: float
    p75: float
    p90: float
    p95: float
    max: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return s[mid]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[f]
    return s[f] + (k - f) * (s[c] - s[f])


def _std(values: list[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))


# ---------------------------------------------------------------------------
# Sender filtering
# ---------------------------------------------------------------------------

def compute_message_distribution(
    messages: Iterable[TelegramMessage],
) -> DistributionStats:
    counts_by_sender: Counter[str] = Counter()
    for msg in messages:
        counts_by_sender[canonical_sender_id(msg)] += 1

    values = sorted(counts_by_sender.values())
    if not values:
        return DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    n = len(values)
    fvalues = [float(v) for v in values]
    mean = sum(fvalues) / n
    return DistributionStats(
        count=n,
        mean=mean,
        std=_std(fvalues, mean),
        min=fvalues[0],
        p25=_percentile(fvalues, 0.25),
        median=_percentile(fvalues, 0.50),
        p75=_percentile(fvalues, 0.75),
        p90=_percentile(fvalues, 0.90),
        p95=_percentile(fvalues, 0.95),
        max=fvalues[-1],
    )


def filter_senders_top_n(
    messages: list[TelegramMessage],
    top_n: int,
) -> set[str]:
    counts: Counter[str] = Counter()
    for msg in messages:
        counts[canonical_sender_id(msg)] += 1
    return {sid for sid, _ in counts.most_common(top_n)}


def filter_senders_by_percentile(
    messages: list[TelegramMessage],
    min_percentile: float = 0.0,
    max_percentile: float = 1.0,
) -> set[str]:
    counts: Counter[str] = Counter()
    for msg in messages:
        counts[canonical_sender_id(msg)] += 1

    values = sorted(counts.values())
    if not values:
        return set()

    lo = _percentile([float(v) for v in values], min_percentile)
    hi = _percentile([float(v) for v in values], max_percentile)
    return {sid for sid, cnt in counts.items() if lo <= cnt <= hi}


def filter_senders_min_messages(
    messages: list[TelegramMessage],
    min_messages: int,
) -> set[str]:
    counts: Counter[str] = Counter()
    for msg in messages:
        counts[canonical_sender_id(msg)] += 1
    return {sid for sid, cnt in counts.items() if cnt >= min_messages}


# ---------------------------------------------------------------------------
# Core analytics
# ---------------------------------------------------------------------------

def compute_sender_reaction_profiles(
    messages: Iterable[TelegramMessage],
    *,
    include_non_human: bool = False,
    allowed_senders: set[str] | None = None,
) -> list[SenderReactionProfile]:
    filtered = filter_messages_for_participants(messages, include_non_human=include_non_human)
    profiles = collect_sender_profiles(filtered)

    by_sender: dict[str, list[TelegramMessage]] = defaultdict(list)
    for msg in filtered:
        sid = canonical_sender_id(msg)
        if allowed_senders is not None and sid not in allowed_senders:
            continue
        by_sender[sid].append(msg)

    result: list[SenderReactionProfile] = []
    for sid, sender_msgs in by_sender.items():
        total_messages = len(sender_msgs)
        total_chars = sum(len(m.text) for m in sender_msgs)
        reaction_counts = [m.total_reactions for m in sender_msgs]
        total_reactions = sum(reaction_counts)
        msgs_with_reactions = sum(1 for r in reaction_counts if r > 0)

        rpm = total_reactions / total_messages if total_messages else 0.0
        rpk = total_reactions / (total_chars / 1000.0) if total_chars else 0.0
        med = _median([float(r) for r in reaction_counts])
        share = msgs_with_reactions / total_messages if total_messages else 0.0

        profile = profiles.get(sid)
        name = profile.display_name if profile else sid

        result.append(SenderReactionProfile(
            sender_id=sid,
            display_name=name,
            total_messages=total_messages,
            total_chars=total_chars,
            total_reactions=total_reactions,
            reactions_per_message=rpm,
            reactions_per_1k_chars=rpk,
            median_reactions=med,
            messages_with_reactions=msgs_with_reactions,
            messages_with_reactions_share=share,
        ))

    return sorted(result, key=lambda r: (-r.reactions_per_message, r.display_name.casefold()))


def compute_sender_reaction_by_period(
    messages: Iterable[TelegramMessage],
    period: Period,
    *,
    include_non_human: bool = False,
    allowed_senders: set[str] | None = None,
) -> list[SenderReactionPeriodRow]:
    if period not in SUPPORTED_PERIODS:
        raise ValueError(f"Unsupported period: {period}")

    filtered = filter_messages_for_participants(messages, include_non_human=include_non_human)
    profiles = collect_sender_profiles(filtered)

    buckets: dict[tuple[date, str], list[TelegramMessage]] = defaultdict(list)
    for msg in filtered:
        sid = canonical_sender_id(msg)
        if allowed_senders is not None and sid not in allowed_senders:
            continue
        bucket_start = period_start(msg.date.date(), period)
        buckets[(bucket_start, sid)].append(msg)

    rows: list[SenderReactionPeriodRow] = []
    for (bstart, sid), msgs in sorted(buckets.items()):
        total_msgs = len(msgs)
        total_chars = sum(len(m.text) for m in msgs)
        total_rx = sum(m.total_reactions for m in msgs)
        msgs_with_rx = sum(1 for m in msgs if m.total_reactions > 0)

        profile = profiles.get(sid)
        name = profile.display_name if profile else sid

        rows.append(SenderReactionPeriodRow(
            period=period,
            label=format_bucket_label(bstart, period),
            start_date=bstart,
            end_date=period_end(bstart, period),
            sender_id=sid,
            display_name=name,
            messages=total_msgs,
            total_reactions=total_rx,
            reactions_per_message=total_rx / total_msgs if total_msgs else 0.0,
            reactions_per_1k_chars=total_rx / (total_chars / 1000.0) if total_chars else 0.0,
            messages_with_reactions=msgs_with_rx,
        ))

    return rows


def compute_reaction_stability(
    messages: Iterable[TelegramMessage],
    period: Period,
    *,
    include_non_human: bool = False,
    allowed_senders: set[str] | None = None,
    min_periods: int = 3,
) -> list[ReactionStabilityProfile]:
    period_rows = compute_sender_reaction_by_period(
        messages, period,
        include_non_human=include_non_human,
        allowed_senders=allowed_senders,
    )

    by_sender: dict[str, list[SenderReactionPeriodRow]] = defaultdict(list)
    names: dict[str, str] = {}
    for row in period_rows:
        by_sender[row.sender_id].append(row)
        names[row.sender_id] = row.display_name

    result: list[ReactionStabilityProfile] = []
    for sid, rows in by_sender.items():
        if len(rows) < min_periods:
            continue

        rpms = [r.reactions_per_message for r in rows]
        mean_rpm = sum(rpms) / len(rpms)
        std_rpm = _std(rpms, mean_rpm)
        cv = std_rpm / mean_rpm if mean_rpm > 0 else 0.0
        med = _median(rpms)
        periods_with_rx = sum(1 for r in rows if r.total_reactions > 0)

        # consistency_score: high mean + low CV = consistently popular
        # Score from 0 to 1, higher is better
        consistency = mean_rpm / (1 + cv) if mean_rpm > 0 else 0.0

        result.append(ReactionStabilityProfile(
            sender_id=sid,
            display_name=names[sid],
            periods_active=len(rows),
            periods_with_reactions=periods_with_rx,
            mean_reactions_per_message=mean_rpm,
            std_reactions_per_message=std_rpm,
            cv_reactions_per_message=cv,
            min_reactions_per_message=min(rpms),
            max_reactions_per_message=max(rpms),
            median_reactions_per_message=med,
            consistency_score=consistency,
        ))

    return sorted(result, key=lambda r: (-r.consistency_score, r.display_name.casefold()))


def get_top_reacted_messages(
    messages: Iterable[TelegramMessage],
    *,
    include_non_human: bool = False,
    top_n: int = 50,
    min_reactions: int = 1,
) -> list[TopReactedMessage]:
    filtered = filter_messages_for_participants(messages, include_non_human=include_non_human)
    profiles = collect_sender_profiles(filtered)

    candidates = [m for m in filtered if m.total_reactions >= min_reactions]
    candidates.sort(key=lambda m: (-m.total_reactions, m.date))

    result: list[TopReactedMessage] = []
    for msg in candidates[:top_n]:
        sid = canonical_sender_id(msg)
        profile = profiles.get(sid)
        name = profile.display_name if profile else sid
        preview = msg.text[:200] if msg.text else ""
        breakdown = tuple((r.emoji, r.count) for r in sorted(msg.reactions, key=lambda r: -r.count))

        result.append(TopReactedMessage(
            message_id=msg.id,
            date=msg.date.isoformat(sep=" "),
            sender_id=sid,
            display_name=name,
            text_preview=preview,
            total_reactions=msg.total_reactions,
            reaction_breakdown=breakdown,
        ))

    return result
