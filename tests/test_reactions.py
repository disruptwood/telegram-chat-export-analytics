from __future__ import annotations

import unittest
from datetime import datetime

from chat_analytics.models import Reaction, TelegramMessage
from chat_analytics.reactions import (
    compute_message_distribution,
    compute_reaction_stability,
    compute_sender_reaction_by_period,
    compute_sender_reaction_profiles,
    filter_senders_by_percentile,
    filter_senders_min_messages,
    filter_senders_top_n,
    get_top_reacted_messages,
)


def make_message(
    message_id: int,
    when: str,
    sender_id: str,
    sender_name: str,
    text: str = "hello",
    reactions: tuple[Reaction, ...] = (),
    *,
    message_type: str = "message",
) -> TelegramMessage:
    return TelegramMessage(
        id=message_id,
        message_type=message_type,
        date=datetime.fromisoformat(when),
        from_name=sender_name,
        from_id=sender_id,
        text=text,
        raw_text=text,
        is_edited=False,
        reply_to_message_id=None,
        action=None,
        actor=None,
        actor_id=None,
        title=None,
        new_title=None,
        reactions=reactions,
    )


def r(emoji: str, count: int) -> Reaction:
    return Reaction(emoji=emoji, count=count)


class TestSenderReactionProfiles(unittest.TestCase):
    def test_basic_reaction_stats(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "hi", (r("👍", 3), r("❤️", 1))),
            make_message(2, "2026-01-02T10:00:00", "user1", "Ana", "hello world"),
            make_message(3, "2026-01-03T10:00:00", "user2", "Ben", "test", (r("👍", 5),)),
        ]
        profiles = compute_sender_reaction_profiles(messages)
        self.assertEqual(len(profiles), 2)

        # Ben has 5 reactions on 1 msg = 5.0 R/Msg, Ana has 4 on 2 = 2.0
        ben = next(p for p in profiles if p.sender_id == "user2")
        self.assertEqual(ben.total_reactions, 5)
        self.assertAlmostEqual(ben.reactions_per_message, 5.0)

        ana = next(p for p in profiles if p.sender_id == "user1")
        self.assertEqual(ana.total_reactions, 4)
        self.assertAlmostEqual(ana.reactions_per_message, 2.0)
        self.assertEqual(ana.messages_with_reactions, 1)

    def test_allowed_senders_filter(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "hi", (r("👍", 3),)),
            make_message(2, "2026-01-02T10:00:00", "user2", "Ben", "test", (r("👍", 5),)),
        ]
        profiles = compute_sender_reaction_profiles(messages, allowed_senders={"user1"})
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].sender_id, "user1")

    def test_reactions_per_1k_chars(self) -> None:
        text = "a" * 500  # 500 chars
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", text, (r("👍", 10),)),
        ]
        profiles = compute_sender_reaction_profiles(messages)
        self.assertAlmostEqual(profiles[0].reactions_per_1k_chars, 20.0)


class TestSenderFiltering(unittest.TestCase):
    def test_top_n_filter(self) -> None:
        messages = [
            make_message(i, f"2026-01-{i:02d}T10:00:00", "user1", "Ana")
            for i in range(1, 11)
        ] + [
            make_message(11, "2026-01-11T10:00:00", "user2", "Ben"),
            make_message(12, "2026-01-12T10:00:00", "user3", "Carol"),
        ]
        top = filter_senders_top_n(messages, 2)
        self.assertIn("user1", top)
        self.assertEqual(len(top), 2)

    def test_min_messages_filter(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana"),
            make_message(2, "2026-01-02T10:00:00", "user1", "Ana"),
            make_message(3, "2026-01-03T10:00:00", "user1", "Ana"),
            make_message(4, "2026-01-04T10:00:00", "user2", "Ben"),
        ]
        result = filter_senders_min_messages(messages, 2)
        self.assertEqual(result, {"user1"})

    def test_percentile_filter(self) -> None:
        messages = [
            make_message(i, f"2026-01-{i:02d}T10:00:00", "user1", "Ana")
            for i in range(1, 21)
        ] + [
            make_message(21, "2026-01-21T10:00:00", "user2", "Ben"),
        ]
        result = filter_senders_by_percentile(messages, min_percentile=0.5)
        # user1 has 20 msgs, user2 has 1 msg; p50 = 10.5, only user1 >= 10.5
        self.assertIn("user1", result)
        self.assertNotIn("user2", result)


class TestMessageDistribution(unittest.TestCase):
    def test_distribution_stats(self) -> None:
        messages = [
            make_message(i, f"2026-01-{(i % 28) + 1:02d}T10:00:00", "user1", "Ana")
            for i in range(1, 11)
        ] + [
            make_message(11, "2026-01-11T10:00:00", "user2", "Ben"),
        ]
        dist = compute_message_distribution(messages)
        self.assertEqual(dist.count, 2)
        self.assertAlmostEqual(dist.mean, 5.5)
        self.assertAlmostEqual(dist.min, 1.0)
        self.assertAlmostEqual(dist.max, 10.0)


class TestReactionByPeriod(unittest.TestCase):
    def test_monthly_breakdown(self) -> None:
        messages = [
            make_message(1, "2026-01-05T10:00:00", "user1", "Ana", "hi", (r("👍", 3),)),
            make_message(2, "2026-01-15T10:00:00", "user1", "Ana", "yo", (r("👍", 1),)),
            make_message(3, "2026-02-05T10:00:00", "user1", "Ana", "test", (r("❤️", 5),)),
        ]
        rows = compute_sender_reaction_by_period(messages, "month")
        self.assertEqual(len(rows), 2)
        jan = rows[0]
        self.assertEqual(jan.label, "2026-01")
        self.assertEqual(jan.total_reactions, 4)
        self.assertAlmostEqual(jan.reactions_per_message, 2.0)

        feb = rows[1]
        self.assertEqual(feb.total_reactions, 5)


class TestReactionStability(unittest.TestCase):
    def test_consistent_sender(self) -> None:
        messages = []
        msg_id = 1
        for month in range(1, 5):
            for day in range(1, 4):
                messages.append(make_message(
                    msg_id, f"2026-{month:02d}-{day:02d}T10:00:00",
                    "user1", "Ana", "text", (r("👍", 2),),
                ))
                msg_id += 1

        stability = compute_reaction_stability(messages, "month", min_periods=3)
        self.assertEqual(len(stability), 1)
        self.assertEqual(stability[0].sender_id, "user1")
        self.assertAlmostEqual(stability[0].cv_reactions_per_message, 0.0)
        self.assertGreater(stability[0].consistency_score, 0)

    def test_min_periods_filter(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "hi", (r("👍", 1),)),
            make_message(2, "2026-02-01T10:00:00", "user1", "Ana", "hi", (r("👍", 1),)),
        ]
        stability = compute_reaction_stability(messages, "month", min_periods=3)
        self.assertEqual(len(stability), 0)


class TestTopReactedMessages(unittest.TestCase):
    def test_top_messages_sorted(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "low", (r("👍", 1),)),
            make_message(2, "2026-01-02T10:00:00", "user1", "Ana", "high", (r("👍", 10), r("❤️", 5))),
            make_message(3, "2026-01-03T10:00:00", "user2", "Ben", "mid", (r("👍", 3),)),
            make_message(4, "2026-01-04T10:00:00", "user2", "Ben", "none"),
        ]
        top = get_top_reacted_messages(messages, top_n=3)
        self.assertEqual(len(top), 3)
        self.assertEqual(top[0].message_id, 2)
        self.assertEqual(top[0].total_reactions, 15)
        self.assertEqual(top[1].message_id, 3)
        self.assertEqual(top[2].message_id, 1)

    def test_min_reactions_filter(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "low", (r("👍", 1),)),
            make_message(2, "2026-01-02T10:00:00", "user1", "Ana", "high", (r("👍", 10),)),
        ]
        top = get_top_reacted_messages(messages, min_reactions=5)
        self.assertEqual(len(top), 1)
        self.assertEqual(top[0].message_id, 2)

    def test_reaction_breakdown(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "test",
                         (r("👍", 5), r("❤️", 3), r("😂", 1))),
        ]
        top = get_top_reacted_messages(messages)
        self.assertEqual(len(top[0].reaction_breakdown), 3)
        self.assertEqual(top[0].reaction_breakdown[0], ("👍", 5))
        self.assertEqual(top[0].reaction_breakdown[1], ("❤️", 3))


class TestTelegramMessageReactions(unittest.TestCase):
    def test_total_reactions_property(self) -> None:
        msg = make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "hi",
                           (r("👍", 3), r("❤️", 2)))
        self.assertEqual(msg.total_reactions, 5)

    def test_no_reactions(self) -> None:
        msg = make_message(1, "2026-01-01T10:00:00", "user1", "Ana", "hi")
        self.assertEqual(msg.total_reactions, 0)


if __name__ == "__main__":
    unittest.main()
