from __future__ import annotations

import unittest
from datetime import datetime

from chat_analytics.models import TelegramMessage
from chat_analytics.participants import build_sender_directory, summarize_participants_by_period


def make_message(
    message_id: int,
    when: str,
    sender_id: str,
    sender_name: str,
    *,
    message_type: str = "message",
) -> TelegramMessage:
    return TelegramMessage(
        id=message_id,
        message_type=message_type,
        date=datetime.fromisoformat(when),
        from_name=sender_name,
        from_id=sender_id,
        text="hello",
        raw_text="hello",
        is_edited=False,
        reply_to_message_id=None,
        action=None,
        actor=None,
        actor_id=None,
        title=None,
        new_title=None,
    )


class ParticipantTests(unittest.TestCase):
    def test_summarize_participants_by_month_excludes_channels_by_default(self) -> None:
        messages = [
            make_message(1, "2026-01-03T10:00:00", "user1", "Ana"),
            make_message(2, "2026-01-05T10:00:00", "user1", "Ana"),
            make_message(3, "2026-01-06T10:00:00", "user2", "Ben"),
            make_message(4, "2026-01-07T10:00:00", "channel1", "Announcements"),
        ]

        summaries, top_rows = summarize_participants_by_period(messages, "month", top_n=10)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].active_senders, 2)
        self.assertEqual(summaries[0].total_messages, 3)
        self.assertEqual(top_rows[0].display_name, "Ana")
        self.assertEqual(top_rows[0].message_count, 2)

    def test_new_core_senders_are_tracked_across_periods(self) -> None:
        messages = [
            make_message(1, "2026-01-03T10:00:00", "user1", "Ana"),
            make_message(2, "2026-01-04T10:00:00", "user1", "Ana"),
            make_message(3, "2026-01-05T10:00:00", "user1", "Ana"),
            make_message(4, "2026-02-03T10:00:00", "user1", "Ana"),
            make_message(5, "2026-02-04T10:00:00", "user1", "Ana"),
            make_message(6, "2026-02-05T10:00:00", "user1", "Ana"),
            make_message(7, "2026-02-06T10:00:00", "user2", "Ben"),
            make_message(8, "2026-02-07T10:00:00", "user2", "Ben"),
            make_message(9, "2026-02-08T10:00:00", "user2", "Ben"),
        ]

        summaries, _ = summarize_participants_by_period(messages, "month", core_threshold=3)

        self.assertEqual(summaries[0].new_core_senders, 1)
        self.assertEqual(summaries[1].new_core_senders, 1)
        self.assertEqual(summaries[1].retained_core_senders, 1)

    def test_build_sender_directory_keeps_latest_name_for_sender(self) -> None:
        messages = [
            make_message(1, "2026-01-03T10:00:00", "user1", "Ana"),
            make_message(2, "2026-02-03T10:00:00", "user1", "Anastasia"),
        ]

        rows = build_sender_directory(messages, include_non_human=False)

        self.assertEqual(rows[0].display_name, "Anastasia")
        self.assertEqual(rows[0].total_messages, 2)


if __name__ == "__main__":
    unittest.main()
