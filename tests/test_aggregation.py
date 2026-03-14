from __future__ import annotations

import unittest
from datetime import datetime

from chat_analytics.aggregation import count_messages_by_period
from chat_analytics.models import TelegramMessage


def make_message(message_id: int, when: str, message_type: str = "message") -> TelegramMessage:
    return TelegramMessage(
        id=message_id,
        message_type=message_type,
        date=datetime.fromisoformat(when),
        from_name="User",
        from_id="user1",
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


class AggregationTests(unittest.TestCase):
    def test_count_messages_by_month_excludes_service_events_by_default(self) -> None:
        messages = [
            make_message(1, "2026-01-05T12:00:00"),
            make_message(2, "2026-01-20T12:00:00"),
            make_message(3, "2026-01-21T12:00:00", message_type="service"),
            make_message(4, "2026-02-01T12:00:00"),
        ]

        rows = count_messages_by_period(messages, "month")

        self.assertEqual(
            [(row.label, row.message_count) for row in rows],
            [("2026-01", 2), ("2026-02", 1)],
        )

    def test_count_messages_by_week_uses_monday_bucket_start(self) -> None:
        messages = [
            make_message(1, "2026-03-02T10:00:00"),
            make_message(2, "2026-03-08T22:00:00"),
            make_message(3, "2026-03-09T09:00:00"),
        ]

        rows = count_messages_by_period(messages, "week")

        self.assertEqual(
            [(row.label, row.start_date.isoformat(), row.message_count) for row in rows],
            [("2026-W10", "2026-03-02", 2), ("2026-W11", "2026-03-09", 1)],
        )


if __name__ == "__main__":
    unittest.main()
