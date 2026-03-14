from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from chat_analytics.models import TelegramMessage
from chat_analytics.visualizations import (
    build_daily_sender_series,
    write_daily_sender_series_json,
    write_heatmap_bubbles_html,
)


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


class VisualizationTests(unittest.TestCase):
    def test_build_daily_sender_series_creates_dense_day_ranges(self) -> None:
        messages = [
            make_message(1, "2026-01-01T10:00:00", "user2", "Ben"),
            make_message(2, "2026-01-01T12:00:00", "user1", "Ana"),
            make_message(3, "2026-01-03T09:00:00", "user1", "Ana"),
            make_message(4, "2026-01-03T11:00:00", "channel1", "News"),
        ]

        series = build_daily_sender_series(messages)

        self.assertEqual(series.days, ["2026-01-01", "2026-01-02", "2026-01-03"])
        self.assertEqual(series.names, {"user1": "Ana", "user2": "Ben"})
        self.assertEqual(series.counts["user1"], [1, 0, 1])
        self.assertEqual(series.counts["user2"], [1, 0, 0])

    def test_writers_emit_expected_files(self) -> None:
        messages = [make_message(1, "2026-01-01T10:00:00", "user1", "Ana")]
        series = build_daily_sender_series(messages)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            json_path = write_daily_sender_series_json(series, root / "daily_all_senders.json")
            html_path = write_heatmap_bubbles_html(root / "heatmap_bubbles.html")

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["days"], ["2026-01-01"])
            self.assertIn("Telegram Chat Activity Bubbles", html_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
