from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from chat_analytics.loader import flatten_text, parse_message, resolve_export_path


class LoaderTests(unittest.TestCase):
    def test_flatten_text_handles_rich_text_entities(self) -> None:
        raw_text = [
            "Meet at ",
            {"type": "link", "text": "https://example.com"},
            " please",
        ]
        self.assertEqual(flatten_text(raw_text), "Meet at https://example.com please")

    def test_parse_message_normalizes_expected_fields(self) -> None:
        message = parse_message(
            {
                "id": 7,
                "type": "message",
                "date": "2026-03-01T10:30:00",
                "from": "Ana",
                "from_id": "user1",
                "text": ["Hi", {"text": "!"}],
                "edited": "2026-03-01T11:00:00",
            }
        )

        self.assertEqual(message.id, 7)
        self.assertTrue(message.is_user_message)
        self.assertEqual(message.text, "Hi!")
        self.assertTrue(message.is_edited)
        self.assertIsNone(message.reply_to_message_id)
        self.assertIsNone(message.action)

    def test_resolve_export_path_accepts_export_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            export_dir = root / "SomeChatExport_2026-03-14"
            export_dir.mkdir()
            (export_dir / "result.json").write_text(json.dumps({"messages": []}), encoding="utf-8")

            self.assertEqual(resolve_export_path(export_dir), export_dir / "result.json")


if __name__ == "__main__":
    unittest.main()
