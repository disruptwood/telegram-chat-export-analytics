from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CliTests(unittest.TestCase):
    def test_export_command_writes_csv_and_summary_from_export_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            export_dir = root / "DemoChatExport_2026-03-14"
            output_dir = root / "outputs"
            export_dir.mkdir()
            input_path = export_dir / "result.json"

            input_path.write_text(
                json.dumps(
                    {
                        "id": 1,
                        "name": "Demo chat",
                        "type": "personal_chat",
                        "messages": [
                            {
                                "id": 1,
                                "type": "message",
                                "date": "2026-03-01T10:00:00",
                                "from": "Ana",
                                "from_id": "user1",
                                "text": "Hello",
                            },
                            {
                                "id": 2,
                                "type": "message",
                                "date": "2026-03-15T10:00:00",
                                "from": "Ana",
                                "from_id": "user1",
                                "text": "World",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path.cwd() / "src")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "chat_analytics.cli",
                    "export",
                    str(export_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue((output_dir / "summary.md").exists())
            self.assertTrue((output_dir / "message_counts_month.csv").exists())

            with (output_dir / "message_counts_month.csv").open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(
                rows,
                [
                    {
                        "period": "month",
                        "label": "2026-03",
                        "start_date": "2026-03-01",
                        "end_date": "2026-03-31",
                        "message_count": "2",
                    }
                ],
            )

    def test_participant_report_writes_summary_and_top_senders(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "result.json"
            output_dir = root / "outputs"

            input_path.write_text(
                json.dumps(
                    {
                        "id": 1,
                        "name": "Demo chat",
                        "type": "personal_chat",
                        "messages": [
                            {
                                "id": 1,
                                "type": "message",
                                "date": "2026-03-01T10:00:00",
                                "from": "Ana",
                                "from_id": "user1",
                                "text": "Hello",
                            },
                            {
                                "id": 2,
                                "type": "message",
                                "date": "2026-03-02T10:00:00",
                                "from": "Ben",
                                "from_id": "user2",
                                "text": "World",
                            },
                            {
                                "id": 3,
                                "type": "message",
                                "date": "2026-03-02T12:00:00",
                                "from": "Announcements",
                                "from_id": "channel1",
                                "text": "News",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path.cwd() / "src")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "chat_analytics.cli",
                    "participant-report",
                    str(input_path),
                    "--output-dir",
                    str(output_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue((output_dir / "participant_summary_month.csv").exists())
            self.assertTrue((output_dir / "top_senders_month.json").exists())
            self.assertTrue((output_dir / "sender_directory.csv").exists())

    def test_heatmap_bubbles_writes_visualization_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "result.json"
            output_dir = root / "outputs"

            input_path.write_text(
                json.dumps(
                    {
                        "id": 1,
                        "name": "Demo chat",
                        "type": "personal_chat",
                        "messages": [
                            {
                                "id": 1,
                                "type": "message",
                                "date": "2026-03-01T10:00:00",
                                "from": "Ana",
                                "from_id": "user1",
                                "text": "Hello",
                            },
                            {
                                "id": 2,
                                "type": "message",
                                "date": "2026-03-03T10:00:00",
                                "from": "Ben",
                                "from_id": "user2",
                                "text": "World",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path.cwd() / "src")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "chat_analytics.cli",
                    "heatmap-bubbles",
                    str(input_path),
                    "--output-dir",
                    str(output_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue((output_dir / "daily_all_senders.json").exists())
            self.assertTrue((output_dir / "heatmap_bubbles.html").exists())


if __name__ == "__main__":
    unittest.main()
