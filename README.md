# Telegram Chat Export Analytics

Reusable toolkit for analyzing large Telegram chat exports.

The project is designed around raw Telegram export folders and focuses on reproducible analytics rather than notebook-only exploration. It currently supports message-volume trends, participant activity over time, and a browser-based animated bubble heatmap of daily sender activity.

## Principles

- One source of truth for Telegram export loading: `chat_analytics.loader`.
- Domain model, aggregations, reporting, visualizations, and CLI are separated.
- All core workflows are testable via `unittest`.
- Export data and generated artifacts stay outside version control by default.

## Structure

- `src/chat_analytics/models.py` - domain entities.
- `src/chat_analytics/loader.py` - export directory and `result.json` loading.
- `src/chat_analytics/aggregation.py` - message-count aggregations by time period.
- `src/chat_analytics/participants.py` - participant and sender analytics.
- `src/chat_analytics/visualizations.py` - reusable visualization datasets and HTML exports.
- `src/chat_analytics/reporting.py` - CSV/JSON/Markdown artifact writers.
- `src/chat_analytics/cli.py` - command-line entrypoints.
- `docs/USER_ACTIVITY_DATA.md` - data dictionary for user activity outputs.
- `tests/` - unit tests.

## Supported Input

Each command accepts either:

- a path to a Telegram export directory that contains `result.json`
- or a direct path to the `result.json` file itself

Example export directory:

```text
SomeChatExport_2026-03-14/
  result.json
  contacts/
  photos/
  video_files/
```

## Quick Start

Running from source is enough:

```bash
PYTHONPATH=src python3 -m chat_analytics.cli export /path/to/SomeChatExport_2026-03-14 --output-dir outputs
```

This generates:

- `outputs/message_counts_day.csv`
- `outputs/message_counts_week.csv`
- `outputs/message_counts_month.csv`
- `outputs/message_counts_year.csv`
- `outputs/summary.md`

Participant analytics:

```bash
PYTHONPATH=src python3 -m chat_analytics.cli participant-report /path/to/SomeChatExport_2026-03-14 --output-dir outputs --top-n 10
```

This generates:

- `outputs/participant_summary_day.csv`
- `outputs/participant_summary_week.csv`
- `outputs/participant_summary_month.csv`
- `outputs/participant_summary_year.csv`
- `outputs/top_senders_day.csv`
- `outputs/top_senders_week.csv`
- `outputs/top_senders_month.csv`
- `outputs/top_senders_year.csv`
- `outputs/top_senders_day.json`
- `outputs/top_senders_week.json`
- `outputs/top_senders_month.json`
- `outputs/top_senders_year.json`
- `outputs/sender_directory.csv`
- `outputs/participant_report.md`

Daily heatmap bubble visualization:

```bash
PYTHONPATH=src python3 -m chat_analytics.cli heatmap-bubbles /path/to/SomeChatExport_2026-03-14 --output-dir outputs
```

This generates:

- `outputs/daily_all_senders.json`
- `outputs/heatmap_bubbles.html`

Open `heatmap_bubbles.html` in a browser from the same folder as `daily_all_senders.json`. The animation shows how sender activity accumulates and decays over time, which makes bursts of sustained participation visible at a glance.

Documentation for participant activity files and metrics: `docs/USER_ACTIVITY_DATA.md`.

To print a time series directly in the terminal:

```bash
PYTHONPATH=src python3 -m chat_analytics.cli counts /path/to/SomeChatExport_2026-03-14 --period month --limit 12
```

## Current Assumptions

- The `date` field from Telegram export is treated as the event timestamp.
- Message counts exclude service events by default.
- Rich Telegram `text` arrays are normalized into plain strings.
- Participant analytics include only senders with `from_id` starting with `user` by default.
- Non-user senders such as channels are still discoverable through `sender_directory.csv`.
- The heatmap bubble dataset is daily and dense: missing dates inside the export range are included with zero counts.
- Export data and generated artifacts are intentionally ignored by git in this repository.

## Development Check

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
