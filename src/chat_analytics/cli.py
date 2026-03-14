from __future__ import annotations

import argparse
from pathlib import Path

from .aggregation import SUPPORTED_PERIODS, count_messages_by_period
from .loader import load_chat_export
from .participants import summarize_participants_by_period, build_sender_directory
from .reporting import (
    write_counts_csv,
    write_participant_report_markdown,
    write_participant_summary_csv,
    write_sender_directory_csv,
    write_summary_markdown,
    write_top_sender_rows_csv,
    write_top_sender_rows_json,
)
from .visualizations import (
    build_daily_sender_series,
    write_daily_sender_series_json,
    write_heatmap_bubbles_html,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telegram-chat-analytics",
        description="Analyze Telegram chat export directories or result.json files.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    counts_parser = subparsers.add_parser("counts", help="Print message counts for a single period.")
    counts_parser.add_argument("input_path", help="Path to a Telegram export directory or result.json file.")
    counts_parser.add_argument("--period", choices=SUPPORTED_PERIODS, default="month")
    counts_parser.add_argument("--limit", type=int, default=0, help="Limit printed rows from the end.")
    counts_parser.add_argument(
        "--include-service",
        action="store_true",
        help="Include Telegram service events alongside user messages.",
    )

    export_parser = subparsers.add_parser(
        "export",
        help="Write CSV series and a Markdown summary.",
    )
    export_parser.add_argument("input_path", help="Path to a Telegram export directory or result.json file.")
    export_parser.add_argument("--output-dir", default="outputs", help="Directory for generated artifacts.")
    export_parser.add_argument(
        "--period",
        dest="periods",
        action="append",
        choices=SUPPORTED_PERIODS,
        help="Period to export. Repeat the flag to export multiple periods. Default: all.",
    )
    export_parser.add_argument(
        "--include-service",
        action="store_true",
        help="Include Telegram service events alongside user messages.",
    )

    participant_parser = subparsers.add_parser(
        "participant-report",
        help="Analyze senders, active participants, and top contributors.",
    )
    participant_parser.add_argument("input_path", help="Path to a Telegram export directory or result.json file.")
    participant_parser.add_argument("--output-dir", default="outputs", help="Directory for generated artifacts.")
    participant_parser.add_argument(
        "--include-non-user",
        action="store_true",
        help="Include channel/chat senders alongside human-like users.",
    )
    participant_parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="How many top senders to export for each period.",
    )

    heatmap_parser = subparsers.add_parser(
        "heatmap-bubbles",
        help="Generate a daily sender dataset and a self-contained HTML bubble visualization.",
    )
    heatmap_parser.add_argument("input_path", help="Path to a Telegram export directory or result.json file.")
    heatmap_parser.add_argument("--output-dir", default="outputs", help="Directory for generated artifacts.")
    heatmap_parser.add_argument(
        "--include-non-user",
        action="store_true",
        help="Include channel/chat senders alongside human-like users.",
    )

    return parser


def render_rows(rows: list, limit: int) -> str:
    visible_rows = rows[-limit:] if limit > 0 else rows
    lines = ["label,start_date,end_date,message_count"]
    for row in visible_rows:
        lines.append(
            ",".join(
                [
                    row.label,
                    row.start_date.isoformat(),
                    row.end_date.isoformat(),
                    str(row.message_count),
                ]
            )
        )
    return "\n".join(lines)


def run_counts(args: argparse.Namespace) -> int:
    chat_export = load_chat_export(args.input_path)
    rows = count_messages_by_period(
        chat_export.messages,
        args.period,
        include_service=args.include_service,
    )
    print(render_rows(rows, args.limit))
    return 0


def run_export(args: argparse.Namespace) -> int:
    chat_export = load_chat_export(args.input_path)
    output_dir = Path(args.output_dir)
    periods = tuple(args.periods) if args.periods else SUPPORTED_PERIODS

    for period in periods:
        rows = count_messages_by_period(
            chat_export.messages,
            period,
            include_service=args.include_service,
        )
        write_counts_csv(rows, output_dir / f"message_counts_{period}.csv")

    write_summary_markdown(
        chat_export,
        output_dir / "summary.md",
        include_service=args.include_service,
    )
    print(f"Artifacts written to {output_dir}")
    return 0


def run_participant_report(args: argparse.Namespace) -> int:
    chat_export = load_chat_export(args.input_path)
    output_dir = Path(args.output_dir)

    sender_directory_rows = build_sender_directory(
        chat_export.messages,
        include_non_human=True,
    )
    write_sender_directory_csv(sender_directory_rows, output_dir / "sender_directory.csv")

    for period in SUPPORTED_PERIODS:
        summaries, top_rows = summarize_participants_by_period(
            chat_export.messages,
            period,
            include_non_human=args.include_non_user,
            top_n=args.top_n,
        )
        write_participant_summary_csv(summaries, output_dir / f"participant_summary_{period}.csv")
        write_top_sender_rows_csv(top_rows, output_dir / f"top_senders_{period}.csv")
        write_top_sender_rows_json(summaries, top_rows, output_dir / f"top_senders_{period}.json")

    write_participant_report_markdown(
        chat_export,
        output_dir / "participant_report.md",
        include_non_human=args.include_non_user,
        top_n=args.top_n,
    )
    print(f"Participant artifacts written to {output_dir}")
    return 0


def run_heatmap_bubbles(args: argparse.Namespace) -> int:
    chat_export = load_chat_export(args.input_path)
    output_dir = Path(args.output_dir)
    series = build_daily_sender_series(
        chat_export.messages,
        include_non_human=args.include_non_user,
    )
    write_daily_sender_series_json(series, output_dir / "daily_all_senders.json")
    write_heatmap_bubbles_html(output_dir / "heatmap_bubbles.html")
    print(f"Heatmap bubble artifacts written to {output_dir}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "counts":
        return run_counts(args)
    if args.command == "export":
        return run_export(args)
    if args.command == "participant-report":
        return run_participant_report(args)
    if args.command == "heatmap-bubbles":
        return run_heatmap_bubbles(args)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
