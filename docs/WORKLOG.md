# Worklog

## 2026-03-13

- Initial toolkit structure for Telegram export analytics.
- Added normalized JSON loading, reusable aggregations, CLI commands, and reporting helpers.
- Added participant analytics: active senders over time, top-N senders, newcomer/core metrics, and concentration metrics.
- Added documentation for user activity datasets and output formats.

## 2026-03-14

- Generalized the project for arbitrary Telegram chat exports instead of a single private chat.
- Removed topic-specific functionality from the public-facing toolkit.
- Added support for passing either an export directory or a direct `result.json` path to CLI commands.
- Prepared the repository for open-source publishing with stronger `.gitignore` defaults and generic documentation.
