# SWGOH — Data + Bot

## Architecture

Everything runs in a **single process** (`python -m swgoh.bot.main_bot`):

| What | How |
|---|---|
| Telegram bot (polling) | python-telegram-bot v20 |
| Ticket snapshots | APScheduler cron, time read from Guilds sheet (`reset_time` column) |
| Daily send_assignments | PTB JobQueue `run_daily` at `SEND_ASSIGNMENTS_TIME` |
| Weekly sync_guilds | APScheduler cron (`SYNC_GUILDS_CRON`) |
| Monthly sync_data | APScheduler cron (`SYNC_DATA_CRON`) |

The external Apps Script webhook dependency has been removed. `/syncguild` now runs the sync directly via `sync_runner.run_sync_guilds_once()`.

## Railway services (current — to be reduced)

| Service | Entry point | Status |
|---|---|---|
| swgoh-comlink | external image | keep |
| LiveBot | `python -m swgoh.bot.main_bot` | **consolidated — runs everything** |
| send_assignments | `python -m swgoh.bot.jobs.send_assignments_daily` | **remove** |
| swgoh_guild_data | `python -m swgoh.processing.sync_guilds` | **remove** |
| swgoh_data | `python -m swgoh.processing.sync_data` | **remove** |

## Raspberry Pi deployment

```bash
# 1. Clone and set up
git clone https://github.com/DarthFarra/swgoh /home/pi/swgoh
cd /home/pi/swgoh
python3 -m venv .venv
.venv/bin/pip install -e .

# 2. Configure
cp .env.example .env
nano .env   # fill in your values

# 3. Install systemd service
sudo cp systemd/swgoh-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable swgoh-bot
sudo systemctl start swgoh-bot

# 4. Check it's running
sudo systemctl status swgoh-bot
sudo journalctl -u swgoh-bot -f
```

After a code update:
```bash
git pull
sudo systemctl restart swgoh-bot
```

## Offline `/syncguild` mock

When Google Sheets is unavailable, the effect of `/syncguild` can be simulated
from a saved Comlink `/guild` response in `guild-comlink.json`. The script
writes CSV equivalents of the `Guilds`, `Players`, `Player_Units`, and
`Player_Skills` tabs:

```bash
python scripts/mock_syncguild.py pTtJHHuYQcSMinxQXXZJgA
```

Use `--comlink-json` to provide another saved Comlink response, `--snapshot`
to use a legacy ticket CSV explicitly, and `--output-dir` to keep the
generated files outside the repository.

## Environment variables

See `.env.example` — single file, all variables documented.

Key variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `COMLINK_BASE` | ✅ | — | Comlink service URL |
| `SERVICE_ACCOUNT_FILE` | ✅ | — | Google credentials (path, JSON, or base64) |
| `SPREADSHEET_ID` | ✅* | — | Google Sheet ID (`*` or `SPREADSHEET_NAME`) |
| `TELEGRAM_BOT_TOKEN` | ✅ | — | Telegram bot token |
| `TIMEZONE` | | `Europe/Madrid` | IANA timezone for sync cron jobs |
| `TICKETS_TIMEZONE` | | `Atlantic/Reykjavik` | IANA timezone for ticket snapshots and reminders |
| `ASSIGNMENTS_TIMEZONE` | | `America/New_York` | IANA timezone for assignment sends |
| `SEND_ASSIGNMENTS_TIME` | | `12:59` | Daily send time (HH:MM) in `ASSIGNMENTS_TIMEZONE` |
| `SYNC_GUILDS_CRON` | | `0 3 * * 0` | Weekly guild sync schedule |
| `SYNC_DATA_CRON` | | `0 2 1 * *` | Monthly data sync schedule |

Discord users can register with `/register`, query their own ROTE assignments
with `/misoperaciones`, and officers/leaders can query another player's
assignments with `/operacionesjugador`. The daily assignment job sends the
same assignment message by Discord DM to registered Discord users. The bot
adds the `discord_id` column to the `Usuarios` sheet automatically when needed.

## Project structure

```
src/swgoh/
├── config.py               ← single source of truth for all env vars
├── creds.py                ← Google credential loader
├── sheets.py               ← single gspread client (shared by all jobs)
├── http.py                 ← Comlink HTTP client
├── comlink.py              ← Comlink API calls
├── processing/
│   ├── sync_data.py
│   └── sync_guilds.py
└── bot/
    ├── config.py           ← thin shim aliasing from core config
    ├── main_bot.py         ← bot + all schedulers
    ├── commands/           ← Telegram command handlers
    ├── jobs/
    │   ├── send_assignments_daily.py
    │   └── snapshot_tickets.py
    └── services/
        ├── sheets.py
        ├── sync_runner.py
        └── ...
```
