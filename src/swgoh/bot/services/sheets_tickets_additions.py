# Additions for src/swgoh/bot/services/sheets.py
# Paste these functions at the end of the existing sheets.py file.
# Also add TICKET_SNAPSHOTS_SHEET to the imports at the top:
#   from .. import config as bot_cfg
#   TICKET_SNAPSHOTS_SHEET = bot_cfg.TICKET_SNAPSHOTS_SHEET

from __future__ import annotations

from datetime import date
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Guilds — reset time
# ---------------------------------------------------------------------------

def get_guild_reset_time(ss, guild_id: str) -> Optional[str]:
    """
    Returns the ticket reset time (HH:MM, 24h, Madrid TZ) for the given
    guild_id, or None if the column/row is missing.

    Reads from the 'reset_time' column in GUILDS_SHEET.
    """
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]

    if "guild id" not in hl or "reset_time" not in hl:
        return None

    i_id = hl.index("guild id")
    i_rt = hl.index("reset_time")

    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid == guild_id:
            raw = (r[i_rt] if i_rt < len(r) else "").strip()
            return raw if raw else None
    return None


def list_guilds_with_reset_time(ss) -> list[tuple[str, str, str]]:
    """
    Returns [(guild_id, guild_name, reset_time_str), ...] for all guilds
    that have a non-empty reset_time column.
    Used by the snapshot scheduler to know which guilds to snapshot and when.
    """
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]

    if "guild id" not in hl or "guild name" not in hl or "reset_time" not in hl:
        return []

    i_id   = hl.index("guild id")
    i_name = hl.index("guild name")
    i_rt   = hl.index("reset_time")

    out = []
    for r in rows:
        gid  = (r[i_id]   if i_id   < len(r) else "").strip()
        name = (r[i_name] if i_name < len(r) else "").strip()
        rt   = (r[i_rt]   if i_rt   < len(r) else "").strip()
        if gid and name and rt:
            out.append((gid, name, rt))
    return out


# ---------------------------------------------------------------------------
# Ticket Snapshots — read / write
# ---------------------------------------------------------------------------

def ensure_ticket_snapshots_headers(ws) -> Dict[str, int]:
    """
    Ensures the Ticket_Snapshots sheet has the required columns.
    Returns a header→index (0-based) map.

    Schema:
        guild_name | player_name | snapshot_date | lifetime_value
    """
    needed = ["guild_name", "player_name", "snapshot_date", "lifetime_value"]
    headers = ws.row_values(1) or []
    low = [h.strip().lower() for h in headers]

    changed = False
    for col in needed:
        if col not in low:
            headers.append(col)
            low.append(col)
            changed = True
    if changed:
        ws.update("1:1", [headers])

    return {h: i for i, h in enumerate([h.strip().lower() for h in headers])}


def upsert_ticket_snapshots(ss, guild_name: str, snapshots: Dict[str, int]) -> None:
    """
    Writes (or overwrites) one row per member for the given guild_name.
    Rows from other guilds are preserved.

    Args:
        ss:           gspread Spreadsheet object
        guild_name:   name of the guild being snapshotted
        snapshots:    {player_name: lifetime_value} for every member
    """
    from .. import config as bot_cfg  # local import to avoid circular at module level
    ws = ss.worksheet(bot_cfg.TICKET_SNAPSHOTS_SHEET)
    hdr_map = ensure_ticket_snapshots_headers(ws)

    i_gn   = hdr_map["guild_name"]
    i_pn   = hdr_map["player_name"]
    i_date = hdr_map["snapshot_date"]
    i_lv   = hdr_map["lifetime_value"]

    today_str = date.today().isoformat()  # YYYY-MM-DD

    all_vals = ws.get_all_values() or []
    existing_rows: list[list[str]] = all_vals[1:] if len(all_vals) > 1 else []

    # Keep rows belonging to OTHER guilds untouched
    other_rows = [
        r for r in existing_rows
        if (r[i_gn] if i_gn < len(r) else "").strip() != guild_name
    ]

    # Build new rows for this guild
    n_cols = len(ws.row_values(1) or [])
    new_rows: list[list[str]] = []
    for player_name, lifetime_val in snapshots.items():
        row: list[str] = [""] * n_cols
        row[i_gn]   = guild_name
        row[i_pn]   = player_name
        row[i_date] = today_str
        row[i_lv]   = str(lifetime_val)
        new_rows.append(row)

    # Sort for stable ordering
    new_rows.sort(key=lambda r: r[i_pn].lower())

    final_rows = other_rows + new_rows
    n_data = len(final_rows)

    ws.resize(rows=max(n_data + 1, 1), cols=n_cols)
    ws.clear()
    ws.update("A1", [list(ws.row_values(1) or [])])  # re-write header
    # Re-fetch header after clear+update to be safe
    hdr_row = [h if i < n_cols else "" for i, h in enumerate(["guild_name", "player_name", "snapshot_date", "lifetime_value"])]
    ws.update("A1", [hdr_row])

    if final_rows:
        ws.update("A2", final_rows)


def read_ticket_snapshot(ss, guild_name: str) -> Optional[tuple[str, Dict[str, int]]]:
    """
    Reads the latest snapshot for guild_name.

    Returns:
        (snapshot_date_str, {player_name_lower: lifetime_value})
        or None if no snapshot exists for this guild.
    """
    from .. import config as bot_cfg
    try:
        ws = ss.worksheet(bot_cfg.TICKET_SNAPSHOTS_SHEET)
    except Exception:
        return None

    headers, rows = _get_all(ws)
    if not rows:
        return None

    hl = [h.strip().lower() for h in headers]
    required = ["guild_name", "player_name", "snapshot_date", "lifetime_value"]
    if any(col not in hl for col in required):
        return None

    i_gn   = hl.index("guild_name")
    i_pn   = hl.index("player_name")
    i_date = hl.index("snapshot_date")
    i_lv   = hl.index("lifetime_value")

    snapshot_date: Optional[str] = None
    result: Dict[str, int] = {}

    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        pn   = (r[i_pn]   if i_pn   < len(r) else "").strip()
        dt   = (r[i_date] if i_date  < len(r) else "").strip()
        lv   = (r[i_lv]   if i_lv   < len(r) else "").strip()

        if not pn:
            continue

        try:
            lifetime = int(lv)
        except (ValueError, TypeError):
            lifetime = 0

        result[pn.lower()] = lifetime
        if snapshot_date is None:
            snapshot_date = dt

    if not result:
        return None

    return snapshot_date, result
