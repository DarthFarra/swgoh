# src/swgoh/bot/services/tb_undeployed_cache.py
"""
In-memory cache for the undeployed-member list snapshotted at the moment
a TB auto-summary message is sent.

Why this exists:
  The auto-summary message includes inline buttons ("Send DMs" / "Publish
  to channel"). When an officer presses one, the bot must act on EXACTLY
  the list of members the message displayed — not a recomputed list,
  which could differ if a new export arrived in between. So at message-
  send time we snapshot the list and cache it keyed by message_id.

Lifetime:
  Lives in `application.bot_data`. No disk persistence (intentional —
  matches the discord_listener design rule that the Discord channel is
  the source of truth).

  On bot restart the cache is empty. Buttons on pre-restart messages
  will hit the "session expired" branch, which is acceptable.

TTL:
  Entries older than UNDEPLOYED_CACHE_TTL_HOURS are evicted on every
  write. We don't run a background cleanup job because the cache only
  grows on auto-summary sends (rare — one per TB export, maybe a few
  times a day at most). Lazy eviction is sufficient.

Thread safety:
  bot_data is accessed from PTB's single asyncio loop. No locking
  needed because asyncio code yields explicitly; we never hold the
  cache across an await point in a way that could race.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Tuple

log = logging.getLogger(__name__)

# Key under application.bot_data. Single source of truth.
_BOT_DATA_KEY = "tb_undeployed_cache"

# Entries older than this are evicted on next write.
UNDEPLOYED_CACHE_TTL_HOURS: int = 48


@dataclass(frozen=True, slots=True)
class UndeployedMember:
    """
    A single undeployed-member entry, captured at auto-summary send time.

    Frozen so cache state can't be mutated by accident from a callback.
    Fields are denormalized (we keep player_name even though we'd usually
    look it up by id) so the callback has everything it needs without
    re-reading the snapshot — which may have been replaced by a newer
    export by the time the button is pressed.

    Fields:
      player_id    - CG player id, the stable identifier.
      player_name  - display name at snapshot time.
      deployed_gp  - power deployed in this phase.
      roster_gp    - member.galactic_power at snapshot time.
      missing_gp   - max(0, roster_gp - deployed_gp).
      pct_deployed - deployed / roster, in [0, 1+]; rare to exceed 1.0
                     when CG over-credits, but caller should handle.
    """
    player_id: str
    player_name: str
    deployed_gp: int
    roster_gp: int
    missing_gp: int
    pct_deployed: float


@dataclass(frozen=True, slots=True)
class UndeployedSnapshot:
    """
    The full payload cached per message_id.

    `guild_id` is the C3PO/CG guild id (from snap.guild_id, equivalent
    to the filename prefix). `guild_name` is denormalized for the same
    "don't re-read the snapshot" reason.

    `created_at` is used for TTL eviction.
    """
    guild_id: str
    guild_name: str
    members: Tuple[UndeployedMember, ...]
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


def set_snapshot(
    bot_data: dict[str, Any],
    message_id: int,
    snapshot: UndeployedSnapshot,
) -> None:
    """
    Store the snapshot keyed by Telegram message_id.

    Evicts entries older than UNDEPLOYED_CACHE_TTL_HOURS as a side
    effect. We tolerate the O(n) sweep because n is tiny (one entry
    per auto-summary, expired within 48h).
    """
    cache = bot_data.setdefault(_BOT_DATA_KEY, {})
    _evict_expired(cache)
    cache[message_id] = snapshot
    log.debug(
        "Cached undeployed snapshot for message_id=%d guild=%s "
        "members=%d (total cached: %d)",
        message_id, snapshot.guild_id, len(snapshot.members), len(cache),
    )


def get_snapshot(
    bot_data: dict[str, Any],
    message_id: int,
) -> Optional[UndeployedSnapshot]:
    """
    Retrieve a cached snapshot, or None if expired/missing.

    Performs an inline TTL check so a stale entry is treated as missing
    even if no _evict_expired pass has happened since it aged out.
    """
    cache = bot_data.get(_BOT_DATA_KEY)
    if not cache:
        return None
    snapshot = cache.get(message_id)
    if snapshot is None:
        return None
    if _is_expired(snapshot):
        # Lazy eviction: drop it now too, so subsequent lookups don't
        # repeat the check.
        cache.pop(message_id, None)
        return None
    return snapshot


def _is_expired(snapshot: UndeployedSnapshot) -> bool:
    age = datetime.now(timezone.utc) - snapshot.created_at
    return age > timedelta(hours=UNDEPLOYED_CACHE_TTL_HOURS)


def _evict_expired(cache: dict[int, UndeployedSnapshot]) -> None:
    """Remove all expired entries. Called on every write to keep size bounded."""
    expired = [mid for mid, snap in cache.items() if _is_expired(snap)]
    for mid in expired:
        cache.pop(mid, None)
    if expired:
        log.debug("Evicted %d expired undeployed snapshots.", len(expired))
