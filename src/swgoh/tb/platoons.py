# src/swgoh/tb/platoons.py
"""
Empty-platoon-slot extraction from TB snapshots.

The TB export carries platoon state under reconZoneStatus[*].platoon[*].squad[*].unit[*].
This module operates on the TYPED parsed form (ReconZone/Platoon/PlatoonSquad/
PlatoonSlot) populated by the parser into TBSnapshot.recon_zones.

Design rules (consistent with the rest of tb/*):
  - Pure functions, no I/O.
  - Result types are small frozen dataclasses with denormalized data,
    so the formatter doesn't need to re-resolve identifiers.
  - Module sits alongside analysis.py rather than inside it because the
    data shape (slot-level vs zone-level) is different enough that
    co-locating would muddle the file. Analysis.py owns "what's the
    score, what's the gap, who hasn't deployed"; this module owns
    "which platoon slots are empty."

What this module does NOT do:
  - Join against ROTE assignments. That's an I/O step the bot's service
    layer handles; the formatter then combines.
  - Translate technical IDs to friendly names. The Characters sheet
    and MapConfig live outside the pure-function layer.
  - Filter by zone state. The caller decides which zones are "active"
    (the auto-summary already has active_planet_zones()).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from .models import TBSnapshot

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The recon zone id is the conflict zone id with this suffix. We strip
# it to look up the parent conflict zone in MapConfig (which keys
# planets by conflict zone id, not recon zone id).
#
# Example:
#   tb3_mixed_phase05_conflict01_recon01 → tb3_mixed_phase05_conflict01
#
# If CG ever changes the suffix, this constant centralizes the fix.
_RECON_SUFFIX: str = "_recon01"


# Map from the snapshot's platoon id → officer-facing "Operation #N".
#
# PROVISIONAL — based on Theory A (array position in export → in-game
# operation number). Every recon zone in our examples had the platoons
# in reverse-numeric order: [tb3-platoon-6, ..., tb3-platoon-1]. If
# position 0 → Operation #1, then tb3-platoon-6 IS Operation #1, etc.
#
# This needs in-game verification on the next TB cycle:
#   * Open any active recon planet (e.g. Mandalore) in the game UI.
#   * Look at "Operation #1" — note its first slot's character.
#   * Compare to the next snapshot's tb3-platoon-6 first slot.
#   * If they match → this mapping is correct, no action needed.
#   * If they differ → the mapping is probably the identity:
#       {f"tb3-platoon-{n}": n for n in range(1, 7)}
#     Update this constant accordingly.
#
# Why not a function: a flat dict is the simplest auditable form. If
# the mapping is ever non-trivial (e.g. varies per planet, which would
# be Theory C), this becomes a dispatch table without a code structure
# change.
PLATOON_ID_TO_OPERATION: Dict[str, int] = {
    "tb3-platoon-6": 1,
    "tb3-platoon-5": 2,
    "tb3-platoon-4": 3,
    "tb3-platoon-3": 4,
    "tb3-platoon-2": 5,
    "tb3-platoon-1": 6,
}


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class EmptyPlatoonSlot:
    """
    One unfilled platoon slot, denormalized for the formatter.

    Fields:
      conflict_zone_id   Parent conflict zone id (e.g. "tb3_mixed_phase05_conflict01").
                         Use this to look up planet_name in MapConfig.
      recon_zone_id      Raw recon zone id (with _recon01 suffix). Kept for
                         debugging/log clarity.
      platoon_number     1-6. The RAW number parsed from "tb3-platoon-N"
                         in the export. NOT the officer-facing operation
                         number; see `operation_number` for that.
      operation_number   1-6. The officer-facing "Operation #N" number,
                         derived via PLATOON_ID_TO_OPERATION. THIS is
                         the value the formatter displays and the ROTE
                         join key uses.
      squad_number       1-3. Internal squad index — not displayed in
                         the message but kept for sorting and future use.
      unit_id            Technical character id (e.g. "CAPITALLEVIATHAN").
                         Use this to look up friendly name in the
                         Characters sheet.
    """
    conflict_zone_id: str
    recon_zone_id: str
    platoon_number: int
    operation_number: int
    squad_number: int
    unit_id: str


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def empty_platoon_slots(
    snap: TBSnapshot,
    active_zone_ids: Optional[List[str]] = None,
) -> List[EmptyPlatoonSlot]:
    """
    Return every empty platoon slot in the snapshot's recon_zones.

    Args:
      snap: parsed TB snapshot. Must have recon_zones populated; if it
        doesn't (parser is from before this feature, or the snapshot has
        no recon zones), returns an empty list.
      active_zone_ids: optional whitelist of CONFLICT zone ids to include.
        If None, returns every empty slot regardless of zone state.
        Pass the result of active_planet_zones() to filter to the
        current phase only.

    Returns:
      List of EmptyPlatoonSlot. Sorted by (conflict_zone_id ascending,
      platoon_number ascending, squad_number ascending). Stable order
      so the formatter can group consecutively by zone+platoon without
      a separate sort.

      Empty list if no empty slots exist or whitelist excludes
      everything.
    """
    recon_zones = getattr(snap, "recon_zones", None)
    if not recon_zones:
        return []

    whitelist = set(active_zone_ids) if active_zone_ids is not None else None
    out: List[EmptyPlatoonSlot] = []

    for recon_zone_id, recon in recon_zones.items():
        conflict_zone_id = _conflict_from_recon(recon_zone_id)
        if whitelist is not None and conflict_zone_id not in whitelist:
            continue

        for platoon in recon.platoons:
            # Translate the raw export platoon number to the officer-facing
            # Operation number. If the mapping doesn't recognize the id
            # (e.g. CG adds a 7th platoon, or rebrands), we skip the
            # platoon and log it — the message can't render meaningfully
            # without the officer number.
            platoon_id_str = f"tb3-platoon-{platoon.platoon_number}"
            op_number = PLATOON_ID_TO_OPERATION.get(platoon_id_str)
            if op_number is None:
                log.warning(
                    "Unknown platoon id %r in zone %r; cannot map to "
                    "officer-facing Operation number. Skipping platoon.",
                    platoon_id_str, recon_zone_id,
                )
                continue

            for squad in platoon.squads:
                for slot in squad.units:
                    if slot.is_filled:
                        continue
                    if not slot.unit_id:
                        # Malformed slot — already-logged at parse time;
                        # skip silently here.
                        continue
                    out.append(EmptyPlatoonSlot(
                        conflict_zone_id=conflict_zone_id,
                        recon_zone_id=recon_zone_id,
                        platoon_number=platoon.platoon_number,
                        operation_number=op_number,
                        squad_number=squad.squad_number,
                        unit_id=slot.unit_id,
                    ))

    # Stable sort by display order: zone, then OPERATION NUMBER (not
    # raw platoon number — the officer-facing display drives sort), then
    # squad. With Theory-A's reversed mapping, sorting by operation_number
    # produces 1,2,3,...,6 which is the natural reading order in the
    # message.
    out.sort(key=lambda s: (s.conflict_zone_id, s.operation_number, s.squad_number))
    return out


# ---------------------------------------------------------------------------
# Helpers — exposed for formatter use (same suffix convention)
# ---------------------------------------------------------------------------

def _conflict_from_recon(recon_zone_id: str) -> str:
    """
    Strip the recon suffix to get the parent conflict zone id.

    Examples:
      tb3_mixed_phase05_conflict01_recon01           → tb3_mixed_phase05_conflict01
      tb3_mixed_phase04_conflict03_bonus_recon01     → tb3_mixed_phase04_conflict03_bonus

    Returns input unchanged if suffix is absent — caller will fail to
    find the parent in MapConfig, which is the right loud-failure path
    if CG changes the convention.
    """
    if recon_zone_id.endswith(_RECON_SUFFIX):
        return recon_zone_id[: -len(_RECON_SUFFIX)]
    return recon_zone_id
