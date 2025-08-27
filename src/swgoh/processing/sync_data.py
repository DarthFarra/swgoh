from typing import Any, Dict, List
from ..sheets import open_or_create, write_sheet
from ..comlink import fetch_metadata, fetch_data_items
from ..config import SHEET_CHARACTERS, SHEET_SHIPS, SHEET_CHARACTERS as CHARS, SHEET_SHIPS as SHIPS, EXCLUDE_BASEID_CONTAINS


def run() -> Dict[str, int]:
meta = fetch_metadata()
version = meta.get("latestGamedataVersion") or meta.get("payload", {}).get("latestGamedataVersion")
if not version: raise SystemExit("No latestGamedataVersion")


du = fetch_data_items(version, "units")
ds = fetch_data_items(version, "skill")


units = du.get("units") or du.get("payload", {}).get("units") or []
skills = ds.get("skill") or ds.get("payload", {}).get("skill") or []


chars_rows: List[List[Any]] = [["baseId","friendlyName","combatType"]]
ships_rows: List[List[Any]] = [["baseId","friendlyName","combatType"]]


for u in units:
bid = (u.get("baseId") or u.get("id") or "").upper()
if not bid: continue
if EXCLUDE_BASEID_CONTAINS and any(s in bid for s in EXCLUDE_BASEID_CONTAINS):
continue
name = u.get("uiName") or u.get("longName") or u.get("name") or u.get("localizedName") or bid
ctype = u.get("combatType") or u.get("combat_type") or 1
try: ctype = int(ctype)
except: ctype = 1
row = [bid, name, ctype]
(ships_rows if ctype==2 else chars_rows).append(row)


ws_chars = open_or_create(SHEET_CHARACTERS)
ws_ships = open_or_create(SHEET_SHIPS)
write_sheet(ws_chars, chars_rows[0], chars_rows[1:])
write_sheet(ws_ships, ships_rows[0], ships_rows[1:])


# Opcional: procesar Zetas/Omicrons (dejamos placeholder ligero)
# Puedes importar de tu script actual la lógica si necesitas el detalle completo


return {"characters": len(chars_rows)-1, "ships": len(ships_rows)-1}


if __name__ == "__main__":
print(run())
