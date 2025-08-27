from typing import Any, Dict, List
from ..sheets import open_or_create, write_sheet
from ..comlink import fetch_metadata, fetch_data_items
from ..config import SHEET_CHARACTERS, SHEET_SHIPS, EXCLUDE_BASEID_CONTAINS


def run() -> Dict[str, int]:
    # 1) Obtener versión de datos
    meta = fetch_metadata()
    version = (
        meta.get("latestGamedataVersion")
        or meta.get("payload", {}).get("latestGamedataVersion")
        or meta.get("data", {}).get("latestGamedataVersion")
    )
    if not version:
        raise SystemExit("No latestGamedataVersion")

    # 2) Descargar items
    du = fetch_data_items(version, "units")
    ds = fetch_data_items(version, "skill")  # reservado por si lo usas más tarde

    units = du.get("units") or du.get("payload", {}).get("units") or []
    # skills = ds.get("skill") or ds.get("payload", {}).get("skill") or []  # no usado aquí

    # 3) Preparar filas para Characters y Ships
    headers = ["baseId", "friendlyName", "combatType"]
    chars_rows: List[List[Any]] = []
    ships_rows: List[List[Any]] = []

    for u in units:
        bid = (u.get("baseId") or u.get("id") or "").upper()
        if not bid:
            continue
        if EXCLUDE_BASEID_CONTAINS and any(substr in bid for substr in EXCLUDE_BASEID_CONTAINS):
            continue

        name = (
            u.get("uiName")
            or u.get("longName")
            or u.get("name")
            or u.get("localizedName")
            or bid
        )
        ctype = u.get("combatType") or u.get("combat_type") or 1
        try:
            ctype = int(ctype)
        except Exception:
            ctype = 1

        row = [bid, str(name), ctype]
        if ctype == 2:
            ships_rows.append(row)
        else:
            chars_rows.append(row)

    # 4) Escribir a Sheets
    ws_chars = open_or_create(SHEET_CHARACTERS)
    ws_ships = open_or_create(SHEET_SHIPS)
    write_sheet(ws_chars, headers, chars_rows)
    write_sheet(ws_ships, headers, ships_rows)

    return {"characters": len(chars_rows), "ships": len(ships_rows)}


if __name__ == "__main__":
    print(run())
