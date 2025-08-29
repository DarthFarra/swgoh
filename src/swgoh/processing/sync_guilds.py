# src/swgoh/processing/sync_guilds.py
import os
import json
import base64
import time
import logging
from typing import Any, Dict, List, Tuple, Optional
from urllib import request, error

import gspread
from google.oauth2.service_account import Credentials

# =========================
# Configuración
# =========================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"
COMLINK_BASE = os.getenv("COMLINK_BASE", "http://comlink:3000").rstrip("/")

SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")

# Columnas que NO se deben tocar en Guilds
PRESERVE_COLS = {"ROTE", "nombre abreviado"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [sync_guilds] %(message)s",
)
log = logging.getLogger("sync_guilds")


# =========================
# Google Sheets helpers
# =========================
def _load_service_account_creds() -> Credentials:
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Variable de entorno {SERVICE_ACCOUNT_ENV} no definida")

    def try_json(s: str) -> Optional[dict]:
        try:
            return json.loads(s)
        except Exception:
            return None

    info = try_json(raw)
    if info is None:
        # ¿base64?
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = try_json(decoded)
        except Exception:
            info = None

    if info is None:
        # ¿ruta a archivo?
        try:
            with open(raw, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception as e:
            raise RuntimeError(
                f"No pude interpretar {SERVICE_ACCOUNT_ENV} como JSON/base64/ruta: {e}"
            )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)


def _open_ss():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID en variables de entorno")
    creds = _load_service_account_creds()
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def _read_all_values(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows


def _header_index_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip().lower(): i for i, h in enumerate(headers)}


def _get_cell(row: List[str], hmap: Dict[str, int], name: str) -> str:
    i = hmap.get(name.lower(), -1)
    return row[i].strip() if 0 <= i < len(row) else ""


# =========================
# HTTP / Comlink helpers
# =========================
def _post_json(path: str, payload: dict, timeout: float = 30.0) -> dict:
    url = f"{COMLINK_BASE}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))


def _post_json_retry(path: str, payloads: List[dict], attempts: int = 4, base_sleep: float = 0.8) -> dict:
    """
    Intenta múltiples payloads (fallback) y reintenta en 5xx/errores temporales.
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        for p in payloads:
            try:
                return _post_json(path, p)
            except error.HTTPError as e:
                # Si es 4xx, probamos siguiente payload; si no hay más payloads, reintento con backoff
                log.warning("HTTP %s en %s con payload=%s", e.code, path, list(p.keys()))
                last_exc = e
                if 400 <= e.code < 500:
                    # Para 4xx probamos el siguiente payload; si agotamos, abortamos salvo que queden intentos
                    continue
            except Exception as ex:
                log.warning("Error en POST %s: %s", path, ex)
                last_exc = ex
                # probamos siguiente payload
                continue

        # Si llegamos aquí, ningún payload ha funcionado; si quedan intentos, backoff
        if attempt < attempts:
            sleep_s = base_sleep * (attempt ** 1.5)
            time.sleep(sleep_s)
        else:
            break
    # Si agotamos, relanzar último error
    if last_exc:
        raise last_exc
    return {}


# =========================
# Logica de escritura que preserva columnas personalizadas
# =========================
def write_guilds_preserving_custom_cols(ws, new_rows: List[Dict[str, Any]], key_col: str = "Guild Id"):
    """
    new_rows: lista de dicts con datos de gremios, ej:
      {"Guild Id": "...", "Guild Name": "...", "Members": 50, "GP": 450_000_000, "Last Raid Id": "...", "Last Raid Score": 12345, ...}

    - No borra ni sobreescribe las columnas "ROTE" y "nombre abreviado".
    - Actualiza filas existentes (match por key_col; si falta, hace fallback a "Guild Name").
    - Agrega columnas nuevas si aparecen en new_rows.
    - Inserta filas nuevas si hay gremios no presentes.
    """
    headers_old, rows_old = _read_all_values(ws)
    hmap_old = _header_index_map(headers_old)

    def row_key_from_old(row: List[str]) -> str:
        key = _get_cell(row, hmap_old, key_col)
        if not key:
            key = _get_cell(row, hmap_old, "Guild Name")
        return key

    old_index: Dict[str, Dict[str, str]] = {}
    for row in rows_old:
        k = row_key_from_old(row)
        if not k:
            continue
        old_index[k] = {headers_old[i]: row[i] for i in range(len(headers_old))}

    # Cabecera final
    headers_final = headers_old[:] if headers_old else []
    cols_from_new = set()
    for r in new_rows:
        cols_from_new |= set(r.keys())
    for col in sorted(cols_from_new):
        if col not in headers_final:
            headers_final.append(col)
    for col in PRESERVE_COLS:
        if col not in headers_final:
            headers_final.append(col)

    hmap_final = {h: i for i, h in enumerate(headers_final)}

    out_rows: List[List[Any]] = []
    for r in new_rows:
        key = str(r.get(key_col) or r.get("Guild Name") or "").strip()
        preserved = old_index.get(key, {})
        row_vals = [""] * len(headers_final)
        for col in headers_final:
            if col in PRESERVE_COLS:
                row_vals[hmap_final[col]] = preserved.get(col, "")
            else:
                row_vals[hmap_final[col]] = r.get(col, preserved.get(col, ""))
        out_rows.append(row_vals)

    # Escribir
    if headers_final != headers_old:
        ws.update("1:1", [headers_final])

    # Limpiar datos actuales y volcar nuevos
    if rows_old:
        ws.resize(rows=1)  # deja solo cabecera
    if out_rows:
        ws.append_rows(out_rows, value_input_option="RAW")


# =========================
# Fetch de datos de Guild
# =========================
def _extract_last_raid(summary: Any) -> Tuple[str, str]:
    """
    summary esperado: lista de objetos con {identifier: <obj|str>, totalPoints: <int>}
    Si hay varios, concatenamos por '; '.
    """
    if not isinstance(summary, list) or not summary:
        return "", ""
    ids, scores = [], []
    for item in summary:
        ident = item.get("identifier")
        if isinstance(ident, (dict, list)):
            ident_str = json.dumps(ident, ensure_ascii=False, separators=(",", ":"))
        else:
            ident_str = "" if ident is None else str(ident)
        ids.append(ident_str)
        scores.append(str(item.get("totalPoints", "")))
    return "; ".join(ids), "; ".join(scores)


def fetch_guild(guild_id: str) -> Dict[str, Any]:
    """
    Llama a /guild con includeRecentGuildActivityInfo=true.
    Intenta dos formatos de payload por compatibilidad:
      1) {"guildId": "<id>", "includeRecentGuildActivityInfo": true}
      2) {"identifier": {"guildId": "<id>"}, "includeRecentGuildActivityInfo": true}
    """
    payloads = [
        {"guildId": guild_id, "includeRecentGuildActivityInfo": True},
        {"identifier": {"guildId": guild_id}, "includeRecentGuildActivityInfo": True},
    ]
    data = _post_json_retry("/guild", payloads)
    return data or {}


def build_guild_row(g: Dict[str, Any]) -> Dict[str, Any]:
    # campos habituales
    gid = str(g.get("id") or g.get("guildId") or "")
    gname = str(g.get("name") or g.get("guildName") or "")
    members = g.get("memberCount") or g.get("members") or ""
    gp = g.get("galacticPower") or g.get("gp") or ""

    last_ids, last_scores = _extract_last_raid(g.get("lastRaidPointsSummary"))

    row = {
        "Guild Id": gid,
        "Guild Name": gname,
        "Members": members,
        "GP": gp,
        "Last Raid Id": last_ids,
        "Last Raid Score": last_scores,
    }
    return row


# =========================
# Main
# =========================
def _collect_guild_ids(ws) -> List[str]:
    headers, rows = _read_all_values(ws)
    if not headers:
        return []
    hmap = _header_index_map(headers)

    possibles = ["Guild Id", "Guild ID", "guild id", "guild_id", "guildid"]
    key_col = None
    for p in possibles:
        if p.lower() in hmap:
            key_col = p
            break
    if not key_col:
        raise RuntimeError(
            f'La hoja "{SHEET_GUILDS}" no tiene columna "Guild Id". '
            f'Añádela (o usa una de: {", ".join(possibles)})'
        )

    ids: List[str] = []
    for r in rows:
        idx = hmap[key_col.lower()]
        v = r[idx].strip() if 0 <= idx < len(r) else ""
        if v:
            ids.append(v)
    # únicos preservando orden
    out, seen = [], set()
    for v in ids:
        if v not in seen:
            out.append(v); seen.add(v)
    return out


def main():
    ss = _open_ss()
    ws_guilds = ss.worksheet(SHEET_GUILDS)

    guild_ids = _collect_guild_ids(ws_guilds)
    if not guild_ids:
        log.info("No se encontraron Guild Id en la hoja %s. Nada que hacer.", SHEET_GUILDS)
        return

    log.info("Procesando %d gremio(s)…", len(guild_ids))

    new_rows: List[Dict[str, Any]] = []
    for gid in guild_ids:
        try:
            g = fetch_guild(gid)
            if not g:
                log.warning("Sin datos para guildId=%s", gid)
                continue
            row = build_guild_row(g)
            # Asegura Guild Id si la API lo devolvió vacío
            if not row.get("Guild Id"):
                row["Guild Id"] = gid
            new_rows.append(row)
        except Exception as e:
            log.error("Error obteniendo guildId=%s: %s", gid, e)

    if not new_rows:
        log.info("No hay filas nuevas para escribir.")
        return

    write_guilds_preserving_custom_cols(ws_guilds, new_rows, key_col="Guild Id")
    log.info("Actualización de Guilds completada (preservando %s).", ", ".join(sorted(PRESERVE_COLS)))


if __name__ == "__main__":
    main()
