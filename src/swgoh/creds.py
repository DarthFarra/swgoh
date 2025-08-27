import os, json, base64
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def load_credentials() -> Credentials:
    def from_info(info):
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    saf = os.getenv("SERVICE_ACCOUNT_FILE")
    if saf:
        if os.path.exists(saf):
            return Credentials.from_service_account_file(saf, scopes=SCOPES)
        txt = saf.strip()
        if txt.startswith("{"):
            return from_info(json.loads(txt))
        try:
            dec = base64.b64decode(saf).decode("utf-8")
            if dec.strip().startswith("{"):
                return from_info(json.loads(dec))
        except Exception:
            pass

    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_CREDENTIALS_JSON")
    if raw:
        return from_info(json.loads(raw))

    b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
    if b64:
        dec = base64.b64decode(b64).decode("utf-8")
        return from_info(json.loads(dec))

    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    if os.path.exists(gac):
        return Credentials.from_service_account_file(gac, scopes=SCOPES)

    raise SystemExit("Credenciales Google no encontradas.")
