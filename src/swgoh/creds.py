import os, json, base64
from google.oauth2.service_account import Credentials


SCOPES = [
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive",
]


def load_credentials() -> Credentials:
def from_info(info):
return Credentials.from_service_account_info(info, scopes=SCOPES)


# 1) SERVICE_ACCOUNT_FILE (ruta | JSON | base64)
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


# 2) GOOGLE_SERVICE_ACCOUNT_JSON
raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if raw:
return from_info(json.loads(raw))


# 3) GOOGLE_SERVICE_ACCOUNT_BASE64
b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
if b64:
dec = base64.b64decode(b64).decode("utf-8")
return from_info(json.loads(dec))


# 4) GOOGLE_APPLICATION_CREDENTIALS
gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
if os.path.exists(gac):
return Credentials.from_service_account_file(gac, scopes=SCOPES)


raise SystemExit("Credenciales Google no encontradas.")
