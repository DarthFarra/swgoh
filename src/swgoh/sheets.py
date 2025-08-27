import gspread
from typing import List, Any
from .creds import load_credentials
from .config import SPREADSHEET_ID, SPREADSHEET_NAME


_gc = None
_sh = None


def _client():
global _gc
if _gc is None:
_gc = gspread.authorize(load_credentials())
return _gc


def spreadsheet():
global _sh
if _sh is not None:
return _sh
gc = _client()
if SPREADSHEET_ID:
_sh = gc.open_by_key(SPREADSHEET_ID)
elif SPREADSHEET_NAME:
_sh = gc.open(SPREADSHEET_NAME)
else:
raise SystemExit("Falta SPREADSHEET_ID o SPREADSHEET_NAME")
return _sh


def open_or_create(title: str):
sh = spreadsheet()
try:
return sh.worksheet(title)
except gspread.WorksheetNotFound:
return sh.add_worksheet(title=title, rows=1, cols=1)


def try_get_worksheet(title: str):
sh = spreadsheet()
try:
return sh.worksheet(title)
except gspread.WorksheetNotFound:
return None


def write_sheet(ws, headers: List[str], rows: List[List[Any]], chunk_size: int = 500):
ws.clear()
ws.update(values=[headers], range_name="A1")
if not rows:
return
start_row = 2
ncols = max(1, len(headers))
for i in range(0, len(rows), chunk_size):
block = rows[i:i+chunk_size]
end_row = start_row + len(block) - 1
rng = f"A{start_row}:{gspread.utils.rowcol_to_a1(end_row, ncols)}"
ws.update(values=block, range_name=rng, value_input_option="RAW")
start_row = end_row + 1
