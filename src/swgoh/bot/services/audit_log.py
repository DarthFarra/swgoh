"""Human-readable audit logs for user commands and scheduled jobs."""
from __future__ import annotations

import functools
import inspect
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from telegram import Update
from gspread.exceptions import WorksheetNotFound

from .. import config as cfg
from .sheets import open_ss
from .sheets_io import _with_backoff

log = logging.getLogger(__name__)

_BUFFER_LOCK = threading.Lock()
_BUFFER: dict[str, tuple[list[str], list[list[Any]]]] = {}
_MAX_BATCH_SIZE = 50
_MAX_BUFFER_ROWS = 5000
_DROPPED_ROWS = 0

COMMAND_HEADERS = [
    "trace_id", "parent_trace_id", "username", "id", "platform", "timestamp",
    "command", "arguments", "step", "action", "result", "status", "error",
    "entity", "entity_id", "message_id", "chat_id",
]
SCHEDULED_HEADERS = [
    "trace_id", "timestamp", "job", "arguments", "step", "action", "result",
    "status", "error", "entity", "entity_id",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _text(value: Any, limit: int = 2000) -> str:
    if value is None or value == "":
        return "—"
    text = str(value).replace("\r", " ").replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _ensure_sheet(sheet_name: str, headers: list[str]):
    ss = open_ss()
    try:
        ws = ss.worksheet(sheet_name)
    except WorksheetNotFound:
        log.info("Creating audit sheet %r.", sheet_name)
        ws = ss.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))
        ws.update("A1", [headers])
        return ws

    current = [value.strip() for value in (ws.row_values(1) or [])]
    if current != headers:
        raise RuntimeError(
            f"Audit sheet {sheet_name!r} has unexpected headers; "
            f"expected {headers!r}, got {current!r}"
        )
    return ws


def enqueue_audit_row(sheet_name: str, headers: list[str], row: list[Any]) -> None:
    """Queue an audit row for batched persistence."""
    global _DROPPED_ROWS
    with _BUFFER_LOCK:
        entry = _BUFFER.setdefault(sheet_name, (headers, []))
        if entry[0] != headers:
            log.error("Audit headers changed for %s; discarding row.", sheet_name)
            return
        queued = sum(len(rows) for _, rows in _BUFFER.values())
        if queued >= _MAX_BUFFER_ROWS:
            _DROPPED_ROWS += 1
            if _DROPPED_ROWS == 1 or _DROPPED_ROWS % 100 == 0:
                log.error(
                    "Audit buffer full (%d rows); dropped %d new rows.",
                    _MAX_BUFFER_ROWS, _DROPPED_ROWS,
                )
            return
        entry[1].append(row)


def _append_rows(sheet_name: str, headers: list[str], rows: list[list[Any]]) -> None:
    _with_backoff(
        _ensure_sheet(sheet_name, headers).append_rows,
        [[_text(value) for value in row] for row in rows],
        value_input_option="USER_ENTERED",
    )


def flush_audit_buffer() -> int:
    """Write all queued audit rows in batches, retaining failed rows."""
    with _BUFFER_LOCK:
        pending = {
            sheet: (headers, rows[:])
            for sheet, (headers, rows) in _BUFFER.items()
            if rows
        }
        for sheet in pending:
            _BUFFER[sheet][1].clear()

    written = 0
    for sheet_name, (headers, rows) in pending.items():
        for start in range(0, len(rows), _MAX_BATCH_SIZE):
            batch = rows[start:start + _MAX_BATCH_SIZE]
            try:
                _append_rows(sheet_name, headers, batch)
                written += len(batch)
            except Exception:
                with _BUFFER_LOCK:
                    current = _BUFFER.setdefault(sheet_name, (headers, []))
                    current[1][0:0] = rows[start:]
                log.exception(
                    "Could not flush audit rows to %s; rows retained.",
                    sheet_name,
                )
                break
    return written


def _append(sheet_name: str, headers: list[str], row: list[Any]) -> None:
    """Compatibility wrapper used by trace objects."""
    enqueue_audit_row(sheet_name, headers, row)


class CommandTrace:
    def __init__(
        self,
        *,
        username: str,
        user_id: Any,
        platform: str,
        command: str,
        arguments: str,
        parent_trace_id: str = "",
        message_id: Any = "",
        chat_id: Any = "",
    ) -> None:
        self.trace_id = uuid.uuid4().hex
        self._base = [
            self.trace_id, parent_trace_id, username, user_id, platform, _now(),
            command, arguments, "", "", "", "", "", "", "", message_id, chat_id,
        ]

    def event(
        self,
        *,
        step: str,
        action: str,
        result: str = "",
        status: str = "success",
        error: str = "",
        entity: str = "",
        entity_id: Any = "",
    ) -> None:
        row = self._base.copy()
        row[5] = _now()
        row[8:15] = [step, action, result, status, error, entity, entity_id]
        _append(cfg.COMMANDS_LOG_SHEET, COMMAND_HEADERS, row)


class ScheduledTrace:
    def __init__(self, *, job: str, arguments: str = "", entity: str = "", entity_id: Any = ""):
        self.trace_id = uuid.uuid4().hex
        self.job = job
        self.arguments = arguments
        self.entity = entity
        self.entity_id = entity_id

    def event(
        self,
        *,
        step: str,
        action: str,
        result: str = "",
        status: str = "success",
        error: str = "",
    ) -> None:
        _append(
            cfg.SCHEDULED_LOG_SHEET,
            SCHEDULED_HEADERS,
            [
                self.trace_id, _now(), self.job, self.arguments, step, action,
                result, status, error, self.entity, self.entity_id,
            ],
        )


def _update_details(update: object) -> tuple[str, Any, str, str, str, Any, Any]:
    if not isinstance(update, Update):
        return "unknown", "", "telegram", type(update).__name__, "", "", ""
    user = update.effective_user
    callback = update.callback_query
    message = update.effective_message
    if callback:
        command = callback.data or "callback"
        arguments = f"callback={callback.data or '—'}"
        message_id = callback.message.message_id if callback.message else ""
    else:
        text = (message.text if message else "") or ""
        parts = text.split(maxsplit=1)
        command = parts[0] if parts else type(update).__name__
        arguments = parts[1] if len(parts) > 1 else ""
        message_id = message.message_id if message else ""
    return (
        user.username if user and user.username else "",
        user.id if user else "",
        "telegram",
        command, arguments,
        message_id,
        update.effective_chat.id if update.effective_chat else "",
    )


def wrap_telegram_callback(callback: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Add receipt/completion rows around every registered Telegram handler."""
    @functools.wraps(callback)
    async def audited(update, context, *args, **kwargs):
        username, user_id, platform, command, arguments, message_id, chat_id = _update_details(update)
        last = context.application.bot_data.setdefault("__audit_last_trace__", {})
        parent = last.get((platform, user_id), "")
        trace = CommandTrace(
            username=username, user_id=user_id, platform=platform,
            command=command, arguments=arguments,
            parent_trace_id=parent, message_id=message_id, chat_id=chat_id,
        )
        last[(platform, user_id)] = trace.trace_id
        trace.event(step="received", action=f"Recibir {command}", result="Interacción aceptada")
        try:
            result = await callback(update, context, *args, **kwargs)
        except Exception as exc:
            trace.event(
                step="failed", action=f"Ejecutar {command}", status="failed",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        trace.event(step="completed", action=f"Completar {command}", result="Handler finalizado")
        return result
    return audited


def wrap_handler(handler):
    callback = getattr(handler, "callback", None)
    if callback is not None and inspect.iscoroutinefunction(callback):
        handler.callback = wrap_telegram_callback(callback)
    return handler


def wrap_discord_callback(callback: Callable[..., Awaitable[Any]], command: str):
    """Add the same human-readable audit boundary to a Discord command."""
    @functools.wraps(callback)
    async def audited(interaction, *args, **kwargs):
        user = interaction.user
        trace = CommandTrace(
            username=getattr(user, "name", ""),
            user_id=getattr(user, "id", ""),
            platform="discord",
            command=f"/{command}",
            arguments="",
            message_id=getattr(interaction, "id", ""),
            chat_id=getattr(getattr(interaction, "channel", None), "id", ""),
        )
        trace.event(step="received", action=f"Recibir /{command}", result="Interacción aceptada")
        try:
            result = await callback(interaction, *args, **kwargs)
        except Exception as exc:
            trace.event(
                step="failed", action=f"Ejecutar /{command}", status="failed",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        trace.event(step="completed", action=f"Completar /{command}", result="Handler finalizado")
        return result
    return audited


def scheduled_job(job: str, arguments: str = ""):
    """Decorate an async scheduled entry point with human-readable audit rows."""
    def decorator(function):
        @functools.wraps(function)
        async def audited(*args, **kwargs):
            trace = ScheduledTrace(job=job, arguments=arguments)
            trace.event(step="started", action=f"Iniciar trabajo programado: {job}")
            try:
                result = await function(*args, **kwargs)
            except Exception as exc:
                trace.event(
                    step="failed", action=f"Ejecutar trabajo programado: {job}",
                    status="failed", error=f"{type(exc).__name__}: {exc}",
                )
                raise
            if result is False:
                trace.event(
                    step="failed", action=f"Finalizar trabajo programado: {job}",
                    result="El trabajo terminó con errores",
                    status="failed",
                )
            else:
                trace.event(
                    step="completed", action=f"Finalizar trabajo programado: {job}",
                    result="Trabajo completado",
                )
            return result
        return audited
    return decorator
