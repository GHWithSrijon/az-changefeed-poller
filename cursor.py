import json
import logging
import os

from config import CURSOR_FILE

logger = logging.getLogger("az-changefeed-poller.cursor")

_STATUS_OK = "ok"
_STATUS_FAILED = "failed"


def load() -> str | None:
    """
    Return the continuation token to resume from.

    On a clean run this is the token after the last successfully processed page.
    After a failure it is the start token of the page that failed, so the service
    replays that page on next startup.
    """
    if not os.path.exists(CURSOR_FILE):
        return None
    try:
        with open(CURSOR_FILE, "r") as f:
            data = json.load(f)
        token = data.get("continuation_token")
        status = data.get("status", _STATUS_OK)
        if status == _STATUS_FAILED:
            logger.warning(
                "Previous run ended with a failure. Resuming from failed page token."
            )
        else:
            logger.info("Loaded continuation token from %s", CURSOR_FILE)
        return token
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read cursor file (%s); starting fresh: %s", CURSOR_FILE, exc)
        return None


def save_progress(continuation_token: str | None) -> None:
    """Persist the token for the end of a successfully processed page."""
    _write(continuation_token, _STATUS_OK)
    logger.debug("Cursor advanced (status=ok).")


def save_failure(continuation_token: str | None) -> None:
    """
    Persist the start token of the page that caused a failure.
    The service will replay this page on next startup.
    """
    _write(continuation_token, _STATUS_FAILED)
    logger.error(
        "Failure cursor saved (status=failed, token=%s). "
        "Service will replay this page on next startup.",
        continuation_token,
    )


def _write(continuation_token: str | None, status: str) -> None:
    try:
        with open(CURSOR_FILE, "w") as f:
            json.dump({"continuation_token": continuation_token, "status": status}, f, indent=2)
    except OSError as exc:
        logger.error("Failed to write cursor to %s: %s", CURSOR_FILE, exc)
