import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from config import (
    CURSOR_FILE,
    CURSOR_S3_BUCKET,
    CURSOR_S3_KEY,
    CURSOR_STORAGE,
    LOCALSTACK_ENDPOINT,
    AWS_REGION,
)

logger = logging.getLogger("az-changefeed-poller.cursor")

_STATUS_OK = "ok"
_STATUS_FAILED = "failed"


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------

class _LocalBackend:
    def read(self) -> dict | None:
        if not os.path.exists(CURSOR_FILE):
            return None
        try:
            with open(CURSOR_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to read cursor file (%s): %s", CURSOR_FILE, exc)
            return None

    def write(self, payload: dict) -> None:
        try:
            with open(CURSOR_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except OSError as exc:
            logger.error("Failed to write cursor to %s: %s", CURSOR_FILE, exc)


class _S3Backend:
    def __init__(self) -> None:
        if not CURSOR_S3_BUCKET:
            raise ValueError("CURSOR_S3_BUCKET must be set when CURSOR_STORAGE=s3")
        kwargs = {"region_name": AWS_REGION}
        if LOCALSTACK_ENDPOINT:
            kwargs["endpoint_url"] = LOCALSTACK_ENDPOINT
            kwargs["aws_access_key_id"] = "test"
            kwargs["aws_secret_access_key"] = "test"
        self._client = boto3.client("s3", **kwargs)
        self._bucket = CURSOR_S3_BUCKET
        self._key = CURSOR_S3_KEY

    def read(self) -> dict | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._key)
            return json.loads(response["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                return None
            logger.warning("Failed to read cursor from s3://%s/%s: %s", self._bucket, self._key, exc)
            return None
        except (json.JSONDecodeError, Exception) as exc:
            logger.warning("Failed to parse cursor from S3: %s", exc)
            return None

    def write(self, payload: dict) -> None:
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=json.dumps(payload, indent=2),
                ContentType="application/json",
            )
        except ClientError as exc:
            logger.error("Failed to write cursor to s3://%s/%s: %s", self._bucket, self._key, exc)


def _get_backend() -> _LocalBackend | _S3Backend:
    if CURSOR_STORAGE == "s3":
        return _S3Backend()
    return _LocalBackend()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load() -> str | None:
    """
    Return the continuation token to resume from.

    On a clean run this is the token after the last successfully processed page.
    After a failure it is the start token of the page that failed, so the service
    replays that page on next startup.
    """
    backend = _get_backend()
    data = backend.read()

    if data is None:
        return None

    token = data.get("continuation_token")
    status = data.get("status", _STATUS_OK)
    location = f"s3://{CURSOR_S3_BUCKET}/{CURSOR_S3_KEY}" if CURSOR_STORAGE == "s3" else CURSOR_FILE

    if status == _STATUS_FAILED:
        logger.warning(
            "Previous run ended with a failure. Resuming from failed page token. [source=%s]",
            location,
        )
    else:
        logger.info("Loaded continuation token from %s", location)

    return token


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
    payload = {
        "continuation_token": continuation_token,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _get_backend().write(payload)
