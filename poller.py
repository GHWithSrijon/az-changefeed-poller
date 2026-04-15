"""
Azure Change Feed Poller — orchestration layer.

Coordinates the Azure ChangeFeedClient, AWS SQS emitter, and cursor cache
to continuously forward BlobCreated events as S3-compatible SQS messages.

Fault handling:
- SQS sends are retried with exponential backoff (see aws_service.py).
- On permanent failure the start token of the failing page is persisted and
  the service exits so the next startup replays exactly that page.
"""

import logging
import sys
import time

import aws_service
import azure_service
import cursor
from config import AZURE_ACCOUNT_NAME, CURSOR_FILE, POLL_INTERVAL_SECONDS, SQS_QUEUE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("az-changefeed-poller")


def poll_once(azure_client, sqs_client, continuation_token: str | None) -> str | None:
    """
    Fetch and process all new change feed pages.

    Saves a progress cursor after each page so that an interruption between pages
    does not reprocess already-forwarded events.

    On an unrecoverable SQS failure (retries exhausted):
      - saves the page's start token as a failure cursor
      - raises to halt the service
    """
    total_pages = 0
    total_forwarded = 0

    for page_start_token, events, page_end_token in azure_service.iter_changes(
        azure_client, continuation_token
    ):
        total_pages += 1

        for event in events:
            event_type: str = event.get("eventType", "")
            logger.info(
                "Processing event eventType=%s subject=%s",
                event_type,
                event.get("subject", ""),
            )

            if event_type != "BlobCreated":
                logger.debug("Skipping non-BlobCreated event: %s", event_type)
                continue

            sqs_event = aws_service.build_blob_created_event(event, AZURE_ACCOUNT_NAME)
            try:
                aws_service.send_event(sqs_client, sqs_event)
                total_forwarded += 1
            except Exception as exc:
                # Retries exhausted — record the page start token so we replay
                # this page on next startup, then stop.
                logger.error(
                    "Permanent SQS failure after all retries [page_start_token=%s]: %s",
                    page_start_token,
                    exc,
                    exc_info=True,
                )
                cursor.save_failure(page_start_token)
                raise

        # Full page processed — advance the cursor past it.
        cursor.save_progress(page_end_token)
        continuation_token = page_end_token

    if total_pages == 0:
        logger.info("No new change feed events.")
    else:
        logger.info(
            "Poll complete — %d page(s), %d BlobCreated event(s) forwarded to SQS.",
            total_pages,
            total_forwarded,
        )

    return continuation_token


def run_poller() -> None:
    logger.info("Starting Azure Change Feed Poller")
    logger.info("  Account  : %s", AZURE_ACCOUNT_NAME)
    logger.info("  SQS URL  : %s", SQS_QUEUE_URL)
    logger.info("  Interval : %ds", POLL_INTERVAL_SECONDS)
    logger.info("  Cursor   : %s", CURSOR_FILE)

    azure_client = azure_service.build_client()
    sqs_client = aws_service.build_client()
    continuation_token = cursor.load()

    while True:
        try:
            continuation_token = poll_once(azure_client, sqs_client, continuation_token)
        except Exception as exc:
            logger.critical("Unrecoverable error — stopping service: %s", exc)
            sys.exit(1)

        logger.debug("Sleeping %ds until next poll.", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)
