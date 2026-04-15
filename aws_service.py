import hashlib
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import AWS_REGION, AZURE_STORAGE_ACCOUNT_URL, LOCALSTACK_ENDPOINT, SQS_QUEUE_URL
from schema import AzureBlob, AzureContainer, AzureMetadata, SQSMessage, SQSRecord

logger = logging.getLogger("az-changefeed-poller.aws")

_RETRY_EXCEPTIONS = (ClientError, ConnectionError, TimeoutError)

_retry = retry(
    retry=retry_if_exception_type(_RETRY_EXCEPTIONS),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


def build_client():
    if len(LOCALSTACK_ENDPOINT) > 0:
        return boto3.client(
            "sqs",
            region_name=AWS_REGION,
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
    return boto3.client("sqs", region_name=AWS_REGION)


def build_blob_created_event(record: dict, account_name: str) -> dict:
    """Map an Azure BlobCreated change feed record to an SQSMessage and return as dict."""
    subject = record.get("subject", "")
    blob_name = subject.split("/blobs/", 1)[-1] if "/blobs/" in subject else subject
    container_name = (
        subject.split("/containers/", 1)[1].split("/blobs/")[0]
        if "/containers/" in subject
        else ""
    )
    data = record.get("data", {})
    event_id = hashlib.sha256(subject.encode()).hexdigest()

    message = SQSMessage(
        records=[
            SQSRecord(
                id=event_id,
                event_time=record.get("eventTime") or datetime.now(timezone.utc).isoformat(),
                storage=AzureContainer(
                    name=container_name,
                    account_name=account_name,
                    blob=AzureBlob(
                        name=blob_name,
                        size=data.get("contentLength", 0),
                        e_tag=data.get("eTag", "").strip('"'),
                        url=data.get("url", ""),
                    ),
                ),
                metadata=AzureMetadata(
                    original_event_type=record.get("eventType", ""),
                    original_subject=subject,
                ),
            )
        ]
    )
    return message.to_dict()


@_retry
def send_event(sqs_client, event: dict) -> None:
    """
    Send one event to SQS.

    Retried up to 5 times with exponential backoff (2s → 4s → 8s → 16s → 60s cap)
    on transient AWS errors.  Raises after all attempts are exhausted.
    """
    response = sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(event),
    )
    blob_key = event["Records"][0]["storage"]["blob"]["name"]
    logger.info("SQS message sent [MessageId=%s] for blob: %s", response["MessageId"], blob_key)
