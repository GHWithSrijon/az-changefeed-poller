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

from config import AWS_REGION, SQS_QUEUE_URL , LOCALSTACK_ENDPOINT

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


def build_s3_create_event(record: dict, account_name: str) -> dict:
    """Map an Azure BlobCreated change feed record to an S3-compatible event envelope."""
    event_time = record.get("eventTime") or datetime.now(timezone.utc).isoformat()
    subject = record.get("subject", "")

    # subject: /blobServices/default/containers/<container>/blobs/<blob>
    blob_name = subject.split("/blobs/", 1)[-1] if "/blobs/" in subject else subject
    container_name = (
        subject.split("/containers/", 1)[1].split("/blobs/")[0]
        if "/containers/" in subject
        else ""
    )

    data = record.get("data", {})

    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "azure:blob",
                "eventTime": event_time,
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                        "name": f"{account_name}-{container_name}",
                        "arn": f"arn:aws:s3:::{account_name}-{container_name}",
                    },
                    "object": {
                        "key": blob_name,
                        "size": data.get("contentLength", 0),
                        "eTag": data.get("eTag", "").strip('"'),
                    },
                },
                "_azure": {
                    "accountName": account_name,
                    "containerName": container_name,
                    "blobName": blob_name,
                    "originalEventType": record.get("eventType", ""),
                    "originalSubject": subject,
                },
            }
        ]
    }


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
    blob_key = event["Records"][0]["s3"]["object"]["key"]
    logger.info("SQS message sent [MessageId=%s] for blob: %s", response["MessageId"], blob_key)
