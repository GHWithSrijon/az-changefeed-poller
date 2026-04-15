import os

from dotenv import load_dotenv

load_dotenv()

AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"]
AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
AZURE_STORAGE_ACCOUNT_URL = os.environ["AZURE_STORAGE_ACCOUNT_URL"]

# Derived from URL: https://mystorageaccount.blob.core.windows.net -> mystorageaccount
AZURE_ACCOUNT_NAME = AZURE_STORAGE_ACCOUNT_URL.split("//")[-1].split(".")[0]

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "")

CURSOR_FILE = os.environ.get("CURSOR_FILE", ".changefeed_cursor.json")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

# Cursor storage backend: "local" (default) or "s3"
CURSOR_STORAGE = os.environ.get("CURSOR_STORAGE", "local")
CURSOR_S3_BUCKET = os.environ.get("CURSOR_S3_BUCKET", "")
CURSOR_S3_KEY = os.environ.get("CURSOR_S3_KEY", "changefeed_cursor.json")
