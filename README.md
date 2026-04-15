# az-changefeed-poller

Polls the Azure Blob Storage Change Feed for `BlobCreated` events and forwards them as structured messages to an AWS SQS queue. Supports resumable polling via a local file-based cursor backed by the Azure SDK continuation token.

---

## Project Structure

```
az-changefeed-poller/
├── main.py            # Entry point
├── poller.py          # Orchestration loop
├── azure_service.py   # Azure ChangeFeedClient — segment discovery & pagination
├── aws_service.py     # AWS SQS client — event mapping & send with retry
├── schema.py          # SQS message dataclass schema
├── cursor.py          # File-based continuation token cache
├── config.py          # Environment variable configuration
└── setup_dev.sh       # Dev environment setup — loads .env, starts LocalStack, creates SQS queue
```

---

## Setup

### Development

**Prerequisites**

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) — authenticated via `az login`
- [LocalStack](https://github.com/localstack/localstack)
- [awslocal](https://github.com/localstack/awscli-local) CLI wrapper

**Steps**

```bash
# 1. Clone and enter the project
cd az-changefeed-poller

# 2. Install dependencies
uv sync

# 3. Copy .env.example and fill in your Azure credentials and settings
cp .env.example .env

# 4. Run the setup script — loads .env, starts LocalStack, creates the SQS queue
./setup_dev.sh

# 5. Start the service
uv run main.py
```

---

### Production

**Prerequisites**

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)
- An Azure Storage account with [Change Feed enabled](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-change-feed#enable-and-disable-the-change-feed)
- An Azure service principal with `Storage Blob Data Reader` role on the storage account
- An AWS SQS queue

**Steps**

```bash
# 1. Clone and enter the project
cd az-changefeed-poller

# 2. Install dependencies
uv sync

# 3. Copy the example env file and fill in all values
cp .env.example .env
```

Update `.env` with your credentials:

```env
AZURE_TENANT_ID=<your-tenant-id>
AZURE_CLIENT_ID=<your-client-id>
AZURE_CLIENT_SECRET=<your-client-secret>
AZURE_STORAGE_ACCOUNT_URL=https://<your-storage-account>.blob.core.windows.net

AWS_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/<account-id>/<queue-name>

CURSOR_FILE=.changefeed_cursor.json
POLL_INTERVAL_SECONDS=60
```

```bash
# 4. Start the service
uv run main.py
```

The service loads `.env` automatically via `python-dotenv`.

---

## Configuration

Copy `.env.example` to `.env` and fill in values, or run `setup_dev.sh` to populate it automatically.

| Variable                    | Required | Default                      | Description                                      |
|-----------------------------|----------|------------------------------|--------------------------------------------------|
| `AZURE_TENANT_ID`           | yes      | —                            | Azure AD tenant ID                               |
| `AZURE_CLIENT_ID`           | yes      | —                            | Service principal client ID                      |
| `AZURE_CLIENT_SECRET`       | yes      | —                            | Service principal client secret                  |
| `AZURE_STORAGE_ACCOUNT_URL` | yes      | —                            | Blob endpoint e.g. `https://<account>.blob.core.windows.net` |
| `AWS_REGION`                | no       | `us-east-1`                  | AWS region for SQS                               |
| `SQS_QUEUE_URL`             | yes      | —                            | Full SQS queue URL                               |
| `LOCALSTACK_ENDPOINT`       | no       | `""`                         | Set to `http://localhost:4566` for local dev     |
| `POLL_INTERVAL_SECONDS`     | no       | `60`                         | Seconds to sleep between poll cycles             |
| `CURSOR_STORAGE`            | no       | `local`                      | Cursor backend: `local` or `s3`                  |
| `CURSOR_FILE`               | no       | `.changefeed_cursor.json`    | Path to cursor file (when `CURSOR_STORAGE=local`)|
| `CURSOR_S3_BUCKET`          | no*      | —                            | S3 bucket for cursor (when `CURSOR_STORAGE=s3`)  |
| `CURSOR_S3_KEY`             | no       | `changefeed_cursor.json`     | S3 object key for cursor file                    |

> \* Required when `CURSOR_STORAGE=s3`

---

## Cursor Cache

The service maintains a local JSON file to track its position in the change feed. On failure the start token of the failing page is saved so the service replays that page on next startup.

```json
{
  "continuation_token": "...",
  "status": "ok",
  "timestamp": "2026-04-15T09:30:00.123456+00:00"
}
```

`status` is either `ok` (last page succeeded) or `failed` (last page failed after retries — will be replayed).

---

## Fault Handling

SQS sends are retried up to **5 times** with exponential backoff:

| Attempt | Wait   |
|---------|--------|
| 1       | 2s     |
| 2       | 4s     |
| 3       | 8s     |
| 4       | 16s    |
| 5       | 60s    |

After all retries are exhausted the failure cursor is saved and the service exits with code `1`.

---

## Azure Change Feed Event

Raw event returned by `ChangeFeedClient.list_changes()` for a `BlobCreated` operation:

```json
{
  "topic": "/subscriptions/31181d1b-e311-4a57-b199-f2c0c540fe95/resourceGroups/rg-changefeed/providers/Microsoft.Storage/storageAccounts/stchangefeed6f69f51a",
  "subject": "/blobServices/default/containers/raw-data/blobs/uploads/2026/04/15/report.csv",
  "eventType": "BlobCreated",
  "eventTime": "2026-04-15T09:30:00.000Z",
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "data": {
    "api": "PutBlob",
    "clientRequestId": "6d79dbfb-0e37-4fc4-8ef5-fd8e3fa44952",
    "requestId": "831e1650-001e-001b-66ab-eeb76e000000",
    "eTag": "0x8D4BCC2E4835CD0",
    "contentType": "text/csv",
    "contentLength": 524288,
    "blobType": "BlockBlob",
    "url": "https://stchangefeed6f69f51a.blob.core.windows.net/raw-data/uploads/2026/04/15/report.csv",
    "sequencer": "00000000000004420000000000028963",
    "storageDiagnostics": {
      "batchId": "b68529f3-68cd-4744-baa4-3c0498ec19f1"
    }
  },
  "dataVersion": "",
  "metadataVersion": "1"
}
```

---

## SQS Message

The message published to SQS for the above event:

```json
{
  "Records": [
    {
      "id": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
      "eventVersion": "2.1",
      "eventSource": "azure:blob",
      "eventName": "BlobCreated",
      "eventTime": "2026-04-15T09:30:00.000Z",
      "storage": {
        "accountName": "stchangefeed6f69f51a",
        "containerName": "raw-data",
        "blob": {
          "name": "uploads/2026/04/15/report.csv",
          "size": 524288,
          "eTag": "0x8D4BCC2E4835CD0",
          "url": "https://stchangefeed6f69f51a.blob.core.windows.net/raw-data/uploads/2026/04/15/report.csv"
        }
      },
      "metadata": {
        "originalEventType": "BlobCreated",
        "originalSubject": "/blobServices/default/containers/raw-data/blobs/uploads/2026/04/15/report.csv"
      }
    }
  ]
}
```

> **`id`** is a SHA-256 hash of the event `subject` — deterministic for the same blob path, useful for downstream deduplication.

---

## Local Development

Start LocalStack and verify the queue:

```bash
# Start LocalStack
localstack start -d

# List queues
awslocal sqs list-queues

# Tail messages (poll once)
awslocal sqs receive-message \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/my-queue \
  --max-number-of-messages 10
```
