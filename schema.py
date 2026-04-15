from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class AzureBlob:
    name: str
    size: int
    e_tag: str


@dataclass
class AzureContainer:
    name: str
    account_name: str
    blob: AzureBlob

    def to_dict(self) -> dict:
        return {
            "accountName": self.account_name,
            "containerName": self.name,
            "blob": {
                "name": self.blob.name,
                "size": self.blob.size,
                "eTag": self.blob.e_tag,
            },
        }


@dataclass
class AzureMetadata:
    original_event_type: str
    original_subject: str

    def to_dict(self) -> dict:
        return {
            "originalEventType": self.original_event_type,
            "originalSubject": self.original_subject,
        }


@dataclass
class SQSRecord:
    event_version: str = "2.1"
    event_source: str = "azure:blob"
    event_name: str = "BlobCreated"
    event_time: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    storage: AzureContainer = field(default=None)
    metadata: AzureMetadata = field(default=None)

    def to_dict(self) -> dict:
        return {
            "eventVersion": self.event_version,
            "eventSource": self.event_source,
            "eventName": self.event_name,
            "eventTime": self.event_time,
            "storage": self.storage.to_dict(),
            "metadata": self.metadata.to_dict(),
        }


@dataclass
class SQSMessage:
    records: list[SQSRecord]

    def to_dict(self) -> dict:
        return {"Records": [r.to_dict() for r in self.records]}
