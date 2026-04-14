import logging
from collections.abc import Generator

from azure.identity import ClientSecretCredential
from azure.storage.blob.changefeed import ChangeFeedClient

from config import (
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_STORAGE_ACCOUNT_URL,
    AZURE_TENANT_ID,
)

logger = logging.getLogger("az-changefeed-poller.azure")


def build_client() -> ChangeFeedClient:
    credential = ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
    )
    return ChangeFeedClient(account_url=AZURE_STORAGE_ACCOUNT_URL, credential=credential)


def iter_changes(
    client: ChangeFeedClient, continuation_token: str | None
) -> Generator[tuple[str | None, list[dict], str | None], None, None]:
    """
    Yield one page of change feed events at a time as:
        (page_start_token, events, page_end_token)

    - page_start_token: the continuation token used to fetch this page.
      Saving this on failure lets the next startup re-fetch the same page.
    - page_end_token: the continuation token *after* this page.
      Saving this on success advances the cursor past the page.
    """
    # list_changes() returns ItemPaged — by_page() returns PageIterator
    # continuation_token is an attribute of PageIterator, not ItemPaged
    item_paged = client.list_changes(results_per_page=100)
    page_iterator = item_paged.by_page(continuation_token=continuation_token)
    current_token = continuation_token

    for page in page_iterator:
        events = list(page)
        next_token = page_iterator.continuation_token
        logger.debug(
            "Fetched page with %d event(s) [start_token=%s]",
            len(events),
            current_token,
        )
        yield current_token, events, next_token
        current_token = next_token
