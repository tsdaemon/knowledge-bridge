from datetime import datetime
import json
import logging
from typing import Tuple
from notion_client import APIResponseError, Client

from ..models import EdgeEntity, NodeEntity

from .base import BaseProvider

logger = logging.getLogger(__name__)


def parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def process_paginated(endpoint_method, last_edited_time=None, **kwargs):
    next_cursor = None
    while True:
        response = endpoint_method(start_cursor=next_cursor, **kwargs)
        for result in response["results"]:
            if (
                last_edited_time
                and parse_datetime(result["last_edited_time"]) < last_edited_time
            ):
                return
            yield result
        has_more = response.get("has_more", False)
        if not has_more:
            return
        next_cursor = response["next_cursor"]


class NotionProvider(BaseProvider):
    def __init__(self, client: Client):
        self.client = client
        self.edges: set[Tuple[str, str, str]] = set()
        self.nodes: dict[str, NodeEntity] = {}

        super().__init__()

    def get_latest_data(
        self, last_sync_timestamp: datetime | None
    ) -> Tuple[list[NodeEntity], list[EdgeEntity]]:
        pages = process_paginated(
            self.client.search,
            last_edited_time=last_sync_timestamp,
            query="",
            filter={"value": "page", "property": "object"},
        )
        for page in self.tqdm(pages, desc="Processing pages"):
            self._process_page(page)

        databases = process_paginated(
            self.client.search,
            last_edited_time=last_sync_timestamp,
            query="",
            filter={"value": "database", "property": "object"},
        )
        for database in self.tqdm(databases, desc="Processing databases"):
            self._process_database(database)

        return (
            list(self.nodes.values()),
            [
                EdgeEntity(
                    source=self.nodes[source], target=self.nodes[target], type=type
                )
                for source, target, type in self.edges
            ],
        )

    def _process_page(self, page):
        if page["id"] in self.nodes:
            logger.info(f"Skipping processed page {page['id']}")
            return

        logger.info(f"Processing page {page['id']}")
        node = NodeEntity(
            id=page["id"],
            type="page",
            created=page["created_time"],
            edited=page["last_edited_time"],
            # Dump all content as json
            text=json.dumps(page["properties"]),
            obsolete=page.get("in_trash", False),
            link=page["url"],
        )
        self.nodes[node.id] = node

        parent = page["parent"]
        if parent["type"] != "workspace":
            edge = (parent[parent["type"]], node.id, "CHILD_PAGE")
            self.edges.add(edge)

        blocks = process_paginated(
            self.client.blocks.children.list, block_id=page["id"]
        )

        for block in blocks:
            self._process_block(block)

    def _process_block(self, block):
        if block["id"] in self.nodes:
            logger.info(f"Skipping processed block {block['id']}")
            return

        logger.info(f"Processing block {block['id']}")

        if block["type"] == "child_page":
            try:
                page = self.client.pages.retrieve(block["id"])
            except APIResponseError as e:
                if e.status == 404:
                    logger.warning(
                        f"Referenced page {block['id']} not found, most likely not shared with the integration. Skipping."
                    )
                    return
                raise
            self._process_page(page)
            return

        if block["type"] == "child_database":
            try:
                database = self.client.databases.retrieve(block["id"])
            except APIResponseError as e:
                if e.status == 404:
                    logger.warning(
                        f"Referenced database {block['id']} not found, most likely not shared with the integration. Skipping."
                    )
                    return
                raise
            self._process_database(database)
            return

        node = NodeEntity(
            id=block["id"],
            type="block",
            created=block["created_time"],
            edited=block["last_edited_time"],
            # Dump all content as json
            text=json.dumps(block[block["type"]]),
            obsolete=block.get("in_trash", False),
            link=None,
        )
        self.nodes[node.id] = node

        parent = block["parent"]
        if parent["type"] != "workspace":
            edge = (parent[parent["type"]], node.id, "CHILD_BLOCK")
            self.edges.add(edge)

        if block.get("has_children"):
            children = process_paginated(
                self.client.blocks.children.list, block_id=block["id"]
            )
            for child in children:
                self._process_block(child)

    def _process_database(self, database):
        if database["id"] in self.nodes:
            logger.info(f"Skipping processed database {database['id']}")
            return

        logger.info(f"Processing database {database['id']}")
        node = NodeEntity(
            id=database["id"],
            type="database",
            created=database["created_time"],
            edited=database["last_edited_time"],
            # Dump all content as json
            text=json.dumps(database["properties"]),
            obsolete=database.get("in_trash", False),
            link=None,
        )
        self.nodes[node.id] = node

        parent = database["parent"]
        if parent["type"] != "workspace":
            edge = (parent[parent["type"]], node.id, "CHILD_DATABASE")
            self.edges.add(edge)

        pages = process_paginated(
            self.client.databases.query, database_id=database["id"]
        )

        for page in pages:
            self._process_page(page)
