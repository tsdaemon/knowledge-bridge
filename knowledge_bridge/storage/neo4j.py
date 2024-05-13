from contextlib import contextmanager
from datetime import datetime
import os
from typing import Generator
import uuid
from neo4j import GraphDatabase, Record, Session

from ..models import BaseNodeEntity, NodeEntity, EdgeEntity

from .base import BaseGraphStorage


class Neo4jGraphStorage(BaseGraphStorage):
    def __init__(self, session: Session):
        self.session = session

    def get_last_sync_timestamp(self, provider: str) -> datetime | None:
        query = (
            "MATCH (n:Sync {provider: $provider}) "
            "RETURN n.timestamp AS timestamp "
            "ORDER BY n.timestamp DESC "
            "LIMIT 1 "
        )
        result = self.session.run(query, provider=provider)
        record = result.single()
        if record is not None:
            return record["timestamp"].to_native()
        else:
            return None

    def incremental_data_sync(
        self, provider: str, nodes: list[NodeEntity], edges: list[EdgeEntity]
    ) -> None:
        # Upsert new nodes and edges
        self.session.write_transaction(self._batch_create_or_update_nodes, nodes)
        self.session.write_transaction(self._batch_create_or_update_edges, edges)

        # Create sync metadata node
        sync_metadata = self.session.write_transaction(
            self._create_sync_metadata, provider
        )

        # Create edges between upgrade metadata and new nodes
        sync_metadata_node = BaseNodeEntity(id=sync_metadata["id"], type="Upgrade")
        sync_metadata_edges = [
            EdgeEntity(source=sync_metadata_node, target=node, type="SYNC")
            for node in nodes
        ]
        self.session.write_transaction(
            self._batch_create_or_update_edges, sync_metadata_edges
        )

    @staticmethod
    def _create_sync_metadata(tx, provider: str) -> Record:
        query = (
            "MERGE (n:Sync {id: $id, provider: $provider, timestamp: datetime()}) "
            "RETURN n"
        )
        result = tx.run(query, provider=provider, id=str(uuid.uuid4()))
        return result.single()[0]

    @staticmethod
    def _batch_create_or_update_nodes(tx, nodes: list[NodeEntity]) -> list[Record]:
        results = []
        for node in nodes:
            query = (
                f"MERGE (n:{node.type} {{id: $id}}) "
                "ON CREATE SET n.created = $created, n.edited = $edited, n.link = $link, n.text = $text, n.obsolete = $obsolete "
                "ON MATCH SET n.created = $created, n.edited = $edited, n.link = $link, n.text = $text, n.obsolete = $obsolete "
                "RETURN n"
            )
            result = tx.run(
                query,
                id=node.id,
                created=node.created.isoformat(),
                edited=node.edited.isoformat(),
                link=node.link,
                text=node.text,
                obsolete=node.obsolete,
            )
            results.append(result.single()[0])
        return results

    @staticmethod
    def _batch_create_or_update_edges(tx, edges: list[EdgeEntity]) -> list[Record]:
        results = []
        for edge in edges:
            query = (
                f"MATCH (source:{edge.source.type} {{id: $sourceId}}), "
                f"(target:{edge.target.type} {{id: $targetId}}) "
                f"MERGE (source)-[r:{edge.type}]->(target) "
                "RETURN r"
            )
            result = tx.run(query, sourceId=edge.source.id, targetId=edge.target.id)
            results.append(result.single()[0])
        return results


@contextmanager
def get_session(database: str | None = None) -> Generator[Session, None, None]:
    uri = os.getenv("NEO4J_URI", "neo4j://localhost")
    username = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    AUTH = (username, password)

    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            yield session
