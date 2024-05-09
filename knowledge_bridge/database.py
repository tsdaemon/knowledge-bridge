from contextlib import contextmanager
from datetime import datetime
import os
import uuid
from neo4j import Driver, GraphDatabase, Record

from knowledge_bridge.models import BaseNodeEntity, NodeEntity, EdgeEntity


class Neo4jDatabase:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_last_sync_timestamp(self, provider: str) -> datetime | None:
        with self.driver.session() as session:
            query = (
                "MATCH (n:Sync {provider: $provider}) "
                "ORDER BY n.timestamp DESC "
                "LIMIT 1 "
                "RETURN n.timestamp AS timestamp"
            )
            result = session.run(query, provider=provider)
            record = result.single()
            if record is not None:
                return datetime.fromisoformat(record["timestamp"])
            else:
                return None

    def incremental_data_sync(
        self, provider: str, nodes: list[NodeEntity], edges: list[EdgeEntity]
    ):
        with self.driver.session() as session:
            # Upsert new nodes and edges
            session.write_transaction(self._batch_create_or_update_nodes, nodes)
            session.write_transaction(self._batch_create_or_update_edges, edges)

            # Create sync metadata node
            sync_metadata = session.write_transaction(
                self._create_sync_metadata, provider
            )

            # Create edges between upgrade metadata and new nodes
            sync_metadata_node = BaseNodeEntity(id=sync_metadata["id"], type="Upgrade")
            sync_metadata_edges = [
                EdgeEntity(source=sync_metadata_node, target=node, type="SYNC")
                for node in nodes
            ]
            session.write_transaction(
                self._batch_create_or_update_edges, sync_metadata_edges
            )

    @staticmethod
    def _create_sync_metadata(tx, provider: str) -> Record:
        query = (
            "MERGE (n:Sync {id: $id, provider: $provider, timestamp: datetime()}) "
            "RETURN n"
        )
        result = tx.run(query, provider=provider, id=uuid.uuid4())
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
def get_driver():
    uri = os.getenv("NEO4J_URI", "neo4j://localhost")
    username = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    AUTH = (username, password)

    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        driver.verify_connectivity()
        yield driver
