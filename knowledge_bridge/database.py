from neo4j import GraphDatabase

from knowledge_bridge.models import NodeEntity, EdgeEntity


class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def upsert_node(self, node: NodeEntity):
        with self.driver.session() as session:
            session.write_transaction(self._create_or_update_node, node)

    def create_edge(self, edge: EdgeEntity):
        with self.driver.session() as session:
            session.write_transaction(self._create_edge, edge)

    @staticmethod
    def _create_or_update_node(tx, node: NodeEntity):
        query = (
            f"MERGE (n:{node.type} {{id: $id}}) "  # Use f-string for label because it cannot be parameterized
            "ON CREATE SET n.created = $created, n.edited = $edited, n.link = $link, n.text = $text "
            "ON MATCH SET n.created = $created, n.edited = $edited, n.link = $link, n.text = $text "
            "RETURN n"
        )
        result = tx.run(
            query,
            id=node.id,
            created=node.created.isoformat(),
            edited=node.edited.isoformat(),
            link=node.link,
            text=node.text,
        )
        return result.single()[0]

    @staticmethod
    def _create_edge(tx, edge: EdgeEntity):
        query = (
            f"MATCH (source:{edge.source.type} {{id: $sourceId}}), "  # Use f-string for source type
            f"(target:{edge.target.type} {{id: $targetId}}) "  # Use f-string for target type
            f"MERGE (source)-[r:{edge.type}]->(target) "  # Use f-string for relationship type
            "RETURN r"
        )
        result = tx.run(query, sourceId=edge.source.id, targetId=edge.target.id)
        return result.single()[0]
