from datetime import datetime
from typing import Generator
from neo4j import Session
import pytest

from knowledge_bridge.models import EdgeEntity, NodeEntity
from knowledge_bridge.storage.neo4j import get_neo4j_session, Neo4jGraphStorage


@pytest.fixture
def database_session() -> Generator[Session, None, None]:
    with get_neo4j_session(uri="neo4j://localhost:7697") as session:
        yield session


@pytest.fixture
def provider_name_for_tests() -> str:
    return "provider_for_tests"


@pytest.fixture(autouse=True)
def clean_database(database_session):
    # Remove all records
    query = "MATCH (n) DETACH DELETE n"

    database_session.run(query)
    yield
    database_session.run(query)


def test_get_last_sync_timestamp(database_session, provider_name_for_tests):
    # GIVEN: Neo4jGraphStorage instance
    storage = Neo4jGraphStorage(database_session)

    # WHEN: get_last_sync_timestamp is called with a provider that has no data
    result = storage.get_last_sync_timestamp(provider_name_for_tests)

    # THEN: None is returned
    assert result is None

    # GIVEN: a sync metadata node in the database
    database_session.run(
        "CREATE (n:Sync {provider: $provider, timestamp: datetime()})",
        provider=provider_name_for_tests,
    )

    # WHEN: get_last_sync_timestamp is called with the provider
    result = storage.get_last_sync_timestamp(provider_name_for_tests)

    # THEN: the timestamp of the sync metadata node is returned
    assert isinstance(result, datetime)

    # THEN: the timestamp is not tz-aware
    assert result.tzinfo is None


@pytest.fixture
def nodes_and_edges() -> tuple[list[NodeEntity], list[EdgeEntity]]:
    nodes = [
        NodeEntity(
            id="page1",
            type="Page",
            edited=datetime.now(),
            created=datetime.now(),
            text="{}",
            link="https://example.com/page1",
            obsolete=False,
        ),
        NodeEntity(
            id="block1",
            type="Block",
            created=datetime.now(),
            edited=datetime.now(),
            link=None,
            text='{"text": [{"type": "text", "text": {"content": "Hello, World!"}}]}',
            obsolete=False,
        ),
        NodeEntity(
            id="page2",
            type="Page",
            edited=datetime.now(),
            created=datetime.now(),
            text="{}",
            link="https://example.com/page2",
            obsolete=True,
        ),
        NodeEntity(
            id="database1",
            type="Database",
            edited=datetime.now(),
            created=datetime.now(),
            text="{}",
            link=None,
            obsolete=False,
        ),
        NodeEntity(
            id="page3",
            type="Page",
            edited=datetime.now(),
            created=datetime.now(),
            text="{}",
            link="https://example.com/page3",
            obsolete=True,
        ),
        NodeEntity(
            id="database2",
            type="Database",
            edited=datetime.now(),
            created=datetime.now(),
            text="{}",
            link=None,
            obsolete=False,
        ),
    ]
    edges = [
        EdgeEntity(source=nodes[3], target=nodes[4], type="CHILD_PAGE"),
        EdgeEntity(source=nodes[0], target=nodes[2], type="CHILD_PAGE"),
        EdgeEntity(source=nodes[0], target=nodes[1], type="CHILD_BLOCK"),
        EdgeEntity(source=nodes[0], target=nodes[3], type="CHILD_DATABASE"),
    ]
    return nodes, edges


def test_incremental_data_sync(
    database_session, provider_name_for_tests, nodes_and_edges
):
    # GIVEN: Neo4jGraphStorage instance
    storage = Neo4jGraphStorage(database_session)

    # GIVEN: a list of nodes and edges
    nodes, edges = nodes_and_edges

    # WHEN: incremental_data_sync is called with the provider and the nodes and edges
    storage.incremental_data_sync(provider_name_for_tests, nodes, edges)

    # THEN: the nodes and edges are created in the database
    for node in nodes:
        result = database_session.run("MATCH (n) WHERE n.id = $id RETURN n", id=node.id)
        assert result.single() is not None

    for edge in edges:
        result = database_session.run(
            "MATCH (n)-[r]->(m) WHERE n.id = $source_id AND m.id = $target_id RETURN r",
            source_id=edge.source.id,
            target_id=edge.target.id,
        )
        assert result.single() is not None

    # THEN: a sync metadata node is created in the database
    result = database_session.run(
        "MATCH (n:Sync) WHERE n.provider = $provider RETURN n",
        provider=provider_name_for_tests,
    )
    assert result.single() is not None

    # THEN: the sync metadata node has edges to the new nodes, verify count of results
    result = database_session.run(
        "MATCH (n:Sync)-[r]->(m) WHERE n.provider = $provider RETURN count(r) as count",
        provider=provider_name_for_tests,
    )
    assert result.single()["count"] == len(nodes)

    # THEN: the last sync timestamp is updated
    result = storage.get_last_sync_timestamp(provider_name_for_tests)
    assert isinstance(result, datetime)


def test_incremental_data_sync_update(
    database_session, provider_name_for_tests, nodes_and_edges
):
    # GIVEN: Neo4jGraphStorage instance
    storage = Neo4jGraphStorage(database_session)

    # GIVEN: a list of nodes and edges
    nodes, edges = nodes_and_edges

    # WHEN: incremental_data_sync is called with the provider and the nodes and edges twice
    storage.incremental_data_sync(provider_name_for_tests, nodes, edges)
    storage.incremental_data_sync(provider_name_for_tests, nodes, edges)

    # THEN: the number of nodes and edges created is expected
    result = database_session.run("MATCH (n:Page) RETURN count(n) as count")
    assert result.single()["count"] == 3
    result = database_session.run("MATCH (n:Block) RETURN count(n) as count")
    assert result.single()["count"] == 1
    result = database_session.run("MATCH (n:Database) RETURN count(n) as count")
    assert result.single()["count"] == 2

    result = database_session.run(
        "MATCH ()-[r:CHILD_PAGE]->() RETURN count(r) as count"
    )
    assert result.single()["count"] == 2
    result = database_session.run(
        "MATCH ()-[r:CHILD_BLOCK]->() RETURN count(r) as count"
    )
    assert result.single()["count"] == 1
    result = database_session.run(
        "MATCH ()-[r:CHILD_DATABASE]->() RETURN count(r) as count"
    )
    assert result.single()["count"] == 1

    # THEN: the number of sync metadata nodes is expected
    result = database_session.run("MATCH (n:Sync) RETURN count(n) as count")
    assert result.single()["count"] == 2
