from datetime import datetime
import pytest

from knowledge_bridge.storage.neo4j import get_session, Neo4jGraphStorage


@pytest.fixture
def database_session():
    with get_session() as session:
        yield session


def test_get_last_sync_timestamp(database_session):
    # GIVEN: Neo4jGraphStorage instance
    storage = Neo4jGraphStorage(database_session)

    # WHEN: get_last_sync_timestamp is called with a provider that has no data
    result = storage.get_last_sync_timestamp("provider")

    # THEN: None is returned
    assert result is None

    # GIVEN: a sync metadata node in the database
    database_session.run(
        "CREATE (n:Sync {provider: $provider, timestamp: datetime()})",
        provider="provider",
    )

    # WHEN: get_last_sync_timestamp is called with the provider
    result = storage.get_last_sync_timestamp("provider")

    # THEN: the timestamp of the sync metadata node is returned
    assert isinstance(result, datetime)
