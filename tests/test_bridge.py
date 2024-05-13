from datetime import datetime
from unittest.mock import Mock

from knowledge_bridge.bridge import Bridge
from knowledge_bridge.providers.base import BaseProvider
from knowledge_bridge.storage.base import BaseGraphStorage


def test_sync():
    # GIVEN: storage mock which provides different timestamp for each provider
    graph_storage = Mock(spec=BaseGraphStorage)
    last_sync_timestamp = datetime.strptime(
        "2021-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
    )

    def last_sync_timestamp_side_effect(provider):
        return last_sync_timestamp if provider == "provider_with_data" else None

    graph_storage.get_last_sync_timestamp.side_effect = last_sync_timestamp_side_effect

    # GIVEN: provider mocks which return different data for each provider
    provider_with_no_data = Mock(spec=BaseProvider)
    provider_with_no_data.get_latest_data.return_value = (
        ["node1", "node2"],
        ["edge1", "edge2"],
    )
    provider_with_data = Mock(spec=BaseProvider)
    provider_with_data.get_latest_data.return_value = (
        ["node3", "node4"],
        ["edge3", "edge4"],
    )
    providers = {
        "provider_with_no_data": provider_with_no_data,
        "provider_with_data": provider_with_data,
    }

    # WHEN: we sync the bridge
    bridge = Bridge(graph_storage=graph_storage, providers=providers)
    bridge.sync()

    # THEN: providers are called for data with respective timestamps
    provider_with_no_data.get_latest_data.assert_called_once_with(None)
    provider_with_data.get_latest_data.assert_called_once_with(last_sync_timestamp)

    # THEN: graph storage is called to sync data for each provider
    graph_storage.incremental_data_sync.assert_any_call(
        "provider_with_no_data",
        ["node1", "node2"],
        ["edge1", "edge2"],
    )
    graph_storage.incremental_data_sync.assert_any_call(
        "provider_with_data",
        ["node3", "node4"],
        ["edge3", "edge4"],
    )
