from typing import Mapping
from knowledge_bridge.storage.base import BaseGraphStorage
from knowledge_bridge.providers.base import BaseProvider


class Bridge(object):
    def __init__(
        self, graph_storage: BaseGraphStorage, providers: Mapping[str, BaseProvider]
    ) -> None:
        self.graph_storage = graph_storage
        self.providers = providers

    def sync(self):
        for name, provider in self.providers.items():
            last_update_ts = self.graph_storage.get_last_sync_timestamp(name)
            nodes, edges = provider.get_latest_data(last_update_ts)
            self.graph_storage.incremental_data_sync(name, nodes, edges)
