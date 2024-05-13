from abc import ABC, abstractmethod
from datetime import datetime

from knowledge_bridge.models import EdgeEntity, NodeEntity


class BaseGraphStorage(ABC):
    @abstractmethod
    def get_last_sync_timestamp(self, provider: str) -> datetime | None:
        raise NotImplementedError

    @abstractmethod
    def incremental_data_sync(
        self, provider: str, nodes: list[NodeEntity], edges: list[EdgeEntity]
    ) -> None:
        raise NotImplementedError
