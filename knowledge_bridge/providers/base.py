from abc import ABC, abstractmethod
from datetime import datetime

from ..models import EdgeEntity, NodeEntity


class BaseProvider(ABC):
    @abstractmethod
    def get_latest_data(
        self, last_sync_timestamp: datetime | None
    ) -> tuple[list[NodeEntity], list[EdgeEntity]]:
        raise NotImplementedError
