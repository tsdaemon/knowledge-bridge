from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Concatenate, ParamSpec
from typing_extensions import TypeVar

from ..models import EdgeEntity, NodeEntity

T = TypeVar("T")
P = ParamSpec("P")
TQDM_TYPE = Callable[Concatenate[T, P], T]


class BaseProvider(ABC):
    def __init__(self) -> None:
        # Optional tqdm decorator which can be overriden by class user
        self.tqdm: TQDM_TYPE = lambda x, *args, **kwargs: x  # type: ignore

    @abstractmethod
    def get_latest_data(
        self, last_sync_timestamp: datetime | None
    ) -> tuple[list[NodeEntity], list[EdgeEntity]]:
        raise NotImplementedError
