from datetime import datetime

from pydantic import BaseModel


class BaseNodeEntity(BaseModel):
    id: str
    type: str

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, BaseNodeEntity):
            return False
        return value.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)


class NodeEntity(BaseNodeEntity):
    created: datetime
    edited: datetime
    link: str | None
    text: str | None
    obsolete: bool = False


class EdgeEntity(BaseModel):
    source: BaseNodeEntity
    target: BaseNodeEntity
    type: str

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, EdgeEntity):
            return False

        return (
            value.source == self.source
            and value.target == self.target
            and value.type == self.type
        )

    def __hash__(self) -> int:
        return hash((self.source, self.target, self.type))
