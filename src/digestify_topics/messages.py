from typing import Any
from uuid import UUID

from pydantic import BaseModel


class Message(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]


class TopicCreated(BaseModel):
    topic_id: UUID
    user_id: UUID


class TopicDeleted(BaseModel):
    topic_id: UUID
    user_id: UUID
