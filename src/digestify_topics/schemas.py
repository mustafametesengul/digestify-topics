from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class Message(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]


class EntityBase(BaseModel):
    id: UUID
    created_at: datetime
    updated_at: datetime


class TopicRead(EntityBase):
    user_id: UUID
    name: str
    description: str
    is_public: bool
    locale: str
    image_url: str | None


class TopicListRead(BaseModel):
    topics: list[TopicRead]


class UserRead(EntityBase):
    created_topic_count: int


class TopicCreated(BaseModel):
    topic: TopicRead
    version: int


class TopicDeleted(BaseModel):
    id: UUID
    version: int
