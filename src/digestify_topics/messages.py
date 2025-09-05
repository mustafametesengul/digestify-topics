from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class Message(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]


class TopicCreated(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    description: str
    is_public: bool
    locale: str
    image_uri: str | None
    created_at: datetime
    updated_at: datetime


class TopicDeleted(BaseModel):
    topic_id: UUID
    user_id: UUID
