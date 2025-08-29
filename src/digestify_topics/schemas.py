from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Entity(BaseModel):
    id: UUID
    created_at: datetime
    updated_at: datetime


class TopicRespone(Entity):
    user_id: UUID
    name: str
    description: str
    is_public: bool
    locale: str
    image_uri: str | None


class TopicsResponse(BaseModel):
    topics: list[TopicRespone]


class UserResponse(Entity):
    created_topic_count: int
