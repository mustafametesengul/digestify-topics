from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class Locale(StrEnum):
    EN = "en-US"
    TR = "tr-TR"


class TopicCreated(BaseModel):
    topic_id: UUID
    user_id: UUID


class TopicDeleted(BaseModel):
    topic_id: UUID
    user_id: UUID


class TopicUpdated(BaseModel):
    topic_id: UUID
    user_id: UUID
    name: str
    description: str | None = None
    is_public: bool | None = None
    locale: Locale | None = None
    image_uri: str | None = None
