from uuid import UUID

from pydantic import BaseModel


class TopicCreated(BaseModel):
    topic_id: UUID
    user_id: UUID


class TopicDeleted(BaseModel):
    topic_id: UUID
    user_id: UUID
