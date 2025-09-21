from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlmodel import Field, SQLModel


class Entity(SQLModel):
    id: UUID = Field(primary_key=True, default_factory=uuid4)
    discarded: bool = Field(nullable=False, index=True, default=False)
    created_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )
    updated_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )
    version: int = Field(nullable=False, index=True, default=0)

    def increment_version(self):
        if self.version == 0:
            self.updated_at = self.created_at
        else:
            self.updated_at = datetime.now(timezone.utc)
        self.version += 1


class Topic(Entity, table=True):
    __tablename__ = "topics"
    name: str = Field(nullable=False)
    description: str = Field(nullable=False)
    user_id: UUID = Field(nullable=False, index=True)
    is_public: bool = Field(nullable=False, index=True)
    locale: str = Field(nullable=False, index=True)
    image_url: str | None = Field(nullable=True, default=None)


class User(Entity, table=True):
    __tablename__ = "users"
    created_topic_count: int = Field(nullable=False, index=True, default=0)


# class Story(Entity, table=True):
#     __tablename__ = "stories"
#     content: str = Field(nullable=False)
#     topic_id: UUID = Field(nullable=False, index=True)
