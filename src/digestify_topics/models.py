from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlmodel import Field, SQLModel


class BaseModel(SQLModel):
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


class Topic(BaseModel, table=True):
    name: str = Field(nullable=False)
    description: str = Field(nullable=False)
    user_id: UUID = Field(nullable=False, index=True)
    is_public: bool = Field(nullable=False, index=True)
    locale: str = Field(nullable=False, index=True)
    image_uri: str | None = Field(nullable=True, default=None)


class User(BaseModel, table=True):
    created_topic_count: int = Field(nullable=False, index=True, default=0)


class OutboxMessage(SQLModel):
    id: UUID = Field(primary_key=True, default_factory=uuid4)
    type: str = Field(nullable=False, index=True)
    entity: str | None = Field(nullable=True, index=True)
    payload: dict = Field(sa_type=JSONB, nullable=False)
    created_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )
    version: int | None = Field(nullable=True, index=True)

    def from_payload(
        cls,
        payload: BaseModel,
        version: str | None = None,
        entity: str | None = None,
    ) -> "OutboxMessage":
        return cls(
            type=payload.__name__,
            entity=entity,
            payload=payload.model_dump(mode="json"),
            version=version,
        )


class HandlerLog(SQLModel):
    message_id: UUID = Field(primary_key=True)
    handler_name: str = Field(primary_key=True)
    created_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )
