from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlmodel import Field, SQLModel


class MessageBase(SQLModel):
    id: UUID = Field(primary_key=True, default_factory=uuid4)
    type: str = Field(nullable=False)
    payload: dict = Field(sa_type=JSONB, nullable=False)
    scheduled_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )


class OutboxMessage(MessageBase, table=True):
    __tablename__ = "outbox_messages"


class MessageLog(MessageBase, table=True):
    __tablename__ = "message_logs"


class HandledMessage(SQLModel, table=True):
    __tablename__ = "handled_messages"
    source: str = Field(primary_key=True)
    message_id: str = Field(primary_key=True)
    handler_name: str = Field(primary_key=True)
    handled_at: datetime = Field(
        nullable=False,
        sa_type=TIMESTAMP(timezone=True),  # type: ignore
        index=True,
        default_factory=lambda: datetime.now(timezone.utc),
    )
