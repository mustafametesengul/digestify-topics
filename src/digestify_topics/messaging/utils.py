from datetime import datetime, timezone

from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.messaging.models import MessageLog, OutboxMessage


def add_outbox_message(
    session: AsyncSession,
    payload: BaseModel,
    scheduled_at: datetime | None = None,
) -> None:
    if scheduled_at is None:
        scheduled_at = datetime.now(timezone.utc)

    outbox_message = OutboxMessage(
        type=payload.__class__.__name__,
        payload=payload.model_dump(mode="json"),
        scheduled_at=scheduled_at,
    )
    message_log = MessageLog.model_validate(outbox_message.model_dump())

    session.add(outbox_message)
    session.add(message_log)
