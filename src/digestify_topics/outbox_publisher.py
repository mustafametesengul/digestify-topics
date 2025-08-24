import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.models import OutboxMessage
from digestify_topics.schemas import Message


class OutboxPublisher:
    _tasks: list[asyncio.Task[None]]

    def __init__(
        self,
        engine: AsyncEngine,
        redis: Redis,
        stream: str,
    ) -> None:
        self._engine = engine
        self._redis = redis
        self._stream = stream
        self._tasks = []

    async def _publish_messages(self, limit: int = 10) -> None:
        while True:
            async with AsyncSession(self._engine) as session:
                statement = (
                    select(OutboxMessage)
                    .order_by(col(OutboxMessage.created_at))
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                result = await session.exec(statement)
                messages = result.all()

                for message in messages:
                    redis_message = Message(
                        id=str(message.id),
                        type=message.type,
                        payload=message.payload,
                    )
                    await self._redis.xadd(
                        self._stream, {"data": redis_message.model_dump_json()}
                    )

                for message in messages:
                    await session.delete(message)

                await session.commit()
            await asyncio.sleep(1)

    def start(self, publisher_count: int = 1) -> None:
        for _ in range(publisher_count):
            task: asyncio.Task[None] = asyncio.create_task(self._publish_messages())
            self._tasks.append(task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
