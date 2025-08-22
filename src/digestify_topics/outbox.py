import asyncio
import inspect
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    ParamSpec,
    TypeVar,
    get_type_hints,
)

from pydantic import BaseModel
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.db import get_engine
from digestify_topics.models import Handler, Outbox
from digestify_topics.stream import get_redis

P = ParamSpec("P")
R = TypeVar("R")
AsyncFunction = Callable[P, Awaitable[R]]

logger = logging.getLogger(__name__)

STREAM_NAME = "digestify_topics"


class RedisMessage(BaseModel):
    id: str
    type: str
    payload: str


class MessagePublisher:
    _tasks: list[asyncio.Task[None]]

    def __init__(self) -> None:
        self._redis = get_redis()
        self._engine = get_engine()
        self._tasks = []

    async def _publish_messages(self, limit: int = 10) -> None:
        while True:
            async with AsyncSession(self._engine) as session:
                statement = (
                    select(Outbox)
                    .order_by(col(Outbox.created_at))
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                result = await session.exec(statement)
                messages = result.all()

                for message in messages:
                    redis_message = RedisMessage(
                        id=str(message.id),
                        type=message.__class__.__name__,
                        payload=message.model_dump_json(),
                    )
                    await self._redis.xadd(
                        STREAM_NAME, {"data": redis_message.model_dump_json()}
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


class MessageHandler:
    # Handlers are zero-arg async callables returning a coroutine that resolves to None
    _handlers: dict[str, Callable[[], Coroutine[Any, Any, None]]]
    _tasks: list[asyncio.Task[None]]

    def __init__(self) -> None:
        self._redis = get_redis()
        self._engine = get_engine()
        self._handlers = {}
        self._tasks = []

    def register(self) -> Callable[[AsyncFunction], AsyncFunction]:
        def decorator(func: AsyncFunction) -> AsyncFunction:
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            param_name = params[0].name
            type_hints = get_type_hints(func)
            if param_name not in type_hints:
                raise RuntimeError(
                    f"Handler {func.__name__} is missing a type annotation on its sole argument."
                )

            MessagePayloadSchema = type_hints[param_name]
            if not (
                isinstance(MessagePayloadSchema, type)
                and issubclass(MessagePayloadSchema, BaseModel)
            ):
                raise TypeError(
                    f"{MessagePayloadSchema} must be a subclass of BaseModel"
                )

            async def handler() -> None:
                consumer = func.__name__
                last_message_id_key = f"last_message_id:{STREAM_NAME}:{consumer}"
                last_message_id_value = await self._redis.get(last_message_id_key)

                last_message_id = "0"
                if last_message_id_value:
                    last_message_id = bytes(last_message_id_value).decode()

                while True:
                    response = await self._redis.xread({STREAM_NAME: last_message_id})

                    if not response:
                        continue

                    message_id, message_data = response[1][0]

                    redis_message_raw = bytes(message_data[b"data"]).decode()

                    redis_message = RedisMessage.model_validate_json(redis_message_raw)
                    if redis_message.type != MessagePayloadSchema.__name__:
                        continue

                    payload = MessagePayloadSchema.model_validate_json(
                        redis_message.payload
                    )

                    try:
                        async with AsyncSession(self._engine) as session:
                            await func(payload, session)

                            handler_log = Handler(
                                message_id=redis_message.id,
                                handler_name=func.__name__,
                            )
                            session.add(handler_log)
                            await session.commit()
                    except Exception as e:
                        logger.exception(f"Error processing event {e}")
                        raise e

                    await self._redis.set(last_message_id_key, message_id)
                    last_message_id = message_id

            self._handlers[MessagePayloadSchema.__name__] = handler

            return func

        return decorator

    def start(self) -> None:
        for handler in self._handlers.values():
            # handler() is a coroutine; create a Task[None] for it
            task: asyncio.Task[None] = asyncio.create_task(handler())
            self._tasks.append(task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
