import asyncio
import inspect
import logging
import uuid
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
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.messages import Message
from digestify_topics.models import HandledMessage

P = ParamSpec("P")
R = TypeVar("R")
AsyncFunction = Callable[P, Awaitable[R]]


logger = logging.getLogger(__name__)


class MessageDispatcher:
    _handlers: dict[str, Callable[[], Coroutine[Any, Any, None]]]
    _tasks: list[asyncio.Task[None]]

    def __init__(self, stream: str) -> None:
        self._handlers = {}
        self._tasks = []
        self._stream = stream
        self._engine: AsyncEngine | None = None
        self._redis: Redis | None = None

    def _get_redis(self) -> Redis:
        if self._redis is None:
            raise ValueError("Redis has not been set.")
        return self._redis

    def _get_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise ValueError("Engine has not been set.")
        return self._engine

    def set_redis(self, redis: Redis) -> None:
        self._redis = redis

    def set_engine(self, engine: AsyncEngine) -> None:
        self._engine = engine

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
                redis = self._get_redis()
                engine = self._get_engine()

                consumer_group = func.__name__
                consumer_name = f"{consumer_group}:{uuid.uuid4().hex[:8]}"

                # Ensure the consumer group exists; create it if not.
                try:
                    await redis.xgroup_create(
                        name=self._stream,
                        groupname=consumer_group,
                        id="$",
                        mkstream=True,
                    )
                except ResponseError as e:
                    # Ignore if the consumer group already exists
                    if "BUSYGROUP" not in str(e):
                        raise

                while True:
                    response = await redis.xreadgroup(
                        groupname=consumer_group,
                        consumername=consumer_name,
                        streams={self._stream: ">"},
                        block=1000,
                        count=1,
                    )
                    if not response:
                        continue

                    message_id, message_data = response[0][1][0]
                    redis_message_raw = bytes(message_data[b"data"]).decode()

                    redis_message = Message.model_validate_json(redis_message_raw)
                    if redis_message.type != MessagePayloadSchema.__name__:
                        # Not our message type; ack and continue so this group doesn't get stuck.
                        await redis.xack(self._stream, consumer_group, message_id)
                        continue

                    try:
                        payload = MessagePayloadSchema.model_validate(
                            redis_message.payload
                        )

                        async with AsyncSession(engine) as session:
                            await func(payload, session)

                            handler_log = HandledMessage(
                                message_id=redis_message.id,
                                handler_name=func.__name__,
                            )
                            session.add(handler_log)
                            await session.commit()
                        # Ack only after successful handling and DB commit
                        await redis.xack(self._stream, consumer_group, message_id)
                    except Exception as e:
                        logger.exception(f"Error processing event {e}")
                        raise e

            self._handlers[MessagePayloadSchema.__name__] = handler

            return func

        return decorator

    def start(self) -> None:
        if self._engine is None or self._redis is None:
            raise ValueError("Engine and Redis must be set before starting.")
        for handler in self._handlers.values():
            task: asyncio.Task[None] = asyncio.create_task(handler())
            self._tasks.append(task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
