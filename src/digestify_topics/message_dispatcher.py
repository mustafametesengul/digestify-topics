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
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.models import HandledMessage
from digestify_topics.schemas import Message

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

                consumer = func.__name__
                last_message_id_key = f"last_message_id:{self._stream}:{consumer}"
                last_message_id_value = await redis.get(last_message_id_key)

                last_message_id = "0"
                if last_message_id_value:
                    last_message_id = bytes(last_message_id_value).decode()

                while True:
                    response = await redis.xread(
                        {self._stream: last_message_id}, block=1000, count=1
                    )
                    if not response:
                        continue

                    message_id, message_data = response[0][1][0]
                    redis_message_raw = bytes(message_data[b"data"]).decode()

                    redis_message = Message.model_validate_json(redis_message_raw)
                    if redis_message.type != MessagePayloadSchema.__name__:
                        await redis.set(last_message_id_key, message_id)
                        last_message_id = message_id
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
                    except Exception as e:
                        logger.exception(f"Error processing event {e}")
                        raise e

                    await redis.set(last_message_id_key, message_id)
                    last_message_id = message_id

            self._handlers[MessagePayloadSchema.__name__] = handler

            return func

        return decorator

    def start(self) -> None:
        for handler in self._handlers.values():
            task: asyncio.Task[None] = asyncio.create_task(handler())
            self._tasks.append(task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
