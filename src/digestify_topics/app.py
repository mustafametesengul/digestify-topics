from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from digestify_topics.ai import dispose_openai, initialize_openai
from digestify_topics.auth import get_auth, mock_get_auth
from digestify_topics.db import dispose_engine, get_engine, initialize_engine
from digestify_topics.handlers import dispatcher
from digestify_topics.outbox_publisher import OutboxPublisher
from digestify_topics.queries import HTTPQueries, MockQueries
from digestify_topics.router import router
from digestify_topics.settings import get_settings
from digestify_topics.stream import (
    dispose_redis,
    get_redis,
    initialize_redis,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    initialize_engine()
    initialize_redis()
    initialize_openai()
    stream = "digestify_topics"
    message_publisher = OutboxPublisher(
        engine=get_engine(),
        redis=get_redis(),
        stream=stream,
    )
    message_publisher.start()
    dispatcher.set_redis(get_redis())
    dispatcher.set_engine(get_engine())
    dispatcher.start()
    try:
        yield
    finally:
        await message_publisher.stop()
        await dispatcher.stop()
        await dispose_openai()
        await dispose_redis()
        await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Digestify Topics",
        lifespan=lifespan,
        debug=settings.debug,
    )
    app.include_router(router)

    if settings.debug:
        app.dependency_overrides[get_auth] = mock_get_auth
        app.dependency_overrides[HTTPQueries] = MockQueries

    return app


app = create_app()
