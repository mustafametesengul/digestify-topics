from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from digestify_topics.ai import dispose_openai, initialize_openai
from digestify_topics.db import dispose_engine, initialize_engine
from digestify_topics.outbox import MessageHandler, MessagePublisher
from digestify_topics.router import router
from digestify_topics.settings import get_settings
from digestify_topics.stream import dispose_redis, initialize_redis


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    initialize_engine()
    initialize_redis()
    initialize_openai()
    message_publisher = MessagePublisher()
    message_handler = MessageHandler()
    await message_publisher.start()
    await message_handler.start()
    try:
        yield
    finally:
        await message_publisher.stop()
        await message_handler.stop()
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

    return app


app = create_app()
