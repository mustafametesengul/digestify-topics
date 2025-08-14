from fastapi import FastAPI

from digestify_topics.router import router
from digestify_topics.settings import get_settings


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Digestify Topics",
        debug=settings.debug,
    )
    app.include_router(router)

    return app


app = create_app()
