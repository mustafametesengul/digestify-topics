from openai import AsyncOpenAI

from .settings import get_settings

_openai: AsyncOpenAI | None = None


LANGUAGE_MODEL = "gpt-4.1"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 768


def initialize_openai() -> None:
    global _openai
    if _openai is not None:
        raise ValueError("OpenAI has already been initialized.")
    settings = get_settings()
    _openai = AsyncOpenAI(api_key=settings.openai_api_key)


def get_openai() -> AsyncOpenAI:
    global _openai
    if _openai is None:
        raise ValueError("OpenAI has not been initialized.")
    return _openai


async def dispose_openai() -> None:
    global _openai
    openai = get_openai()
    await openai.close()
    _openai = None
