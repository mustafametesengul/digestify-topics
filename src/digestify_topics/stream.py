from redis.asyncio import Redis

from digestify_topics.settings import get_settings

_redis: Redis | None = None


def initialize_redis() -> None:
    global _redis
    if _redis is not None:
        raise ValueError("Redis has already been initialized.")
    settings = get_settings()
    _redis = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
    )


def get_redis() -> Redis:
    global _redis
    if _redis is None:
        raise ValueError("Redis has not been initialized.")
    return _redis


async def dispose_redis() -> None:
    global _redis
    redis = get_redis()
    await redis.close()
    _redis = None
