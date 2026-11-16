from redis.asyncio import ConnectionPool, Redis

from app.core.config import redis_settings

redis_pool = ConnectionPool.from_url(
    str(redis_settings.REDIS_URL),
    max_connections=redis_settings.REDIS_MAX_CONNECTIONS,
    decode_responses=True,  # возвращает str вместо bytes
)


def get_redis() -> Redis:
    return Redis(connection_pool=redis_pool)