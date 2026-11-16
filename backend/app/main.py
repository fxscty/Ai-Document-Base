from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.kafka import close_kafka_producer, init_kafka_producer
from app.core.redis import get_redis, redis_pool
from app.routers import auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = get_redis()
    await redis.ping()
    await init_kafka_producer()
    yield
    await close_kafka_producer()
    await redis_pool.aclose()


app = FastAPI(title="AI Document Base", lifespan=lifespan)

app.include_router(auth.router, prefix="/api/v1")