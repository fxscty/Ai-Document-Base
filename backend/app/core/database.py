from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import settings

engine = create_async_engine(str(settings.database_url), echo=True, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(
    autocommit=False, autoflush=False, expire_on_commit=False, class_=AsyncSession, bind=engine
)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session