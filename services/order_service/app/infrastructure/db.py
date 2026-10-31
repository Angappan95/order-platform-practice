from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from ..config import get_settings

DB_URL = get_settings().database_url

engine = create_async_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

SessionFactory = async_sessionmaker(
    engine, 
    expire_on_commit=False
)

async def get_session():
    async with SessionFactory() as session:
        yield session