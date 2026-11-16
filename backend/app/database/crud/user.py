from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import User
from app.schemas.user import UserCreate, UserUpdate


class UserCRUD:
    @staticmethod
    async def get_by_id(db: AsyncSession, user_id: int) -> User | None:
        return await db.get(User, user_id)

    @staticmethod
    async def get_by_email(db: AsyncSession, email: str) -> User | None:
        result = await db.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_username(db: AsyncSession, username: str) -> User | None:
        result = await db.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()

    @staticmethod
    async def create(
        db: AsyncSession,
        obj_in: UserCreate,
        hashed_password: str,
    ) -> User | None:
        db_obj = User(
            email=obj_in.email,
            username=obj_in.username,
            hashed_password=hashed_password,
        )
        db.add(db_obj)
        try:
            await db.commit()
            await db.refresh(db_obj)
            return db_obj
        except IntegrityError:
            await db.rollback()
            return None

    @staticmethod
    async def update(
        db: AsyncSession,
        db_obj: User,
        obj_in: UserUpdate,
        hashed_password: str | None = None,
    ) -> User | None:
        if obj_in.username is not None:
            db_obj.username = obj_in.username
        if hashed_password is not None:
            db_obj.hashed_password = hashed_password

        try:
            await db.commit()
            await db.refresh(db_obj)
            return db_obj
        except IntegrityError:
            await db.rollback()
            return None

    @staticmethod
    async def delete(db: AsyncSession, user_id: int) -> bool:
        db_obj = await db.get(User, user_id)
        if db_obj is None:
            return False

        await db.delete(db_obj)
        await db.commit()
        return True