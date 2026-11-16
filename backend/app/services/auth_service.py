from datetime import UTC, datetime

from fastapi import HTTPException, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.crud.user import UserCRUD
from app.database.models import User
from app.schemas.auth import LoginRequest, TokenPair
from app.schemas.user import UserCreate
from app.security.oauth2 import (
    create_access_token,
    create_refresh_token,
    decode_token,
    refresh_token_redis_key,
    verify_refresh_token,
)
from app.security.passwords import hash_password, verify_password


class AuthService:
    @staticmethod
    def _remaining_ttl(exp: int) -> int:
        return max(1, exp - int(datetime.now(UTC).timestamp()))

    @classmethod
    async def _issue_token_pair(cls, user_id: int, redis: Redis) -> TokenPair:
        access_token, _ = create_access_token(user_id)
        refresh_token, refresh_payload = create_refresh_token(user_id)

        ttl = cls._remaining_ttl(refresh_payload.exp)
        await redis.setex(
            refresh_token_redis_key(user_id, refresh_payload.jti),
            ttl,
            "1"
        )
        return TokenPair(access_token=access_token, refresh_token=refresh_token)

    @classmethod
    async def register(
        cls, 
        db: AsyncSession, 
        redis: Redis, 
        user_in: UserCreate
    ) -> tuple[User, TokenPair]: 
        if await UserCRUD.get_by_email(db, user_in.email):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this email already exists"
            )
        if await UserCRUD.get_by_username(db, user_in.username):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this username already exists",
            )

        hashed = await hash_password(user_in.password)
        user = await UserCRUD.create(db, user_in, hashed_password=hashed)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this email or username already exists",
            )

        tokens = await cls._issue_token_pair(user.id, redis)
        return user, tokens


    @classmethod
    async def login(
        cls,
        db: AsyncSession,
        redis: Redis,
        credentials: LoginRequest,
    ) -> TokenPair:
        user = await UserCRUD.get_by_email(db, credentials.email)
        if user is None or not await verify_password(
            credentials.password, user.hashed_password
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return await cls._issue_token_pair(user.id, redis)

    @classmethod
    async def refresh(
        cls,
        db: AsyncSession,
        redis: Redis,
        refresh_token: str,
    ) -> TokenPair:
        payload = await verify_refresh_token(refresh_token, redis)
        user_id = int(payload.sub)

        # Атомарная ротация: удаляем старый refresh-токен; если 0 — гонка или повторное использование
        deleted = await redis.delete(refresh_token_redis_key(user_id, payload.jti))
        if deleted == 0:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token already used or revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = await UserCRUD.get_by_id(db, user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return await cls._issue_token_pair(user.id, redis)

    @classmethod
    async def logout(
        cls,
        redis: Redis,
        access_token: str,
        refresh_token: str | None = None,
    ) -> None:
        access_payload = decode_token(access_token, expected_type="access")
        ttl = cls._remaining_ttl(access_payload.exp)
        await redis.setex(f"blacklist:{access_payload.jti}", ttl, "1")

        if refresh_token:
            refresh_payload = decode_token(refresh_token, expected_type="refresh")
            if refresh_payload.sub == access_payload.sub:
                await redis.delete(
                    refresh_token_redis_key(refresh_payload.sub, refresh_payload.jti)
                )

    @staticmethod
    async def logout_all(redis: Redis, user_id: int, access_token: str) -> None:
        access_payload = decode_token(access_token, expected_type="access")
        ttl = AuthService._remaining_ttl(access_payload.exp)
        await redis.setex(f"blacklist:{access_payload.jti}", ttl, "1")

        keys = [
            key async for key in redis.scan_iter(match=f"refresh:{user_id}:*")
        ]
        if keys:
            await redis.delete(*keys)