from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import uuid4

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import jwtsettings
from app.core.database import get_db
from app.core.redis import get_redis
from app.database.models import User
from app.schemas.auth import TokenPayload

bearer_scheme = HTTPBearer(auto_error=False)


def _create_token(
    subject: str | int,
    token_type: Literal["access", "refresh"],
    expires_delta: timedelta,
) -> tuple[str, TokenPayload]:
    now = datetime.now(UTC)
    expire = now + expires_delta
    payload = TokenPayload(
        sub=str(subject),
        jti=str(uuid4()),
        type=token_type,
        exp=int(expire.timestamp()),
    )
    encoded_jwt = jwt.encode(
        payload.model_dump(),
        jwtsettings.SECRET_KEY,
        algorithm=jwtsettings.ALGORITHM,
    )
    return encoded_jwt, payload


def create_access_token(user_id: int) -> tuple[str, TokenPayload]:
    return _create_token(
        subject=user_id,
        token_type="access",
        expires_delta=timedelta(minutes=jwtsettings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )


def create_refresh_token(user_id: int) -> tuple[str, TokenPayload]:
    return _create_token(
        subject=user_id,
        token_type="refresh",
        expires_delta=timedelta(days=jwtsettings.REFRESH_TOKEN_EXPIRE_DAYS),
    )


def decode_token(
    token: str,
    expected_type: Literal["access", "refresh"],
) -> TokenPayload:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        raw_payload = jwt.decode(
            token,
            jwtsettings.SECRET_KEY,
            algorithms=[jwtsettings.ALGORITHM],
            options={"require": ["exp", "sub", "jti", "type"]},
        )
        payload = TokenPayload.model_validate(raw_payload)
    except (InvalidTokenError, ValidationError):
        raise credentials_exception

    if payload.type != expected_type:
        raise credentials_exception

    return payload


def refresh_token_redis_key(user_id: str | int, jti: str) -> str:
    return f"refresh:{user_id}:{jti}"


async def verify_refresh_token(token: str, redis: Redis) -> TokenPayload:
    payload = decode_token(token, expected_type="refresh")
    exists = await redis.exists(refresh_token_redis_key(payload.sub, payload.jti))
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token is invalid or revoked",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise credentials_exception

    payload = decode_token(credentials.credentials, expected_type="access")

    # Проверка blacklist для отозванных access-токенов (при logout)
    if await redis.exists(f"blacklist:{payload.jti}"):
        raise credentials_exception

    try:
        user_id = int(payload.sub)
    except ValueError:
        raise credentials_exception

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise credentials_exception

    return user


CurrentUser = Annotated[User, Depends(get_current_user)]