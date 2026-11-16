from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis
from app.schemas.auth import LoginRequest, RefreshTokenRequest, TokenPair
from app.schemas.user import UserCreate, UserResponse
from app.security.oauth2 import CurrentUser, bearer_scheme
from app.services.auth_service import AuthService

router = APIRouter(prefix="/auth", tags=["Auth"])

DbSession = Annotated[AsyncSession, Depends(get_db)]
RedisClient = Annotated[Redis, Depends(get_redis)]


@router.post(
    "/register",
    response_model=TokenPair,
    status_code=status.HTTP_201_CREATED,
)
async def register(
    user_in: UserCreate,
    db: DbSession,
    redis: RedisClient,
) -> TokenPair:
    _, tokens = await AuthService.register(db=db, redis=redis, user_in=user_in)
    return tokens


@router.post("/login", response_model=TokenPair)
async def login(
    credentials: LoginRequest,
    db: DbSession,
    redis: RedisClient,
) -> TokenPair:
    return await AuthService.login(db=db, redis=redis, credentials=credentials)


@router.post("/refresh", response_model=TokenPair)
async def refresh_tokens(
    body: RefreshTokenRequest,
    db: DbSession,
    redis: RedisClient,
) -> TokenPair:
    return await AuthService.refresh(
        db=db,
        redis=redis,
        refresh_token=body.refresh_token,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    redis: RedisClient,
    body: RefreshTokenRequest | None = None,
) -> None:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    await AuthService.logout(
        redis=redis,
        access_token=credentials.credentials,
        refresh_token=body.refresh_token if body else None,
    )


@router.post("/logout-all", status_code=status.HTTP_204_NO_CONTENT)
async def logout_all(
    current_user: CurrentUser,
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
    redis: RedisClient,
) -> None:
    await AuthService.logout_all(
        redis=redis,
        user_id=current_user.id,
        access_token=credentials.credentials,
    )


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: CurrentUser) -> UserResponse:
    return current_user