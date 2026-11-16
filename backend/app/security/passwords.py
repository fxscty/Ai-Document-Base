import asyncio

from pwdlib import PasswordHash
from pwdlib.hashers.argon2 import Argon2Hasher

password_hasher = PasswordHash((Argon2Hasher(memory_cost=19456, time_cost=2, parallelism=1),))

async def hash_password(password: str) -> str:
    return await asyncio.to_thread(password_hasher.hash, password)

async def verify_password(password: str, hashed: str) -> bool:
    return await asyncio.to_thread(password_hasher.verify, password, hashed)