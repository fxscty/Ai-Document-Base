from pydantic import PostgresDsn, RedisDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVER: str
    POSTGRES_PORT: str
    POSTGRES_DB: str

    @computed_field
    def database_url(self) -> PostgresDsn:
        return PostgresDsn(
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class JWTSettings(BaseSettings):
    SECRET_KEY: str
    ALGORITHM: str = 'HS256'
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class RedisSettings(BaseSettings):
    REDIS_URL: RedisDsn = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 20

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class KafkaSettings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_DOCUMENT_GROUP_ID: str = "document-processors"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
jwtsettings = JWTSettings()
redis_settings = RedisSettings()
kafka_settings = KafkaSettings()