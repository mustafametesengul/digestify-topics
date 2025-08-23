from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    debug: bool = Field(default=...)
    jwks_url: str = Field(default=...)
    postgres_host: str = Field(default=...)
    postgres_port: int = Field(default=...)
    postgres_user: str = Field(default=...)
    postgres_password: str = Field(default=...)
    postgres_db: str = Field(default=...)
    redis_host: str = Field(default=...)
    redis_port: int = Field(default=...)
    redis_password: str = Field(default=...)
    openai_api_key: str = Field(default=...)


_settings = Settings()


def get_settings() -> Settings:
    global _settings
    return _settings
