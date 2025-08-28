from pydantic_settings import BaseSettings, SettingsConfigDict


class UvicornSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="uvicorn__",
        env_file=".env",
        extra="ignore",
    )

    app: str = "newssearch.main:app"
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "info"
    reload: bool = False
    limit_max_requests: int | None = None


class Settings(BaseSettings):
    UVICORN_SETTINGS: UvicornSettings = UvicornSettings()
