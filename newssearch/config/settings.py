import urllib3
from pydantic import computed_field
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


class HTTPTransportSettings(BaseSettings):
    user_agent: str = "NewsSearch/1.0 pet-project"

    max_retries: int = 3
    backoff_factor: float = 0.1
    max_backoff: int = 120
    status_forcelist: list[int] = [413, 429, 502, 503, 504]
    allowed_methods: frozenset[str] = urllib3.Retry.DEFAULT_ALLOWED_METHODS
    default_timeout: int = 30
    chunk_size: int = 8192


class NewsClientSettings(BaseSettings):
    base_url: str = "http://data.commoncrawl.org/crawl-data/CC-NEWS"
    file_url_path: str = "/{yyyy}/{mm}/CC-NEWS-{timestamp}-{id}.warc.gz"
    paths_url_path: str = "/{yyyy}/{mm}/warc.paths.gz"

    @computed_field  # type: ignore [prop-decorator]
    @property
    def paths_url(self) -> str:
        return self.base_url + self.paths_url_path

    @computed_field  # type: ignore [prop-decorator]
    @property
    def file_url(self) -> str:
        return self.base_url + self.file_url_path


class NewsETLSettings(BaseSettings):
    date_format: str = "%Y/%m"
    warc_paths_table_length: int = 10

    max_workers: int = 3


class Settings(BaseSettings):
    UVICORN_SETTINGS: UvicornSettings = UvicornSettings()
    HTTP_TRANSPORT_SETTINGS: HTTPTransportSettings = HTTPTransportSettings()
    NEWS_CLIENT_SETTINGS: NewsClientSettings = NewsClientSettings()
    NEWS_ETL_SETTINGS: NewsETLSettings = NewsETLSettings()
