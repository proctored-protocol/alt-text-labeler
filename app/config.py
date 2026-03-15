from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = Field(default="local")
    log_level: str = Field(default="INFO")

    firehose_base_uri: str = Field(default="wss://bsky.network/xrpc")
    firehose_cursor: int | None = Field(default=None)
    firehose_dry_run: bool = Field(default=True)

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/alt_labeler"
    )

    label_missing_alt: str = Field(default="missing-alt-text")
    label_partial_alt: str = Field(default="partial-alt-text")

    bsky_handle: str | None = Field(default=None)
    bsky_app_password: str | None = Field(default=None)
    bsky_pds_url: str = Field(default="https://bsky.social")

    test_viewer_handle: str | None = Field(default=None)
    test_viewer_app_password: str | None = Field(default=None)

    ozone_base_url: str | None = Field(default=None)
    ozone_proxy_did: str | None = Field(default=None)
    ozone_handle: str | None = Field(default=None)
    ozone_app_password: str | None = Field(default=None)
    publish_via_ozone: bool = Field(default=False)

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper().strip()

    @field_validator("firehose_cursor", mode="before")
    @classmethod
    def blank_cursor_to_none(cls, value):
        if value in ("", None):
            return None
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()