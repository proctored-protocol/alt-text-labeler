from __future__ import annotations

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

    # --- general -------------------------------------------------------------

    app_env: str = Field(default="local")
    log_level: str = Field(default="INFO")

    # --- database ------------------------------------------------------------

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/alt_labeler"
    )

    # --- firehose / consumers ------------------------------------------------

    firehose_base_uri: str = Field(default="wss://bsky.network/xrpc")
    firehose_stream_name: str = Field(default="subscribe_repos")

    head_tracker_resume_from_consumer_state: bool = Field(default=True)
    head_tracker_start_cursor: int | None = Field(default=None)

    intake_resume_from_consumer_state: bool = Field(default=True)
    intake_start_cursor: int | None = Field(default=None)

    # --- rules / labels ------------------------------------------------------

    rule_version: str = Field(default="v2")
    label_missing_alt: str = Field(default="missing-alt-text")
    label_partial_alt: str = Field(default="partial-alt-text")

    # --- apply worker --------------------------------------------------------

    apply_batch_size: int = Field(default=200)
    apply_lease_seconds: int = Field(default=60)
    apply_idle_sleep_seconds: float = Field(default=1.0)
    apply_max_attempts: int = Field(default=5)

    # --- publish worker ------------------------------------------------------

    publish_enabled: bool = Field(default=False)
    publish_backend: str = Field(default="ozone")

    publish_batch_size: int = Field(default=50)
    publish_lease_seconds: int = Field(default=90)
    publish_idle_sleep_seconds: float = Field(default=1.0)
    publish_max_attempts: int = Field(default=10)
    publish_backoff_base_seconds: int = Field(default=15)

    # --- visibility baseline worker -----------------------------------------

    visibility_batch_size: int = Field(default=50)
    visibility_lease_seconds: int = Field(default=180)
    visibility_idle_sleep_seconds: float = Field(default=1.0)
    visibility_max_attempts: int = Field(default=5)
    visibility_retry_seconds: int = Field(default=30)
    visibility_max_age_seconds: int = Field(default=7200)
    visibility_request_timeout_seconds: int = Field(default=30)
    visibility_initial_delay_seconds: int = Field(default=300)

    # --- visibility remediation worker --------------------------------------

    remediation_batch_size: int = Field(default=50)
    remediation_lease_seconds: int = Field(default=180)
    remediation_idle_sleep_seconds: float = Field(default=1.0)
    remediation_max_attempts: int = Field(default=2)

    remediation_first_delay_seconds: int = Field(default=300)
    remediation_second_delay_seconds: int = Field(default=600)

    remediation_check_timeout_seconds: int = Field(default=90)
    remediation_check_poll_seconds: int = Field(default=5)
    remediation_unlabel_sleep_seconds: float = Field(default=2.0)

    # --- control plane / watchdog -------------------------------------------

    watchdog_poll_seconds: int = Field(default=30)
    heartbeat_stale_seconds: int = Field(default=120)
    intake_stall_seconds: int = Field(default=180)
    target_lag_seconds_min: int = Field(default=30)
    target_lag_seconds_max: int = Field(default=60)

    # --- bluesky / ozone / verification -------------------------------------

    bsky_pds_url: str = Field(default="https://bsky.social")

    ozone_base_url: str | None = Field(default=None)
    ozone_proxy_did: str | None = Field(default=None)
    ozone_handle: str | None = Field(default=None)
    ozone_app_password: str | None = Field(default=None)

    verifier_labeler_did: str | None = Field(default=None)
    verifier_appview_url: str = Field(default="https://bsky.social")
    test_viewer_handle: str | None = Field(default=None)
    test_viewer_app_password: str | None = Field(default=None)

    # --- dashboard -----------------------------------------------------------

    dashboard_host: str = Field(default="0.0.0.0")
    dashboard_port: int = Field(default=8765)
    dashboard_username: str | None = Field(default=None)
    dashboard_password: str | None = Field(default=None)

    @field_validator("app_env")
    @classmethod
    def normalize_app_env(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("firehose_stream_name")
    @classmethod
    def normalize_firehose_stream_name(cls, value: str) -> str:
        return value.strip()

    @field_validator("publish_backend")
    @classmethod
    def normalize_publish_backend(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator(
        "head_tracker_start_cursor",
        "intake_start_cursor",
        mode="before",
    )
    @classmethod
    def blank_cursor_to_none(cls, value):
        if value in ("", None):
            return None
        return value

    @field_validator(
        "ozone_base_url",
        "ozone_proxy_did",
        "ozone_handle",
        "ozone_app_password",
        "verifier_labeler_did",
        "test_viewer_handle",
        "test_viewer_app_password",
        "dashboard_username",
        "dashboard_password",
        mode="before",
    )
    @classmethod
    def blank_string_to_none(cls, value):
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()