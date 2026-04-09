from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "SynthForge Backend"
    app_version: str = "0.1.0"
    environment: str = "dev"

    max_row_count: int = Field(default=10_000_000, ge=1)
    default_row_count: int = Field(default=100_000, ge=1)
    sample_rows_returned: int = Field(default=20, ge=1, le=200)

    unity_catalog_name: str = "main"
    unity_schema_name: str = "synthforge"
    delta_bronze_path: str = "/Volumes/main/synthforge/bronze"
    delta_silver_path: str = "/Volumes/main/synthforge/silver"
    delta_gold_path: str = "/Volumes/main/synthforge/gold"

    databricks_dbu_rate_usd: float = Field(default=0.55, ge=0)
    vm_hourly_rate_usd: float = Field(default=1.25, ge=0)
    default_workers: int = Field(default=4, ge=1)
    default_runtime_hours: float = Field(default=1.5, ge=0.1)

    faker_seed: int = 42
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    artifact_ttl_minutes: int = Field(default=60, ge=1, le=1440)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
