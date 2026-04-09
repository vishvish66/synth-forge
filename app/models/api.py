from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

DomainType = Literal["credit", "healthcare"]


class GenerateRequest(BaseModel):
    schema_json: dict[str, Any]
    prompt: str = Field(min_length=10, max_length=4000)
    row_count: int = Field(default=100_000, ge=1, le=10_000_000)
    domain: DomainType
    template_id: str | None = None
    include_kafka_templates: bool = False
    include_validation_metrics: bool = True

    @field_validator("schema_json")
    @classmethod
    def validate_schema_json(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("schema_json cannot be empty")
        return value


class TableArtifact(BaseModel):
    name: str
    row_count: int
    sample_rows: list[dict[str, Any]]
    basic_stats: dict[str, Any]


class CostEstimate(BaseModel):
    workers: int
    runtime_hours: float
    dbu_rate_usd: float
    vm_hourly_rate_usd: float
    total_estimated_cost_usd: float
    assumptions: list[str]


class GenerateResponse(BaseModel):
    request_id: UUID
    generated_at: datetime
    domain: DomainType
    template_id: str
    tables: list[TableArtifact]
    pyspark_pipeline_code: str
    kafka_templates: dict[str, str] | None = None
    compliance_markdown: str
    cost_estimate: CostEstimate
    audit_trail: list[str]
    download_endpoints: dict[str, Any] | None = None
    validation_metrics: dict[str, Any] | None = None
    data_quality_report: dict[str, Any] | None = None
