from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote
from uuid import UUID, uuid4

from app.core.config import Settings
from app.models.api import GenerateRequest, GenerateResponse, TableArtifact
from app.services.artifact_store import artifact_store
from app.services.compliance import generate_compliance_markdown
from app.services.cost_estimator import estimate_databricks_cost
from app.services.domain_templates import resolve_template
from app.services.pyspark_codegen import generate_kafka_templates, generate_pyspark_pipeline_code
from app.services.schema_parser import parse_schema
from app.services.synthetic_data import basic_table_stats, generate_synthetic_data


def generate_synthforge_artifacts(payload: GenerateRequest, settings: Settings) -> GenerateResponse:
    request_id: UUID = uuid4()
    request_id_str = str(request_id)
    template = resolve_template(payload.domain, payload.template_id)
    parsed_schema = parse_schema(payload.schema_json)
    generation = generate_synthetic_data(
        parsed_schema=parsed_schema,
        prompt=payload.prompt,
        row_count=payload.row_count,
        domain=payload.domain,
        template=template,
        seed=settings.faker_seed,
    )

    tables = []
    for table_name, df in generation.tables.items():
        sample_rows = df.head(settings.sample_rows_returned).to_dict(orient="records")
        tables.append(
            TableArtifact(
                name=table_name,
                row_count=len(df),
                sample_rows=sample_rows,
                basic_stats=basic_table_stats(df),
            )
        )

    pyspark_code = generate_pyspark_pipeline_code(parsed_schema, payload.domain, settings)
    compliance_markdown = generate_compliance_markdown(
        payload.domain,
        payload.prompt,
        template,
        parsed_schema,
        generation.validation_metrics,
    )
    cost_estimate = estimate_databricks_cost(payload.row_count, settings)

    kafka_templates = generate_kafka_templates(payload.domain) if payload.include_kafka_templates else None
    artifact_store.put(
        request_id=request_id_str,
        domain=payload.domain,
        tables=generation.tables,
        ttl_minutes=settings.artifact_ttl_minutes,
        metadata={
            "template_id": template.template_id,
            "row_count": payload.row_count,
            "validation_metrics": generation.validation_metrics,
            "data_quality_report": generation.data_quality_report,
        },
    )
    audit_trail = generation.audit_trail + [
        f"Applied built-in template: {template.template_id}.",
        f"Stored downloadable synthetic artifacts in memory (TTL: {settings.artifact_ttl_minutes} minutes).",
        "Generated Databricks Bronze/Silver/Gold PySpark pipeline template.",
        "Generated compliance checklist (GDPR/HIPAA/PCI).",
        "Computed validation metrics and data quality report.",
        "Estimated Databricks run cost using configured defaults.",
    ]

    return GenerateResponse(
        request_id=request_id,
        generated_at=datetime.now(timezone.utc),
        domain=payload.domain,
        template_id=template.template_id,
        tables=tables,
        pyspark_pipeline_code=pyspark_code,
        kafka_templates=kafka_templates,
        compliance_markdown=compliance_markdown,
        cost_estimate=cost_estimate,
        audit_trail=audit_trail,
        validation_metrics=generation.validation_metrics if payload.include_validation_metrics else None,
        data_quality_report=generation.data_quality_report,
        download_endpoints={
            "zip": f"/api/v1/downloads/{request_id_str}/zip",
            "csv_by_table": {
                table_name: f"/api/v1/downloads/{request_id_str}/csv/{quote(table_name, safe='')}"
                for table_name in generation.tables.keys()
            },
            "parquet_by_table": {
                table_name: f"/api/v1/downloads/{request_id_str}/parquet/{quote(table_name, safe='')}"
                for table_name in generation.tables.keys()
            },
            "expires_in_minutes": settings.artifact_ttl_minutes,
        },
    )
