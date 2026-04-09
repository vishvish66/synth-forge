from __future__ import annotations

from io import BytesIO
import logging
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from app.core.config import Settings, get_settings
from app.models.api import GenerateRequest, GenerateResponse
from app.services.artifact_store import StoredArtifact, artifact_store
from app.services.orchestrator import generate_synthforge_artifacts
from app.services.schema_parser import SchemaParserError


logger = logging.getLogger(__name__)
router = APIRouter(tags=["generate"])


@router.post("/generate", response_model=GenerateResponse)
def generate_endpoint(
    payload: GenerateRequest,
    settings: Settings = Depends(get_settings),
) -> GenerateResponse:
    if payload.row_count > settings.max_row_count:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"row_count exceeds configured max_row_count={settings.max_row_count}",
        )
    try:
        result = generate_synthforge_artifacts(payload=payload, settings=settings)
    except SchemaParserError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Unexpected error during synthetic generation.",
            extra={"domain": payload.domain, "row_count": payload.row_count},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error while generating synthetic artifacts",
        ) from exc

    logger.info(
        "Synthetic artifacts generated.",
        extra={"domain": payload.domain, "row_count": payload.row_count, "tables": len(result.tables)},
    )
    return result


@router.get("/downloads/{request_id}/csv/{table_name}")
def download_table_csv(
    request_id: str,
    table_name: str,
) -> StreamingResponse:
    artifact = _get_artifact_or_404(request_id)
    if table_name not in artifact.tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"table_name='{table_name}' not found for request_id='{request_id}'",
        )

    csv_bytes = artifact.tables[table_name].to_csv(index=False).encode("utf-8")
    return StreamingResponse(
        content=iter([csv_bytes]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table_name}_{request_id}.csv"'},
    )


@router.get("/downloads/{request_id}/zip")
def download_all_tables_zip(
    request_id: str,
) -> StreamingResponse:
    artifact = _get_artifact_or_404(request_id)
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, mode="w", compression=ZIP_DEFLATED) as zf:
        for table_name, df in artifact.tables.items():
            zf.writestr(f"{table_name}.csv", df.to_csv(index=False))
    zip_buffer.seek(0)

    return StreamingResponse(
        content=zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="synthforge_{request_id}.zip"'},
    )


@router.get("/downloads/{request_id}/parquet/{table_name}")
def download_table_parquet(
    request_id: str,
    table_name: str,
) -> StreamingResponse:
    artifact = _get_artifact_or_404(request_id)
    if table_name not in artifact.tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"table_name='{table_name}' not found for request_id='{request_id}'",
        )

    parquet_buffer = BytesIO()
    try:
        artifact.tables[table_name].to_parquet(parquet_buffer, index=False, engine="pyarrow")
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Parquet export requires pyarrow. Install it and retry.",
        ) from exc
    parquet_buffer.seek(0)

    return StreamingResponse(
        content=parquet_buffer,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{table_name}_{request_id}.parquet"'},
    )


def _get_artifact_or_404(request_id: str) -> StoredArtifact:
    artifact = artifact_store.get(request_id)
    if artifact is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "Download artifact not found or expired. Generate again and download within the TTL window."
            ),
        )
    return artifact


@router.get("/validations/{request_id}")
def get_validation_metrics(request_id: str) -> dict:
    artifact = _get_artifact_or_404(request_id)
    return {
        "request_id": request_id,
        "template_id": artifact.metadata.get("template_id"),
        "row_count": artifact.metadata.get("row_count"),
        "validation_metrics": artifact.metadata.get("validation_metrics"),
        "data_quality_report": artifact.metadata.get("data_quality_report"),
        "expires_at_utc": artifact.expires_at.isoformat(),
    }
