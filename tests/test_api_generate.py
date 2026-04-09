from __future__ import annotations

from fastapi.testclient import TestClient


def test_generate_returns_expected_artifacts(client: TestClient, healthcare_payload: dict) -> None:
    res = client.post("/api/v1/generate", json=healthcare_payload)
    assert res.status_code == 200, res.text
    body = res.json()

    assert body["domain"] == "healthcare"
    assert body["template_id"] in {"healthcare_claims_outcomes_v1", "healthcare_claims"}
    assert body["tables"]
    assert "pyspark_pipeline_code" in body
    assert "compliance_markdown" in body
    assert "cost_estimate" in body
    assert "download_endpoints" in body
    assert "validation_metrics" in body
    assert "data_quality_report" in body


def test_download_csv_zip_parquet_and_validation(client: TestClient, healthcare_payload: dict) -> None:
    gen = client.post("/api/v1/generate", json=healthcare_payload)
    assert gen.status_code == 200, gen.text
    body = gen.json()
    request_id = body["request_id"]

    tables = [t["name"] for t in body["tables"]]
    assert "patients" in tables

    csv_url = body["download_endpoints"]["csv_by_table"]["patients"]
    zip_url = body["download_endpoints"]["zip"]
    parquet_url = body["download_endpoints"]["parquet_by_table"]["patients"]

    csv_res = client.get(csv_url)
    assert csv_res.status_code == 200
    assert "text/csv" in csv_res.headers["content-type"]
    assert b"patient_name" in csv_res.content

    zip_res = client.get(zip_url)
    assert zip_res.status_code == 200
    assert "application/zip" in zip_res.headers["content-type"]

    parquet_res = client.get(parquet_url)
    assert parquet_res.status_code in {200, 501}
    if parquet_res.status_code == 200:
        assert parquet_res.content

    val_res = client.get(f"/api/v1/validations/{request_id}")
    assert val_res.status_code == 200
    val_body = val_res.json()
    assert "validation_metrics" in val_body
    assert "data_quality_report" in val_body


def test_generate_rejects_row_count_over_limit(client: TestClient, healthcare_payload: dict) -> None:
    healthcare_payload["row_count"] = 20_000_001
    res = client.post("/api/v1/generate", json=healthcare_payload)
    assert res.status_code in {400, 422}
