from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def healthcare_payload() -> dict:
    return {
        "schema_json": {
            "tables": [
                {
                    "name": "patients",
                    "primary_key": "id",
                    "fields": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "patient_name", "type": "string", "pii": True},
                        {"name": "email", "type": "string", "pii": True},
                        {"name": "zip_code", "type": "string", "pii": True},
                        {"name": "age", "type": "integer"},
                        {"name": "comorbidity_count", "type": "integer"},
                        {"name": "chronic_condition_flag", "type": "boolean"},
                    ],
                },
                {
                    "name": "claims",
                    "primary_key": "id",
                    "fields": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "patient_id", "type": "integer", "foreign_key": "patients.id"},
                        {"name": "diagnosis_code", "type": "string"},
                        {"name": "procedure_code", "type": "string"},
                        {"name": "length_of_stay", "type": "integer"},
                        {"name": "cost", "type": "float"},
                        {"name": "amount_paid", "type": "float"},
                        {"name": "readmission_risk_score", "type": "float"},
                        {"name": "service_date", "type": "timestamp"},
                    ],
                },
            ]
        },
        "prompt": "Generate healthcare synthetic data with strong correlations and compliance output.",
        "row_count": 2000,
        "domain": "healthcare",
        "template_id": "healthcare_claims",
        "include_kafka_templates": False,
        "include_validation_metrics": True,
    }
