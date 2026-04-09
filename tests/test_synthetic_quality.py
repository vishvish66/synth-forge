from __future__ import annotations

from app.services.schema_parser import parse_schema
from app.services.synthetic_data import compute_validation_metrics, generate_synthetic_data
from app.services.domain_templates import resolve_template


def test_healthcare_correlations_and_referential_integrity() -> None:
    payload = {
        "tables": [
            {
                "name": "patients",
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": False},
                    {"name": "patient_name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "zip_code", "type": "string"},
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
                    {"name": "length_of_stay", "type": "integer"},
                    {"name": "cost", "type": "float"},
                    {"name": "readmission_risk_score", "type": "float"},
                    {"name": "service_date", "type": "timestamp"},
                ],
            },
        ]
    }
    parsed = parse_schema(payload)
    template = resolve_template("healthcare", "healthcare_claims")
    result = generate_synthetic_data(
        parsed_schema=parsed,
        prompt="quality test",
        row_count=3000,
        domain="healthcare",
        template=template,
        seed=42,
    )

    patients = result.tables["patients"]
    claims = result.tables["claims"]
    assert len(patients) == 3000
    assert len(claims) == 3000

    # Referential integrity
    assert set(claims["patient_id"].unique()).issubset(set(patients["id"].unique()))

    merged = claims.merge(patients, left_on="patient_id", right_on="id", how="inner", suffixes=("_claim", "_patient"))
    assert len(merged) > 0

    corr_age_cost = merged["age"].corr(merged["cost"])
    corr_age_los = merged["age"].corr(merged["length_of_stay"])
    corr_comorb_readmit = merged["comorbidity_count"].corr(merged["readmission_risk_score"])

    assert corr_age_cost > 0.10
    assert corr_age_los > 0.08
    assert corr_comorb_readmit > 0.10

    # ZIP format sanity
    assert patients["zip_code"].astype(str).str.fullmatch(r"\d{5}").all()

    # Name/email quality sanity
    assert patients["patient_name"].astype(str).str.contains(" ").mean() > 0.9
    assert patients["email"].astype(str).str.contains("@").mean() > 0.95

    metrics = compute_validation_metrics({"patients": patients, "claims": claims}, "healthcare")
    assert "correlation_matrix" in metrics
