from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker
from scipy.special import expit

from app.models.schema import FieldSpec, ParsedSchema, TableSpec
from app.services.domain_templates import DomainTemplate


@dataclass
class SyntheticGenerationResult:
    tables: dict[str, pd.DataFrame]
    audit_trail: list[str]
    validation_metrics: dict[str, Any]
    data_quality_report: dict[str, Any]


HEALTHCARE_CONDITIONS: dict[str, dict[str, Any]] = {
    "E11.9": {
        "description": "Type 2 diabetes mellitus without complications",
        "weight": 0.34,
        "severity": 1.15,
        "procedures": [
            ("83036", "Hemoglobin A1c", 1.0),
            ("99214", "Established patient office visit", 1.1),
        ],
    },
    "I10": {
        "description": "Essential primary hypertension",
        "weight": 0.30,
        "severity": 1.05,
        "procedures": [
            ("93000", "Electrocardiogram complete", 1.25),
            ("99213", "Office visit evaluation", 1.0),
        ],
    },
    "I25.10": {
        "description": "Atherosclerotic heart disease of native coronary artery",
        "weight": 0.22,
        "severity": 1.45,
        "procedures": [
            ("92928", "Percutaneous coronary intervention", 1.75),
            ("93306", "Echocardiography complete", 1.4),
        ],
    },
    "J45.909": {
        "description": "Unspecified asthma uncomplicated",
        "weight": 0.14,
        "severity": 0.95,
        "procedures": [
            ("94010", "Spirometry", 1.05),
            ("99214", "Established patient office visit", 1.1),
        ],
    },
}


def generate_synthetic_data(
    parsed_schema: ParsedSchema,
    prompt: str,
    row_count: int,
    domain: str,
    template: DomainTemplate,
    seed: int = 42,
) -> SyntheticGenerationResult:
    del prompt
    fake = Faker("en_US")
    Faker.seed(seed)
    np.random.seed(seed)

    ordered_tables = _topological_table_order(parsed_schema.tables)
    table_data: dict[str, pd.DataFrame] = {}
    audit_trail = [
        "No user-provided row values persisted; generation executed in-memory.",
        f"Detected {len(parsed_schema.tables)} table(s) and inferred relationships.",
        f"Applied domain profile: {domain}.",
        f"Applied domain template: {template.template_id}.",
        "Generation strategy: parse schema -> generate parents first -> generate FK children using correlated context.",
    ]

    for table in ordered_tables:
        target_rows = max(1, int(row_count * table.row_multiplier))
        table_name_lower = table.name.lower()
        if domain == "healthcare" and table_name_lower == "patients":
            table_data[table.name] = _generate_healthcare_patients_fast(table, target_rows, fake)
        elif domain == "healthcare" and table_name_lower in {"claims", "encounters"} and "patients" in table_data:
            fk = next((f for f in table.foreign_keys if f.ref_table == "patients"), None)
            fk_col = fk.column if fk else "patient_id"
            table_data[table.name] = _generate_healthcare_claims_fast(
                table=table,
                row_count=target_rows,
                patients_df=table_data["patients"],
                patient_fk_col=fk_col,
                fake=fake,
            )
        else:
            rows: list[dict[str, Any]] = []
            for idx in range(1, target_rows + 1):
                rows.append(
                    _generate_record(
                        table=table,
                        idx=idx,
                        tables=table_data,
                        fake=fake,
                        domain=domain,
                    )
                )
            table_data[table.name] = pd.DataFrame(rows)
        audit_trail.append(f"Generated table '{table.name}' with {target_rows} synthetic rows.")

    validation_metrics = compute_validation_metrics(table_data, domain)
    data_quality_report = build_data_quality_report(table_data)

    return SyntheticGenerationResult(
        tables=table_data,
        audit_trail=audit_trail,
        validation_metrics=validation_metrics,
        data_quality_report=data_quality_report,
    )


def basic_table_stats(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "null_pct_by_column": {c: float(df[c].isna().mean()) for c in df.columns},
        "distinct_by_column": {c: int(df[c].nunique(dropna=True)) for c in df.columns},
    }


def build_data_quality_report(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {
        "table_count": len(tables),
        "tables": {name: basic_table_stats(df) for name, df in tables.items()},
    }


def compute_validation_metrics(tables: dict[str, pd.DataFrame], domain: str) -> dict[str, Any]:
    if domain != "healthcare":
        return {"domain": domain, "message": "Validation metrics currently specialized for healthcare template."}

    patients = tables.get("patients")
    claims = tables.get("claims")
    if patients is None or claims is None or len(patients) == 0 or len(claims) == 0:
        return {"domain": domain, "message": "Missing patients/claims tables for healthcare correlation validation."}

    patient_key = "id" if "id" in patients.columns else patients.columns[0]
    claim_fk = "patient_id" if "patient_id" in claims.columns else patient_key
    merged = claims.merge(patients, left_on=claim_fk, right_on=patient_key, how="inner", suffixes=("_claim", "_patient"))
    if len(merged) == 0:
        return {"domain": domain, "message": "No joined records available for validation metrics."}

    corr_cols = [c for c in ["age", "cost", "length_of_stay", "comorbidity_count", "readmission_risk_score"] if c in merged.columns]
    corr_matrix = merged[corr_cols].corr(numeric_only=True).round(4).to_dict() if len(corr_cols) >= 2 else {}

    age_bins = pd.cut(merged["age"], bins=[0, 39, 59, 120], labels=["18-39", "40-59", "60+"], include_lowest=True) if "age" in merged.columns else None
    grouped_age = (
        merged.assign(age_group=age_bins)
        .groupby("age_group", observed=True)[["cost", "length_of_stay", "readmission_risk_score"]]
        .agg(["mean", "std", "min", "max"])
        .round(3)
        .to_dict()
        if age_bins is not None and all(col in merged.columns for col in ["cost", "length_of_stay", "readmission_risk_score"])
        else {}
    )

    chronic_group = (
        merged.groupby("chronic_condition_flag")[["cost", "length_of_stay", "readmission_risk_score"]]
        .mean()
        .round(3)
        .to_dict()
        if "chronic_condition_flag" in merged.columns and all(col in merged.columns for col in ["cost", "length_of_stay", "readmission_risk_score"])
        else {}
    )

    return {
        "domain": domain,
        "joined_row_count": int(len(merged)),
        "correlation_matrix": corr_matrix,
        "group_summary_by_age": grouped_age,
        "group_summary_by_chronic_flag": chronic_group,
    }


def _generate_record(
    table: TableSpec,
    idx: int,
    tables: dict[str, pd.DataFrame],
    fake: Faker,
    domain: str,
) -> dict[str, Any]:
    record: dict[str, Any] = {}
    selected_parents: dict[str, dict[str, Any]] = {}

    for field in table.fields:
        if field.name == table.primary_key:
            record[field.name] = idx
            continue
        fk = next((f for f in table.foreign_keys if f.column == field.name), None)
        if fk and fk.ref_table in tables and len(tables[fk.ref_table]) > 0:
            parent_row = _sample_parent_row(tables[fk.ref_table])
            selected_parents[fk.ref_table] = parent_row
            record[field.name] = parent_row.get(fk.ref_column)

    context = _build_correlated_context(domain, table.name.lower(), selected_parents, fake)

    for field in table.fields:
        if field.name in record:
            record[field.name] = _apply_constraints(field, record[field.name])
            continue
        value = _generate_field_value(field, fake, context)
        record[field.name] = _apply_constraints(field, value)

    return record


def _sample_parent_row(df: pd.DataFrame) -> dict[str, Any]:
    idx = int(np.random.randint(0, len(df)))
    return df.iloc[idx].to_dict()


def _build_correlated_context(
    domain: str,
    table_name: str,
    parents: dict[str, dict[str, Any]],
    fake: Faker,
) -> dict[str, Any]:
    if domain == "healthcare":
        if table_name == "patients":
            return _healthcare_patient_context(fake)
        return _healthcare_claim_context(parents.get("patients"))
    if domain == "credit":
        return _credit_context()
    return _generic_context(fake)


def _generate_field_value(field: FieldSpec, fake: Faker, context: dict[str, Any]) -> Any:
    name = field.name.lower()

    if name in {"patient_name", "full_name", "name"}:
        return context.get("patient_name", fake.name())
    if name == "email":
        return context.get("email", fake.email())
    if name in {"zip", "zipcode", "zip_code", "postal_code"}:
        return context.get("zip_code", _realistic_us_zip(fake))
    if name in {"date_of_birth", "dob"}:
        age = int(context.get("age", np.random.randint(18, 80)))
        return date.today() - timedelta(days=age * 365 + int(np.random.randint(0, 365)))
    if name in {"age", "patient_age"}:
        return int(context.get("age", np.random.randint(18, 80)))
    if name in {"gender"}:
        return context.get("gender", np.random.choice(["Female", "Male", "Other"], p=[0.49, 0.49, 0.02]).item())
    if name in {"insurance_type"}:
        return context.get("insurance_type", np.random.choice(["Commercial", "Medicare", "Medicaid"]).item())
    if name in {"comorbidity_count"}:
        return int(context.get("comorbidity_count", np.random.poisson(2)))
    if name in {"chronic_condition_flag"}:
        return bool(context.get("chronic_condition_flag", np.random.binomial(1, 0.35)))
    if name in {"diagnosis_code", "icd10_code"}:
        return context.get("diagnosis_code", "E11.9")
    if name in {"diagnosis_description"}:
        return context.get("diagnosis_description", "Clinical diagnosis")
    if name in {"procedure_code", "cpt_code"}:
        return context.get("procedure_code", "99213")
    if name in {"procedure_description"}:
        return context.get("procedure_description", "Office visit evaluation")
    if name in {"length_of_stay", "los_days"}:
        return int(context.get("length_of_stay", 1))
    if name in {"cost", "claim_cost", "billed_amount", "amount"}:
        return float(context.get("cost", np.random.lognormal(mean=7.0, sigma=0.5)))
    if name in {"amount_paid"}:
        return float(context.get("amount_paid", np.random.uniform(250, 1500)))
    if name in {"readmission_risk_score"}:
        return float(context.get("readmission_risk_score", np.random.uniform(0.01, 0.25)))
    if name in {"lab_test_abnormal_flag"}:
        return bool(context.get("lab_test_abnormal_flag", np.random.binomial(1, 0.3)))
    if name in {"medication_prescribed_flag"}:
        return bool(context.get("medication_prescribed_flag", np.random.binomial(1, 0.7)))
    if name in {"claim_status"}:
        return context.get("claim_status", np.random.choice(["approved", "pending", "denied"]).item())
    if name in {"admission_type"}:
        return context.get("admission_type", np.random.choice(["elective", "urgent", "emergency"]).item())
    if name in {"provider_id"}:
        return f"PRV-{np.random.randint(100000, 999999)}"

    if field.faker:
        faker_fn = getattr(fake, field.faker, None)
        if callable(faker_fn):
            return faker_fn()
    if "name" in name:
        return fake.name()
    if "email" in name:
        return fake.email()
    if "date" in name and field.type in {"date", "datetime"}:
        d = date.today() - timedelta(days=int(np.random.randint(0, 1000)))
        return d if field.type == "date" else datetime.combine(d, datetime.min.time())

    return _generate_by_type(field, fake)


def _generate_by_type(field: FieldSpec, fake: Faker) -> Any:
    if field.type == "integer":
        low = int(field.min_value if field.min_value is not None else 0)
        high = int(field.max_value if field.max_value is not None else 10000)
        return int(np.random.randint(low, max(low + 1, high + 1)))
    if field.type == "float":
        low = float(field.min_value if field.min_value is not None else 0.0)
        high = float(field.max_value if field.max_value is not None else 1000.0)
        return float(low if high <= low else np.random.uniform(low, high))
    if field.type == "boolean":
        return bool(np.random.binomial(1, 0.5))
    if field.type == "date":
        return date.today() - timedelta(days=int(np.random.randint(0, 1000)))
    if field.type == "datetime":
        return datetime.utcnow() - timedelta(hours=int(np.random.randint(0, 24 * 365)))
    if "id" in field.name.lower():
        return fake.uuid4()
    return fake.word()


def _apply_constraints(field: FieldSpec, value: Any) -> Any:
    if field.allowed_values:
        if value in field.allowed_values:
            return value
        return np.random.choice(field.allowed_values).item()

    if field.type == "integer":
        v = int(value)
        if field.min_value is not None:
            v = max(v, int(field.min_value))
        if field.max_value is not None:
            v = min(v, int(field.max_value))
        return v
    if field.type == "float":
        v = float(value)
        if field.min_value is not None:
            v = max(v, float(field.min_value))
        if field.max_value is not None:
            v = min(v, float(field.max_value))
        return round(v, 2)
    return value


def _healthcare_patient_context(fake: Faker) -> dict[str, Any]:
    age = int(np.clip(np.random.normal(66, 12), 18, 95) if np.random.rand() < 0.45 else np.clip(np.random.normal(43, 14), 18, 95))
    chronic_probability = float(np.clip(expit((age - 53) / 8) * 0.85, 0.08, 0.96))
    chronic_condition_flag = bool(np.random.binomial(1, chronic_probability))

    comorb_lambda = 0.4 + (age / 45.0) + (1.8 if chronic_condition_flag else 0.0)
    comorbidity_count = int(np.clip(np.random.poisson(comorb_lambda), 0, 8))

    return {
        "patient_name": fake.name(),
        "email": fake.email(),
        "zip_code": _realistic_us_zip(fake),
        "age": age,
        "gender": np.random.choice(["Female", "Male", "Other"], p=[0.49, 0.49, 0.02]).item(),
        "insurance_type": np.random.choice(["Commercial", "Medicare", "Medicaid"], p=[0.44, 0.36, 0.20]).item(),
        "comorbidity_count": comorbidity_count,
        "chronic_condition_flag": chronic_condition_flag,
    }


def _healthcare_claim_context(patient: dict[str, Any] | None) -> dict[str, Any]:
    age = int(patient.get("age", np.random.randint(25, 85)) if patient else np.random.randint(25, 85))
    comorbidity_count = int(patient.get("comorbidity_count", np.random.poisson(2)) if patient else np.random.poisson(2))
    chronic_flag = bool(patient.get("chronic_condition_flag", np.random.binomial(1, 0.35)) if patient else np.random.binomial(1, 0.35))

    diagnosis_code = _sample_healthcare_condition(age, comorbidity_count, chronic_flag)
    condition = HEALTHCARE_CONDITIONS[diagnosis_code]
    proc_code, proc_desc, proc_complexity = condition["procedures"][int(np.random.randint(0, len(condition["procedures"])))]
    severity = float(condition["severity"])

    los_center = 1.2 + (age * 0.03) + (comorbidity_count * 0.62) + (proc_complexity * 0.8)
    length_of_stay = int(max(1, np.random.gamma(shape=2.3, scale=max(los_center / 2.3, 0.55))))

    base_cost = (
        250
        + (age * 16)
        + (comorbidity_count * 580)
        + (severity * 1400)
        + (proc_complexity * 900)
        + (length_of_stay * 410)
    )
    cost = float(np.random.lognormal(mean=np.log(max(base_cost, 200)), sigma=0.38))
    amount_paid = float(cost * np.random.uniform(0.66, 0.95))

    readmission_logit = (
        -4.6
        + (age * 0.03)
        + (comorbidity_count * 0.46)
        + (length_of_stay * 0.2)
        + (severity * 0.9)
        + (0.5 if chronic_flag else 0.0)
    )
    readmission_risk_score = float(np.clip(expit(readmission_logit), 0.01, 0.98))

    abnormal_prob = float(np.clip(0.16 + 0.07 * comorbidity_count + 0.06 * proc_complexity + (0.08 if chronic_flag else 0.0), 0.08, 0.96))
    medication_prob = float(np.clip(0.55 + 0.05 * comorbidity_count + 0.08 * severity + (0.08 if chronic_flag else 0.0), 0.45, 0.99))

    return {
        "age": age,
        "comorbidity_count": comorbidity_count,
        "chronic_condition_flag": chronic_flag,
        "diagnosis_code": diagnosis_code,
        "diagnosis_description": condition["description"],
        "procedure_code": proc_code,
        "procedure_description": proc_desc,
        "length_of_stay": length_of_stay,
        "cost": round(cost, 2),
        "amount_paid": round(amount_paid, 2),
        "readmission_risk_score": round(readmission_risk_score, 4),
        "lab_test_abnormal_flag": bool(np.random.binomial(1, abnormal_prob)),
        "medication_prescribed_flag": bool(np.random.binomial(1, medication_prob)),
        "claim_status": np.random.choice(["approved", "pending", "denied"], p=[0.82, 0.13, 0.05]).item(),
        "admission_type": np.random.choice(["elective", "urgent", "emergency"], p=[0.35, 0.34, 0.31]).item(),
    }


def _sample_healthcare_condition(age: int, comorbidity_count: int, chronic_flag: bool) -> str:
    codes = list(HEALTHCARE_CONDITIONS.keys())
    base_weights = np.array([HEALTHCARE_CONDITIONS[c]["weight"] for c in codes], dtype=float)
    chronic_boost = np.array([0.11, 0.09, 0.13, -0.08], dtype=float) if chronic_flag else np.array([0.01, 0.01, -0.02, 0.02], dtype=float)
    senior_boost = np.array([0.04, 0.06, 0.12, -0.05], dtype=float) if age >= 60 else np.array([-0.03, -0.02, -0.05, 0.10], dtype=float)
    comorb_boost = min(comorbidity_count, 8) * np.array([0.01, 0.012, 0.02, -0.01], dtype=float)

    weights = np.clip(base_weights + chronic_boost + senior_boost + comorb_boost, 0.01, None)
    weights = weights / weights.sum()
    return np.random.choice(codes, p=weights).item()


def _credit_context() -> dict[str, Any]:
    mcc_weights = {"5411": 0.28, "5812": 0.22, "5999": 0.20, "4111": 0.18, "4900": 0.12}
    mccs = list(mcc_weights)
    probs = np.array([mcc_weights[m] for m in mccs], dtype=float)
    probs = probs / probs.sum()
    mcc = np.random.choice(mccs, p=probs).item()

    event_hour = int(np.random.randint(0, 24))
    amount = float(np.random.lognormal(mean=3.1, sigma=0.95))
    mcc_mult = {"5411": 0.8, "5812": 1.0, "5999": 1.15, "4111": 0.7, "4900": 1.3}[mcc]
    amount = round(amount * mcc_mult, 2)

    night_risk = 0.018 if event_hour in {0, 1, 2, 3, 4, 23} else 0.0
    high_amount_risk = min(0.12, max(0.0, (amount - 250.0) / 5000.0))
    mcc_risk = {"5999": 0.018, "5812": 0.012, "5411": 0.004, "4111": 0.008, "4900": 0.01}[mcc]
    fraud_probability = float(min(0.45, 0.008 + night_risk + high_amount_risk + mcc_risk))
    return {"mcc": mcc, "event_hour": event_hour, "cost": amount, "fraud_probability": fraud_probability}


def _generic_context(fake: Faker) -> dict[str, Any]:
    return {
        "patient_name": fake.name(),
        "email": fake.email(),
        "zip_code": _realistic_us_zip(fake),
        "age": int(np.random.randint(18, 80)),
        "comorbidity_count": int(np.random.poisson(2)),
    }


def _realistic_us_zip(fake: Faker) -> str:
    # Weighted first digit distribution (USPS region-like spread).
    region_digit = np.random.choice(
        ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        p=[0.10, 0.12, 0.10, 0.11, 0.11, 0.11, 0.10, 0.09, 0.09, 0.07],
    ).item()
    zip_raw = fake.zipcode()
    numeric = "".join(ch for ch in zip_raw if ch.isdigit())
    if len(numeric) < 5:
        numeric = f"{np.random.randint(0, 100000):05d}"
    zip5 = f"{region_digit}{numeric[1:5]}"
    return zip5


def _generate_healthcare_patients_fast(table: TableSpec, row_count: int, fake: Faker) -> pd.DataFrame:
    n = row_count
    primary_key = table.primary_key

    # Cached Faker pools reduce provider-call overhead for large n.
    first_name_pool = np.array([fake.first_name() for _ in range(500)], dtype=object)
    last_name_pool = np.array([fake.last_name() for _ in range(500)], dtype=object)
    fn = first_name_pool[np.random.randint(0, len(first_name_pool), n)]
    ln = last_name_pool[np.random.randint(0, len(last_name_pool), n)]
    patient_name = np.char.add(np.char.add(fn.astype(str), " "), ln.astype(str))

    email_local = np.char.lower(np.char.add(np.char.add(fn.astype(str), "."), ln.astype(str)))
    email_suffix = np.random.randint(10, 999, n).astype(str)
    email = np.char.add(np.char.add(email_local, email_suffix), "@examplehealth.org")

    age = np.where(
        np.random.rand(n) < 0.45,
        np.clip(np.random.normal(66, 12, n), 18, 95),
        np.clip(np.random.normal(43, 14, n), 18, 95),
    ).astype(int)
    chronic_probability = np.clip(expit((age - 53) / 8) * 0.85, 0.08, 0.96)
    chronic_condition_flag = (np.random.rand(n) < chronic_probability).astype(bool)
    comorb_lambda = 0.4 + (age / 45.0) + np.where(chronic_condition_flag, 1.8, 0.0)
    comorbidity_count = np.clip(np.random.poisson(comorb_lambda), 0, 8).astype(int)

    gender = np.random.choice(["Female", "Male", "Other"], size=n, p=[0.49, 0.49, 0.02])
    insurance_type = np.random.choice(["Commercial", "Medicare", "Medicaid"], size=n, p=[0.44, 0.36, 0.20])
    region_digit = np.random.choice(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"], size=n, p=[0.10, 0.12, 0.10, 0.11, 0.11, 0.11, 0.10, 0.09, 0.09, 0.07])
    zip_code = np.char.add(region_digit.astype(str), np.char.zfill(np.random.randint(0, 10000, n).astype(str), 4))
    date_of_birth = np.array(
        [date.today() - timedelta(days=int(a) * 365 + int(np.random.randint(0, 365))) for a in age],
        dtype=object,
    )

    col_map: dict[str, Any] = {
        primary_key: np.arange(1, n + 1, dtype=int),
        "id": np.arange(1, n + 1, dtype=int),
        "patient_name": patient_name,
        "email": email,
        "zip_code": zip_code,
        "age": age,
        "gender": gender,
        "insurance_type": insurance_type,
        "comorbidity_count": comorbidity_count,
        "chronic_condition_flag": chronic_condition_flag,
        "date_of_birth": date_of_birth,
    }
    return _build_df_from_table_spec(table, n, col_map, fake)


def _generate_healthcare_claims_fast(
    table: TableSpec,
    row_count: int,
    patients_df: pd.DataFrame,
    patient_fk_col: str,
    fake: Faker,
) -> pd.DataFrame:
    n = row_count
    primary_key = table.primary_key

    patient_ids = patients_df["id"].to_numpy() if "id" in patients_df.columns else np.arange(1, len(patients_df) + 1)
    sampled_idx = np.random.randint(0, len(patient_ids), size=n)
    sampled_patient_ids = patient_ids[sampled_idx]

    age = _safe_sample_numeric(patients_df, "age", sampled_idx, default_low=25, default_high=85)
    comorbidity_count = _safe_sample_numeric(patients_df, "comorbidity_count", sampled_idx, default_low=0, default_high=5)
    chronic = _safe_sample_bool(patients_df, "chronic_condition_flag", sampled_idx, default_p=0.35)

    diagnosis_code = _sample_condition_vectorized(age, comorbidity_count, chronic)
    condition_data = [HEALTHCARE_CONDITIONS[c] for c in diagnosis_code]
    diagnosis_description = np.array([c["description"] for c in condition_data], dtype=object)
    severity = np.array([float(c["severity"]) for c in condition_data], dtype=float)

    proc_code = np.empty(n, dtype=object)
    proc_desc = np.empty(n, dtype=object)
    proc_complexity = np.empty(n, dtype=float)
    for i, c in enumerate(condition_data):
        code, desc, complexity = c["procedures"][int(np.random.randint(0, len(c["procedures"])))]
        proc_code[i] = code
        proc_desc[i] = desc
        proc_complexity[i] = complexity

    los_center = 1.2 + (age * 0.03) + (comorbidity_count * 0.62) + (proc_complexity * 0.8)
    length_of_stay = np.maximum(1, np.random.gamma(shape=2.3, scale=np.maximum(los_center / 2.3, 0.55))).astype(int)

    base_cost = 250 + (age * 16) + (comorbidity_count * 580) + (severity * 1400) + (proc_complexity * 900) + (length_of_stay * 410)
    cost = np.random.lognormal(mean=np.log(np.maximum(base_cost, 200)), sigma=0.38)
    amount_paid = cost * np.random.uniform(0.66, 0.95, n)

    readmission_logit = -4.6 + (age * 0.03) + (comorbidity_count * 0.46) + (length_of_stay * 0.2) + (severity * 0.9) + np.where(chronic, 0.5, 0.0)
    readmission_risk_score = np.clip(expit(readmission_logit), 0.01, 0.98)

    abnormal_prob = np.clip(0.16 + 0.07 * comorbidity_count + 0.06 * proc_complexity + np.where(chronic, 0.08, 0.0), 0.08, 0.96)
    medication_prob = np.clip(0.55 + 0.05 * comorbidity_count + 0.08 * severity + np.where(chronic, 0.08, 0.0), 0.45, 0.99)
    lab_flag = (np.random.rand(n) < abnormal_prob).astype(bool)
    med_flag = (np.random.rand(n) < medication_prob).astype(bool)

    claim_status = np.random.choice(["approved", "pending", "denied"], size=n, p=[0.82, 0.13, 0.05])
    admission_type = np.random.choice(["elective", "urgent", "emergency"], size=n, p=[0.35, 0.34, 0.31])
    provider_id = np.char.add("PRV-", np.random.randint(100000, 999999, n).astype(str))
    service_date = np.array(
        [date.today() - timedelta(days=int(np.random.randint(0, 365 * 2))) for _ in range(n)],
        dtype=object,
    )
    claim_date = service_date

    col_map: dict[str, Any] = {
        primary_key: np.arange(1, n + 1, dtype=int),
        "id": np.arange(1, n + 1, dtype=int),
        patient_fk_col: sampled_patient_ids,
        "patient_id": sampled_patient_ids,
        "diagnosis_code": diagnosis_code,
        "diagnosis_description": diagnosis_description,
        "procedure_code": proc_code,
        "procedure_description": proc_desc,
        "length_of_stay": length_of_stay,
        "cost": np.round(cost, 2),
        "amount_paid": np.round(amount_paid, 2),
        "readmission_risk_score": np.round(readmission_risk_score, 4),
        "lab_test_abnormal_flag": lab_flag,
        "medication_prescribed_flag": med_flag,
        "claim_status": claim_status,
        "admission_type": admission_type,
        "provider_id": provider_id,
        "service_date": service_date,
        "claim_date": claim_date,
        "age": age,
        "comorbidity_count": comorbidity_count,
        "chronic_condition_flag": chronic,
    }
    return _build_df_from_table_spec(table, n, col_map, fake)


def _build_df_from_table_spec(table: TableSpec, n: int, col_map: dict[str, Any], fake: Faker) -> pd.DataFrame:
    data: dict[str, Any] = {}
    for field in table.fields:
        if field.name in col_map:
            series = pd.Series(col_map[field.name])
        else:
            series = _generate_series_by_type(field, n, fake)
        data[field.name] = _apply_constraints_series(field, series).tolist()
    return pd.DataFrame(data)


def _generate_series_by_type(field: FieldSpec, n: int, fake: Faker) -> pd.Series:
    if field.type == "integer":
        low = int(field.min_value if field.min_value is not None else 0)
        high = int(field.max_value if field.max_value is not None else 10000)
        return pd.Series(np.random.randint(low, max(low + 1, high + 1), size=n))
    if field.type == "float":
        low = float(field.min_value if field.min_value is not None else 0.0)
        high = float(field.max_value if field.max_value is not None else 1000.0)
        vals = np.full(n, low) if high <= low else np.random.uniform(low, high, size=n)
        return pd.Series(vals)
    if field.type == "boolean":
        return pd.Series((np.random.rand(n) < 0.5).astype(bool))
    if field.type == "date":
        return pd.Series([date.today() - timedelta(days=int(np.random.randint(0, 1000))) for _ in range(n)])
    if field.type == "datetime":
        return pd.Series([datetime.utcnow() - timedelta(hours=int(np.random.randint(0, 24 * 365))) for _ in range(n)])
    if "name" in field.name.lower():
        return pd.Series([fake.name() for _ in range(n)])
    if "email" in field.name.lower():
        return pd.Series([fake.email() for _ in range(n)])
    return pd.Series([fake.word() for _ in range(n)])


def _apply_constraints_series(field: FieldSpec, series: pd.Series) -> pd.Series:
    out = series
    if field.allowed_values:
        allowed = set(field.allowed_values)
        if not out.isin(allowed).all():
            fallback = np.random.choice(field.allowed_values, size=len(out))
            out = pd.Series(np.where(out.isin(allowed), out, fallback))
    if field.type == "integer":
        out = out.astype(int)
        if field.min_value is not None:
            out = out.clip(lower=int(field.min_value))
        if field.max_value is not None:
            out = out.clip(upper=int(field.max_value))
    if field.type == "float":
        out = out.astype(float)
        if field.min_value is not None:
            out = out.clip(lower=float(field.min_value))
        if field.max_value is not None:
            out = out.clip(upper=float(field.max_value))
        out = out.round(2)
    return out


def _safe_sample_numeric(df: pd.DataFrame, col: str, idx: np.ndarray, default_low: int, default_high: int) -> np.ndarray:
    if col in df.columns:
        return df[col].to_numpy()[idx].astype(float)
    return np.random.randint(default_low, default_high + 1, size=len(idx)).astype(float)


def _safe_sample_bool(df: pd.DataFrame, col: str, idx: np.ndarray, default_p: float) -> np.ndarray:
    if col in df.columns:
        return df[col].to_numpy()[idx].astype(bool)
    return (np.random.rand(len(idx)) < default_p).astype(bool)


def _sample_condition_vectorized(age: np.ndarray, comorbidity_count: np.ndarray, chronic: np.ndarray) -> np.ndarray:
    codes = np.array(list(HEALTHCARE_CONDITIONS.keys()), dtype=object)
    out = np.empty(len(age), dtype=object)
    masks = [
        (age >= 60) & chronic,
        (age >= 60) & (~chronic),
        (age < 60) & chronic,
        (age < 60) & (~chronic),
    ]
    probs = [
        [0.34, 0.28, 0.30, 0.08],
        [0.33, 0.31, 0.22, 0.14],
        [0.39, 0.30, 0.19, 0.12],
        [0.29, 0.28, 0.16, 0.27],
    ]
    for mask, p in zip(masks, probs):
        count = int(mask.sum())
        if count == 0:
            continue
        sampled = np.random.choice(codes, size=count, p=p)
        # extra comorbidity push toward cardiac/diabetes
        boost_idx = np.where(mask & (comorbidity_count >= 5))[0]
        if len(boost_idx) > 0:
            sampled_boost = np.random.choice(np.array(["E11.9", "I25.10"], dtype=object), size=len(boost_idx), p=[0.6, 0.4])
            out[boost_idx] = sampled_boost
        base_idx = np.where(mask & ~(comorbidity_count >= 5))[0]
        out[base_idx] = sampled[: len(base_idx)]
    return out


def _topological_table_order(tables: list[TableSpec]) -> list[TableSpec]:
    table_map = {t.name: t for t in tables}
    visited: set[str] = set()
    stack: list[TableSpec] = []

    def visit(table_name: str) -> None:
        if table_name in visited:
            return
        visited.add(table_name)
        table = table_map[table_name]
        for fk in table.foreign_keys:
            if fk.ref_table in table_map:
                visit(fk.ref_table)
        stack.append(table)

    for name in table_map:
        visit(name)
    return stack
