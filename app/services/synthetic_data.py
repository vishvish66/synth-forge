from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker
from scipy.stats import lognorm

from app.models.schema import FieldSpec, ParsedSchema, TableSpec
from app.services.domain_templates import DomainTemplate
from app.services.schema_parser import get_schema_profile


@dataclass
class SyntheticGenerationResult:
    tables: dict[str, pd.DataFrame]
    audit_trail: list[str]
    validation_metrics: dict[str, Any]
    data_quality_report: dict[str, Any]


@dataclass
class CorrelationRule:
    source: str
    target: str
    direction: str  # "positive" | "negative"
    strength: float
    kind: str  # "num_num" | "bool_num" | "num_bool"


def generate_synthetic_data(
    parsed_schema: ParsedSchema,
    prompt: str,
    row_count: int,
    domain: str,
    template: DomainTemplate,
    seed: int = 42,
) -> SyntheticGenerationResult:
    del domain, template
    fake = Faker("en_US")
    Faker.seed(seed)
    np.random.seed(seed)

    profile = get_schema_profile(parsed_schema)
    strategy = _pick_generation_strategy(profile)
    corr_plan = _build_correlation_plan(parsed_schema, prompt)

    ordered_tables = _topological_table_order(parsed_schema.tables)
    table_data: dict[str, pd.DataFrame] = {}
    audit_trail = [
        "No user-provided row values persisted; generation executed in-memory.",
        f"Schema profile: tables={profile['total_tables']}, columns={profile['total_columns']}, tier={profile['size_tier']}.",
        f"Generation strategy selected: {strategy}.",
        f"Correlation rules inferred: {sum(len(v) for v in corr_plan.values())}.",
    ]

    for table in ordered_tables:
        target_rows = max(1, int(row_count * table.row_multiplier))
        rules = corr_plan.get(table.name, [])
        df = _generate_table(
            table=table,
            row_count=target_rows,
            parent_tables=table_data,
            fake=fake,
            strategy=strategy,
            rules=rules,
        )
        table_data[table.name] = df
        audit_trail.append(f"Generated table '{table.name}' with {target_rows} rows and {len(table.fields)} columns.")

    validation_metrics = compute_validation_metrics(table_data, parsed_schema)
    data_quality_report = build_data_quality_report(table_data)
    return SyntheticGenerationResult(
        tables=table_data,
        audit_trail=audit_trail,
        validation_metrics=validation_metrics,
        data_quality_report=data_quality_report,
    )


def _pick_generation_strategy(profile: dict[str, Any]) -> str:
    cols = int(profile["total_columns"])
    if cols < 50:
        return "vectorized_small"
    if cols < 200:
        return "vectorized_medium"
    return "batched_large"


def _generate_table(
    table: TableSpec,
    row_count: int,
    parent_tables: dict[str, pd.DataFrame],
    fake: Faker,
    strategy: str,
    rules: list[CorrelationRule],
) -> pd.DataFrame:
    if strategy != "batched_large":
        df = _generate_table_batch(table, row_count, parent_tables, fake, rules)
        return df

    # Large schemas: row-batch generation prevents oversized one-shot allocations.
    batch_size = 25_000
    chunks: list[pd.DataFrame] = []
    remaining = row_count
    pk_offset = 0
    while remaining > 0:
        n = min(batch_size, remaining)
        chunk = _generate_table_batch(table, n, parent_tables, fake, rules, pk_offset=pk_offset)
        chunks.append(chunk)
        pk_offset += n
        remaining -= n
    return pd.concat(chunks, ignore_index=True)


def _generate_table_batch(
    table: TableSpec,
    row_count: int,
    parent_tables: dict[str, pd.DataFrame],
    fake: Faker,
    rules: list[CorrelationRule],
    pk_offset: int = 0,
) -> pd.DataFrame:
    n = row_count
    data: dict[str, Any] = {}
    parent_context: dict[str, pd.Series] = {}

    # 1) Primary key
    data[table.primary_key] = np.arange(1 + pk_offset, n + 1 + pk_offset, dtype=int)

    # 2) FK sampling from generated parent IDs (100% referential integrity)
    for fk in table.foreign_keys:
        parent = parent_tables.get(fk.ref_table)
        if parent is None or len(parent) == 0:
            continue
        parent_col = fk.ref_column if fk.ref_column in parent.columns else parent.columns[0]
        sampled_idx = np.random.randint(0, len(parent), size=n)
        sampled_parent = parent.iloc[sampled_idx].reset_index(drop=True)
        sampled = sampled_parent[parent_col].to_numpy()
        data[fk.column] = sampled
        # Parent feature context for FK-driven cross-table correlations.
        for c in sampled_parent.columns:
            parent_context[f"{fk.ref_table}__{c}"] = sampled_parent[c]

    # 3) Remaining columns (vectorized by type/name)
    for field in table.fields:
        if field.name in data:
            data[field.name] = _apply_constraints_series(field, pd.Series(data[field.name])).to_numpy()
            continue
        data[field.name] = _generate_field_series(field, n, fake).to_numpy()

    df = pd.DataFrame(data, columns=[f.name for f in table.fields])
    df = _apply_parent_influence(df, table, parent_context)
    df = _apply_correlation_rules(df, table, rules)
    return df


def _generate_field_series(field: FieldSpec, n: int, fake: Faker) -> pd.Series:
    name = field.name.lower()

    if field.allowed_values:
        return pd.Series(np.random.choice(field.allowed_values, size=n))
    if field.faker and hasattr(fake, field.faker):
        provider = getattr(fake, field.faker)
        return pd.Series([provider() for _ in range(n)])
    if "email" in name:
        return pd.Series([fake.email() for _ in range(n)])
    if "name" in name:
        return pd.Series([fake.name() for _ in range(n)])
    if "zip" in name or "postal" in name:
        return pd.Series([_realistic_zip(fake) for _ in range(n)])
    if "phone" in name:
        return pd.Series([fake.phone_number() for _ in range(n)])
    if "id" in name and field.type == "string":
        return pd.Series([fake.uuid4() for _ in range(n)])

    if field.type == "integer":
        low = int(field.min_value if field.min_value is not None else 0)
        high = int(field.max_value if field.max_value is not None else 10000)
        return pd.Series(np.random.randint(low, max(low + 1, high + 1), size=n))
    if field.type == "float":
        low = float(field.min_value if field.min_value is not None else 0.0)
        high = float(field.max_value if field.max_value is not None else 1000.0)
        if "amount" in name or "price" in name or "cost" in name or "value" in name:
            sigma = 0.85
            scale = max((high - low) / 4 if high > low else 1.0, 1.0)
            vals = lognorm(s=sigma, scale=scale).rvs(size=n) + low
            vals = np.clip(vals, low, high if high > low else vals.max())
            return pd.Series(np.round(vals, 2))
        vals = np.full(n, low) if high <= low else np.random.uniform(low, high, size=n)
        return pd.Series(np.round(vals, 4))
    if field.type == "boolean":
        p = 0.18 if "fraud" in name or "risk" in name or "abnormal" in name else 0.5
        return pd.Series((np.random.rand(n) < p).astype(bool))
    if field.type == "date":
        return pd.Series([date.today() - timedelta(days=int(np.random.randint(0, 365 * 2))) for _ in range(n)])
    if field.type == "datetime":
        return pd.Series([datetime.utcnow() - timedelta(hours=int(np.random.randint(0, 24 * 365))) for _ in range(n)])
    return pd.Series([fake.word() for _ in range(n)])


def _apply_constraints_series(field: FieldSpec, series: pd.Series) -> pd.Series:
    out = series
    if field.allowed_values:
        allowed = set(field.allowed_values)
        if not out.isin(allowed).all():
            fallback = np.random.choice(field.allowed_values, size=len(out))
            out = pd.Series(np.where(out.isin(allowed), out, fallback))
    if field.type == "integer":
        out = pd.to_numeric(out, errors="coerce").fillna(0).astype(int)
        if field.min_value is not None:
            out = out.clip(lower=int(field.min_value))
        if field.max_value is not None:
            out = out.clip(upper=int(field.max_value))
    if field.type == "float":
        out = pd.to_numeric(out, errors="coerce").fillna(0.0).astype(float)
        if field.min_value is not None:
            out = out.clip(lower=float(field.min_value))
        if field.max_value is not None:
            out = out.clip(upper=float(field.max_value))
        out = out.round(4)
    return out


def _build_correlation_plan(parsed_schema: ParsedSchema, prompt: str) -> dict[str, list[CorrelationRule]]:
    prompt_lower = prompt.lower()
    table_rules: dict[str, list[CorrelationRule]] = {}

    driver_tokens = ("age", "tenure", "seniority", "duration", "quantity", "score")
    value_tokens = ("amount", "price", "cost", "value", "total")
    risk_tokens = ("risk", "fraud", "readmission", "probability", "score")
    flag_tokens = ("flag", "is_", "has_", "status")

    for table in parsed_schema.tables:
        fields = table.fields
        name_to_field = {f.name: f for f in fields}
        numeric = [f for f in fields if f.type in {"integer", "float"}]
        booleans = [f for f in fields if f.type == "boolean" or any(t in f.name.lower() for t in flag_tokens)]
        rules: list[CorrelationRule] = []

        drivers = [f for f in numeric if any(tok in f.name.lower() for tok in driver_tokens)]
        value_targets = [f for f in numeric if any(tok in f.name.lower() for tok in value_tokens)]
        risk_targets = [f for f in numeric if any(tok in f.name.lower() for tok in risk_tokens)]
        bool_targets = [f for f in fields if f.type == "boolean" and any(tok in f.name.lower() for tok in risk_tokens + value_tokens)]

        for d in drivers:
            for t in value_targets:
                if d.name == t.name:
                    continue
                rules.append(CorrelationRule(d.name, t.name, "positive", 0.35, "num_num"))
            for t in risk_targets:
                if d.name == t.name:
                    continue
                rules.append(CorrelationRule(d.name, t.name, "positive", 0.30, "num_num"))
            for t in bool_targets:
                rules.append(CorrelationRule(d.name, t.name, "positive", 0.25, "num_bool"))

        for b in booleans:
            for t in risk_targets + value_targets:
                if b.name == t.name:
                    continue
                rules.append(CorrelationRule(b.name, t.name, "positive", 0.22, "bool_num"))

        # Prompt-directed heuristic: detect "X higher Y" pattern.
        for source in name_to_field:
            for target in name_to_field:
                if source == target:
                    continue
                pattern = f"{source.lower()} higher {target.lower()}"
                if pattern in prompt_lower and name_to_field[source].type in {"integer", "float"} and name_to_field[target].type in {"integer", "float"}:
                    rules.append(CorrelationRule(source, target, "positive", 0.50, "num_num"))

        # Keep plan compact for very wide schemas.
        table_rules[table.name] = rules[:120]
    return table_rules


def _apply_correlation_rules(df: pd.DataFrame, table: TableSpec, rules: list[CorrelationRule]) -> pd.DataFrame:
    if not rules:
        return df
    field_map = {f.name: f for f in table.fields}

    for r in rules:
        if r.source not in df.columns or r.target not in df.columns:
            continue
        source = df[r.source]
        target = df[r.target]
        src_field = field_map.get(r.source)
        tgt_field = field_map.get(r.target)
        if src_field is None or tgt_field is None:
            continue

        sign = 1.0 if r.direction == "positive" else -1.0
        if r.kind == "num_num" and src_field.type in {"integer", "float"} and tgt_field.type in {"integer", "float"}:
            src_norm = _normalize_numeric(source)
            tgt = pd.to_numeric(target, errors="coerce").fillna(pd.to_numeric(target, errors="coerce").median())
            adjusted = tgt + sign * r.strength * src_norm * (tgt.std() if tgt.std() > 0 else 1.0)
            df[r.target] = _apply_constraints_series(tgt_field, adjusted)
        elif r.kind == "bool_num" and tgt_field.type in {"integer", "float"}:
            src_bool = source.astype(bool).astype(int)
            tgt = pd.to_numeric(target, errors="coerce").fillna(0.0)
            adjusted = tgt + sign * r.strength * src_bool * (tgt.std() if tgt.std() > 0 else 1.0)
            df[r.target] = _apply_constraints_series(tgt_field, adjusted)
        elif r.kind == "num_bool" and tgt_field.type == "boolean":
            src_norm = _normalize_numeric(source)
            logits = (src_norm * (2.2 * r.strength)).clip(-4, 4)
            probs = 1.0 / (1.0 + np.exp(-logits))
            df[r.target] = (np.random.rand(len(df)) < probs).astype(bool)
    return df


def _normalize_numeric(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)
    std = s.std()
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - s.mean()) / std


def _apply_parent_influence(df: pd.DataFrame, table: TableSpec, parent_context: dict[str, pd.Series]) -> pd.DataFrame:
    if not parent_context:
        return df
    cols = {f.name: f for f in table.fields}

    # Look for typical driver columns from parent rows.
    parent_age = _first_context_series(parent_context, suffix="__age")
    parent_tenure = _first_context_series(parent_context, suffix="__tenure")
    parent_comorb = _first_context_series(parent_context, suffix="__comorbidity_count")
    parent_driver = parent_age if parent_age is not None else parent_tenure

    if parent_driver is not None:
        d = _normalize_numeric(parent_driver)
        for target in df.columns:
            t = target.lower()
            field = cols.get(target)
            if field is None:
                continue
            if field.type in {"integer", "float"} and any(k in t for k in ["amount", "price", "cost", "value", "duration", "length_of_stay", "risk", "score"]):
                base = pd.to_numeric(df[target], errors="coerce").fillna(0.0)
                adjusted = base + (0.28 * d * (base.std() if base.std() > 0 else 1.0))
                df[target] = _apply_constraints_series(field, adjusted)
            if field.type == "boolean" and any(k in t for k in ["risk", "fraud", "abnormal", "flag"]):
                probs = (0.5 + 0.20 * d).clip(0.05, 0.95)
                df[target] = (np.random.rand(len(df)) < probs).astype(bool)

    if parent_comorb is not None:
        c = _normalize_numeric(parent_comorb)
        for target in df.columns:
            t = target.lower()
            field = cols.get(target)
            if field is None:
                continue
            if field.type in {"integer", "float"} and any(k in t for k in ["risk", "score", "cost", "length_of_stay"]):
                base = pd.to_numeric(df[target], errors="coerce").fillna(0.0)
                adjusted = base + (0.24 * c * (base.std() if base.std() > 0 else 1.0))
                df[target] = _apply_constraints_series(field, adjusted)
    return df


def _first_context_series(context: dict[str, pd.Series], suffix: str) -> pd.Series | None:
    for k, v in context.items():
        if k.endswith(suffix):
            return pd.to_numeric(v, errors="coerce").fillna(0.0)
    return None


def compute_validation_metrics(
    tables: dict[str, pd.DataFrame],
    parsed_schema_or_domain: ParsedSchema | str,
) -> dict[str, Any]:
    if isinstance(parsed_schema_or_domain, str):
        domain = parsed_schema_or_domain
        # Backward-compatible mode for existing callers/tests.
        if domain == "healthcare" and "patients" in tables and "claims" in tables:
            patients = tables["patients"]
            claims = tables["claims"]
            if not patients.empty and not claims.empty and "patient_id" in claims.columns and "id" in patients.columns:
                merged = claims.merge(patients, left_on="patient_id", right_on="id", how="inner")
                corr_cols = [
                    c for c in ["age", "cost", "length_of_stay", "comorbidity_count", "readmission_risk_score"]
                    if c in merged.columns
                ]
                corr = merged[corr_cols].corr(numeric_only=True).round(4).to_dict() if len(corr_cols) >= 2 else {}
                return {"correlation_matrix": corr, "joined_row_count": int(len(merged))}
        parsed_schema = ParsedSchema(tables=[], relationships={})
    else:
        parsed_schema = parsed_schema_or_domain

    table_metrics: dict[str, Any] = {}
    for t in parsed_schema.tables:
        df = tables.get(t.name)
        if df is None or df.empty:
            continue
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        selected = numeric_cols[:12]
        corr = df[selected].corr(numeric_only=True).round(4).to_dict() if len(selected) >= 2 else {}
        table_metrics[t.name] = {
            "rows": int(len(df)),
            "numeric_columns": selected,
            "correlation_matrix": corr,
        }

    fk_checks: list[dict[str, Any]] = []
    for t in parsed_schema.tables:
        child = tables.get(t.name)
        if child is None:
            continue
        for fk in t.foreign_keys:
            parent = tables.get(fk.ref_table)
            if parent is None or fk.column not in child.columns:
                continue
            parent_col = fk.ref_column if fk.ref_column in parent.columns else parent.columns[0]
            child_vals = set(child[fk.column].dropna().unique().tolist())
            parent_vals = set(parent[parent_col].dropna().unique().tolist())
            missing = child_vals - parent_vals
            fk_checks.append(
                {
                    "child_table": t.name,
                    "fk_column": fk.column,
                    "parent_table": fk.ref_table,
                    "parent_column": parent_col,
                    "valid_fk_rate": round(1.0 if len(child_vals) == 0 else 1 - (len(missing) / max(len(child_vals), 1)), 4),
                }
            )

    return {"table_metrics": table_metrics, "fk_integrity_checks": fk_checks}


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


def _realistic_zip(fake: Faker) -> str:
    zip_raw = fake.zipcode()
    digits = "".join(ch for ch in zip_raw if ch.isdigit())
    if len(digits) < 5:
        digits = f"{np.random.randint(0, 100000):05d}"
    return digits[:5]


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
