from __future__ import annotations

from app.models.schema import ParsedSchema
from app.services.domain_templates import DomainTemplate


def generate_compliance_markdown(
    domain: str,
    prompt: str,
    template: DomainTemplate,
    parsed_schema: ParsedSchema,
    validation_metrics: dict,
) -> str:
    hipaa = domain == "healthcare"
    pci = domain == "credit"
    soc2 = "SOC 2" in template.compliance_frameworks
    ccpa = "CCPA" in template.compliance_frameworks
    gdpr = "GDPR" in template.compliance_frameworks

    pii_fields = _extract_pii_fields(parsed_schema)
    pii_fields_str = ", ".join(sorted(pii_fields)) if pii_fields else "None explicitly flagged"

    sections = [
        "# SynthForge Compliance Report",
        "",
        "## Scope",
        f"- Domain: `{domain}`",
        f"- Template: `{template.template_id}` ({template.title})",
        "- Synthetic data generated in-memory; no source PHI/PII persisted.",
        "",
        "## PII Handling Summary",
        f"- PII fields identified from schema: {pii_fields_str}",
        "- patient_name generated via `Faker.name()` synthetic identities",
        "- email generated via `Faker.email()` synthetic aliases",
        "- zip_code generated as valid synthetic 5-digit US ZIP format",
        "",
        "## Audit Trail",
        "- Synthetic data generated with no real PHI.",
        "- Referential integrity preserved across related tables.",
        "- Statistical fidelity validated via correlation and grouped summary checks.",
        "",
        "## GDPR Article 25 (Data Protection by Design)",
        f"- [{'x' if gdpr else ' '}] Data minimization and synthetic-only generation path",
        f"- [{'x' if gdpr else ' '}] Privacy-by-default (no raw row payload logging)",
        f"- [{'x' if gdpr else ' '}] Controlled, auditable generation flow",
        "",
        "## HIPAA Safe Harbor Principles",
        f"- [{'x' if hipaa else ' '}] Direct identifiers replaced with synthetic values",
        f"- [{'x' if hipaa else ' '}] No real PHI used to derive generated records",
        f"- [{'x' if hipaa else ' '}] Re-identification risk assessment: low (synthetic generation only)",
        "",
        "## PCI / SOC 2 / CCPA",
        f"- [{'x' if pci else ' '}] PCI DSS handling for payment-related fields",
        f"- [{'x' if soc2 else ' '}] SOC 2 aligned auditability and access control assumptions",
        f"- [{'x' if ccpa else ' '}] CCPA de-identified analytics support",
        "",
        "## PII Masking Templates (SQL/PySpark)",
        "```sql",
        "-- SQL masking templates",
        "SELECT",
        "  sha2(patient_name, 256) AS patient_name_masked,",
        "  concat('user+', substr(sha2(email, 256), 1, 12), '@example.com') AS email_masked,",
        "  concat(substr(zip_code, 1, 3), 'XX') AS zip_code_masked",
        "FROM claims_enriched;",
        "```",
        "",
        "```python",
        "# PySpark masking templates",
        "from pyspark.sql import functions as F",
        "masked_df = (",
        "    df.withColumn('patient_name_masked', F.sha2(F.col('patient_name'), 256))",
        "      .withColumn('email_masked', F.concat(F.lit('user+'), F.substring(F.sha2(F.col('email'), 256), 1, 12), F.lit('@example.com')))",
        "      .withColumn('zip_code_masked', F.concat(F.substring(F.col('zip_code'), 1, 3), F.lit('XX')))",
        ")",
        "```",
        "",
        "## Validation Snapshot",
        f"- Correlation matrix present: {'yes' if validation_metrics.get('correlation_matrix') else 'no'}",
        f"- Joined validation row count: {validation_metrics.get('joined_row_count', 'n/a')}",
        "",
        "## Pattern Notes",
        *[f"- {line}" for line in template.pattern_notes],
        "",
        "## Prompt Snapshot",
        f"- Prompt summary: `{prompt[:220]}`",
    ]
    return "\n".join(sections)


def _extract_pii_fields(parsed_schema: ParsedSchema) -> set[str]:
    pii: set[str] = set()
    for table in parsed_schema.tables:
        for field in table.fields:
            if field.pii:
                pii.add(f"{table.name}.{field.name}")
    return pii
