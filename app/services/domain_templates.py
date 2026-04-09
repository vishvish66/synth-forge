from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class DomainTemplate:
    template_id: str
    domain: Literal["credit", "healthcare"]
    title: str
    description: str
    pattern_notes: list[str]
    compliance_frameworks: list[str]


TEMPLATES: dict[str, DomainTemplate] = {
    "experian_credit_risk_v1": DomainTemplate(
        template_id="experian_credit_risk_v1",
        domain="credit",
        title="Experian-Style Credit Risk",
        description=(
            "Credit card and account-level synthetic profile with amount/fraud and utilization/"
            "delinquency behavior patterns."
        ),
        pattern_notes=[
            "High-value and card-not-present transactions increase fraud likelihood.",
            "Higher utilization and recent delinquencies reduce synthetic score bands.",
            "Night-time and high-risk merchant categories are weighted for higher fraud rates.",
        ],
        compliance_frameworks=["GDPR", "PCI DSS", "SOC 2", "CCPA"],
    ),
    "healthcare_claims_outcomes_v1": DomainTemplate(
        template_id="healthcare_claims_outcomes_v1",
        domain="healthcare",
        title="Healthcare Claims + Outcomes",
        description=(
            "Claims and encounters profile where diagnosis and age jointly influence cost and "
            "length-of-stay distributions."
        ),
        pattern_notes=[
            "Diagnosis cohort influences age mix and claim severity.",
            "Higher age and chronic diagnoses skew toward higher cost and longer stays.",
            "Comorbidity-style noise is added through diagnosis-dependent cost multipliers.",
        ],
        compliance_frameworks=["HIPAA", "GDPR", "SOC 2", "CCPA"],
    ),
    "regulated_customer360_v1": DomainTemplate(
        template_id="regulated_customer360_v1",
        domain="credit",
        title="Regulated Customer 360",
        description=(
            "Cross-functional identity, account, and transaction template emphasizing PII-safe "
            "data products and compliance-ready masking patterns."
        ),
        pattern_notes=[
            "Identity confidence and account tenure affect risk and event velocity.",
            "Fraud and dispute probabilities move with transaction behavior volatility.",
            "Template is designed for synthetic customer-level analytics under strict controls.",
        ],
        compliance_frameworks=["GDPR", "PCI DSS", "HIPAA", "SOC 2", "CCPA"],
    ),
}

TEMPLATE_ALIASES: dict[str, str] = {
    "healthcare_claims": "healthcare_claims_outcomes_v1",
    "credit_risk": "experian_credit_risk_v1",
}


def resolve_template(domain: str, requested_template_id: str | None) -> DomainTemplate:
    if requested_template_id:
        resolved_id = TEMPLATE_ALIASES.get(requested_template_id, requested_template_id)
        template = TEMPLATES.get(resolved_id)
        if not template:
            raise ValueError(
                f"Unknown template_id='{requested_template_id}'. Available: {', '.join(sorted(TEMPLATES))}"
            )
        if template.domain != domain and template.template_id != "regulated_customer360_v1":
            raise ValueError(f"Template '{requested_template_id}' is not valid for domain='{domain}'")
        return template

    if domain == "credit":
        return TEMPLATES["experian_credit_risk_v1"]
    return TEMPLATES["healthcare_claims_outcomes_v1"]
