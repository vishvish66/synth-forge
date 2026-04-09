# SynthForge Project Summary

## What This Project Is
FastAPI backend + Next.js frontend for SynthForge, a synthetic data generation product for regulated domains.

## Current Backend Focus
- Multi-table healthcare synthetic generation (`patients` + `claims`)
- Referential integrity preserved through FK-aware generation
- Enterprise-style realism and correlations:
  - age -> comorbidity_count
  - age/comorbidity/chronic -> length_of_stay/cost/readmission_risk_score
- Compliance artifacts for GDPR/HIPAA/PCI/SOC2/CCPA
- Download endpoints for CSV/Parquet/ZIP
- Validation outputs (correlation matrix + grouped summaries + data quality report)

## Important Constraints
- Never store real customer PHI/PII payloads
- Keep generation and artifact retention in-memory with TTL
- Prefer surgical blast-radius changes only
