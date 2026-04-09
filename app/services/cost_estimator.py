from __future__ import annotations

from app.core.config import Settings
from app.models.api import CostEstimate


def estimate_databricks_cost(row_count: int, settings: Settings) -> CostEstimate:
    workers = settings.default_workers
    runtime = settings.default_runtime_hours + (row_count / 5_000_000) * 0.5
    dbu_cost = workers * runtime * settings.databricks_dbu_rate_usd
    vm_cost = workers * runtime * settings.vm_hourly_rate_usd
    total = round(dbu_cost + vm_cost, 2)
    return CostEstimate(
        workers=workers,
        runtime_hours=round(runtime, 2),
        dbu_rate_usd=settings.databricks_dbu_rate_usd,
        vm_hourly_rate_usd=settings.vm_hourly_rate_usd,
        total_estimated_cost_usd=total,
        assumptions=[
            "Assumes all-purpose cluster pricing and 4 workers by default.",
            "Runtime scales linearly with row_count for MVP estimates.",
            "Storage/network costs are excluded from this rough estimate.",
        ],
    )
