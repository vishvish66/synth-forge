export type Domain = "credit" | "healthcare";

export type GenerateRequest = {
  schema_json: Record<string, unknown>;
  prompt: string;
  row_count: number;
  domain: Domain;
  template_id?: string;
  include_kafka_templates?: boolean;
};

export type GenerateResponse = {
  request_id: string;
  generated_at: string;
  domain: Domain;
  template_id: string;
  tables: {
    name: string;
    row_count: number;
    sample_rows: Record<string, unknown>[];
    basic_stats: Record<string, unknown>;
  }[];
  pyspark_pipeline_code: string;
  compliance_markdown: string;
  cost_estimate: {
    workers: number;
    runtime_hours: number;
    dbu_rate_usd: number;
    vm_hourly_rate_usd: number;
    total_estimated_cost_usd: number;
    assumptions: string[];
  };
  audit_trail: string[];
  validation_metrics?: Record<string, unknown>;
  data_quality_report?: Record<string, unknown>;
  download_endpoints?: {
    zip: string;
    csv_by_table: Record<string, string>;
    parquet_by_table?: Record<string, string>;
    expires_in_minutes: number;
  };
};

const DEFAULT_BACKEND = "http://127.0.0.1:8000";

export function backendBaseUrl(): string {
  return process.env.NEXT_PUBLIC_BACKEND_URL || DEFAULT_BACKEND;
}

export async function generateSyntheticData(payload: GenerateRequest): Promise<GenerateResponse> {
  const response = await fetch(`${backendBaseUrl()}/api/v1/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    let detail = `Request failed with status ${response.status}`;
    try {
      const err = (await response.json()) as { detail?: unknown };
      if (typeof err.detail === "string") detail = err.detail;
      if (Array.isArray(err.detail)) detail = "Validation failed. Check schema JSON and fields.";
    } catch {}
    throw new Error(detail);
  }

  return (await response.json()) as GenerateResponse;
}

export function absoluteBackendUrl(path: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  return `${backendBaseUrl()}${path.startsWith("/") ? path : `/${path}`}`;
}
