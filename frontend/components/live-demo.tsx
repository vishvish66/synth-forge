"use client";

import * as Tabs from "@radix-ui/react-tabs";
import { motion } from "framer-motion";
import { Loader2, Play, Sparkles } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import Markdown from "react-markdown";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

import { GenerateResponse, absoluteBackendUrl, generateSyntheticData } from "@/lib/api";

const creditSchema = {
  tables: [
    {
      name: "customers",
      primary_key: "id",
      fields: [
        { name: "id", type: "integer", nullable: false },
        { name: "full_name", type: "string", pii: true },
        { name: "email", type: "string", pii: true },
        { name: "credit_score", type: "integer" },
        { name: "utilization_ratio", type: "float" }
      ]
    },
    {
      name: "transactions",
      primary_key: "id",
      fields: [
        { name: "id", type: "integer", nullable: false },
        { name: "customer_id", type: "integer" },
        { name: "mcc", type: "string" },
        { name: "event_hour", type: "integer" },
        { name: "amount", type: "float" },
        { name: "fraud_probability", type: "float" },
        { name: "is_fraud", type: "boolean" },
        { name: "event_date", type: "date" }
      ]
    }
  ]
};

const healthcareSchema = {
  tables: [
    {
      name: "patients",
      primary_key: "id",
      fields: [
        { name: "id", type: "integer", nullable: false },
        { name: "patient_name", type: "string", pii: true },
        { name: "email", type: "string", pii: true },
        { name: "age", type: "integer" }
      ]
    },
    {
      name: "claims",
      primary_key: "id",
      fields: [
        { name: "id", type: "integer", nullable: false },
        { name: "patient_id", type: "integer" },
        { name: "diagnosis_code", type: "string" },
        { name: "length_of_stay", type: "integer" },
        { name: "cost", type: "float" },
        { name: "service_date", type: "date" }
      ]
    }
  ]
};

export function LiveDemo() {
  const [domain, setDomain] = useState<"credit" | "healthcare">("credit");
  const [rowCount, setRowCount] = useState<number>(50000);
  const [prompt, setPrompt] = useState(
    "Credit card transactions, preserve fraud patterns and amount distributions, GDPR + PCI compliant."
  );
  const [schemaText, setSchemaText] = useState<string>(JSON.stringify(creditSchema, null, 2));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GenerateResponse | null>(null);
  const [etaSeconds, setEtaSeconds] = useState<number>(0);
  const [countdownSeconds, setCountdownSeconds] = useState<number>(0);
  const startTimeRef = useRef<number | null>(null);
  const throughputRef = useRef<number>(domain === "healthcare" ? 2600 : 3500); // rows/sec initial baseline

  const table = useMemo(() => result?.tables?.[0], [result]);
  const columns = useMemo(() => (table?.sample_rows?.[0] ? Object.keys(table.sample_rows[0]) : []), [table]);

  const templateId = domain === "credit" ? "experian_credit_risk_v1" : "healthcare_claims_outcomes_v1";
  const tableNames = useMemo(() => result?.tables.map((t) => t.name) ?? [], [result]);
  const etaLabel = useMemo(() => formatDuration(loading ? countdownSeconds : etaSeconds), [loading, countdownSeconds, etaSeconds]);

  useEffect(() => {
    const estimate = estimateDurationSeconds(rowCount, domain, throughputRef.current);
    setEtaSeconds(estimate);
    if (!loading) {
      setCountdownSeconds(estimate);
    }
  }, [rowCount, domain, loading]);

  useEffect(() => {
    if (!loading) return;
    const timer = window.setInterval(() => {
      setCountdownSeconds((prev) => Math.max(0, prev - 1));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [loading]);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const estimate = estimateDurationSeconds(rowCount, domain, throughputRef.current);
    setEtaSeconds(estimate);
    setCountdownSeconds(estimate);
    setLoading(true);
    setError(null);
    startTimeRef.current = Date.now();

    try {
      const parsed = JSON.parse(schemaText) as Record<string, unknown>;
      const response = await generateSyntheticData({
        schema_json: parsed,
        prompt,
        row_count: rowCount,
        domain,
        template_id: templateId,
        include_kafka_templates: true
      });
      setResult(response);
      if (startTimeRef.current) {
        const elapsedSec = Math.max(1, Math.round((Date.now() - startTimeRef.current) / 1000));
        const observedThroughput = rowCount / elapsedSec;
        throughputRef.current = throughputRef.current * 0.65 + observedThroughput * 0.35;
        setEtaSeconds(estimateDurationSeconds(rowCount, domain, throughputRef.current));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate synthetic data");
    } finally {
      setLoading(false);
      startTimeRef.current = null;
    }
  }

  return (
    <section id="demo" className="container-x py-24">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.45 }}
      >
        <h2 className="section-title">Live Demo</h2>
        <p className="section-copy mt-3 max-w-3xl text-lg">
          Test the full generation flow now. This form hits your running FastAPI backend and returns preview data, code,
          compliance report, and cost estimate.
        </p>
      </motion.div>

      <div className="mt-8 grid gap-8 lg:grid-cols-[1fr_1.2fr]">
        <form onSubmit={handleSubmit} className="glass neon-ring rounded-2xl p-6">
          <label className="mb-2 block text-sm font-medium text-cyan-200">Domain</label>
          <select
            value={domain}
            onChange={(e) => {
              const next = e.target.value as "credit" | "healthcare";
              setDomain(next);
              setPrompt(
                next === "credit"
                  ? "Credit card transactions, preserve fraud patterns and amount distributions, GDPR + PCI compliant."
                  : "Medical claims with diagnosis and age correlation, HIPAA + GDPR compliant."
              );
            }}
            className="mb-4 w-full rounded-xl border bg-[#0f1625] p-3 text-sm"
          >
            <option value="credit">Credit Card Transactions</option>
            <option value="healthcare">Medical Claims</option>
          </select>

          <div className="mb-4 flex items-center justify-between">
            <label className="block text-sm font-medium text-cyan-200">Schema JSON</label>
            <div className="flex gap-2">
              <button
                type="button"
                className="rounded-lg border px-3 py-1.5 text-xs hover:bg-cyan-500/10"
                onClick={() => {
                  setDomain("credit");
                  setSchemaText(JSON.stringify(creditSchema, null, 2));
                }}
              >
                Use Credit Example
              </button>
              <button
                type="button"
                className="rounded-lg border px-3 py-1.5 text-xs hover:bg-cyan-500/10"
                onClick={() => {
                  setDomain("healthcare");
                  setSchemaText(JSON.stringify(healthcareSchema, null, 2));
                }}
              >
                Use Healthcare Example
              </button>
            </div>
          </div>
          <textarea
            value={schemaText}
            onChange={(e) => setSchemaText(e.target.value)}
            className="mb-4 h-56 w-full rounded-xl border bg-[#0f1625] p-3 font-[var(--font-plex-mono)] text-xs"
          />

          <label className="mb-2 block text-sm font-medium text-cyan-200">Row Count</label>
          <input
            type="number"
            min={1}
            value={rowCount}
            onChange={(e) => setRowCount(Number(e.target.value))}
            className="mb-4 w-full rounded-xl border bg-[#0f1625] p-3 text-sm"
          />

          <label className="mb-2 block text-sm font-medium text-cyan-200">Prompt (Optional)</label>
          <textarea
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            className="mb-6 h-24 w-full rounded-xl border bg-[#0f1625] p-3 text-sm"
          />

          <div className="flex items-center gap-3">
            <button
              type="submit"
              disabled={loading}
              className="inline-flex flex-1 items-center justify-center gap-2 rounded-xl bg-cyan-400 px-4 py-3 text-sm font-semibold text-black hover:bg-cyan-300 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
              Generate Synthetic Data + Pipeline
            </button>
            <div className="min-w-[92px] rounded-xl border border-cyan-400/40 bg-cyan-500/10 px-3 py-2 text-center">
              <p className="text-[10px] uppercase tracking-wide text-cyan-200">{loading ? "Time Left" : "ETA"}</p>
              <p className="text-sm font-semibold text-cyan-100">{etaLabel}</p>
            </div>
          </div>

          {error ? <p className="mt-3 text-sm text-rose-300">{error}</p> : null}
        </form>

        <div className="glass rounded-2xl p-6">
          {!result ? (
            <div className="flex h-full min-h-[420px] flex-col items-center justify-center rounded-xl border border-dashed border-cyan-500/40 bg-cyan-500/5 text-center">
              <Play className="mb-3 h-8 w-8 text-cyan-300" />
              <p className="text-lg font-medium">Run the demo to view generated outputs</p>
              <p className="mt-1 text-sm text-slate-400">Preview data, PySpark code, compliance report, and cost.</p>
            </div>
          ) : (
            <Tabs.Root defaultValue="preview">
              <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
                <p className="text-xs text-slate-400">
                  Request ID: <span className="font-[var(--font-plex-mono)] text-slate-200">{result.request_id}</span>
                </p>
                <div className="flex flex-wrap gap-2">
                  <a
                    href={absoluteBackendUrl(result.download_endpoints?.zip || `/api/v1/downloads/${result.request_id}/zip`)}
                    className="rounded-lg border border-cyan-400/50 bg-cyan-500/10 px-3 py-1.5 text-xs font-medium text-cyan-200 hover:bg-cyan-500/20"
                  >
                    Download ZIP (All Tables)
                  </a>
                  {tableNames.map((name) => (
                    <div key={name} className="flex gap-2">
                      <a
                        href={absoluteBackendUrl(
                          result.download_endpoints?.csv_by_table?.[name] ||
                            `/api/v1/downloads/${result.request_id}/csv/${name}`
                        )}
                        className="rounded-lg border px-3 py-1.5 text-xs hover:bg-white/5"
                      >
                        {name}.csv
                      </a>
                      <a
                        href={absoluteBackendUrl(
                          result.download_endpoints?.parquet_by_table?.[name] ||
                            `/api/v1/downloads/${result.request_id}/parquet/${name}`
                        )}
                        className="rounded-lg border border-cyan-500/40 px-3 py-1.5 text-xs text-cyan-200 hover:bg-cyan-500/10"
                      >
                        {name}.parquet
                      </a>
                    </div>
                  ))}
                </div>
              </div>
              <Tabs.List className="mb-4 flex flex-wrap gap-2">
                {["preview", "pyspark", "compliance", "cost", "quality"].map((tab) => (
                  <Tabs.Trigger
                    key={tab}
                    value={tab}
                    className="rounded-lg border px-3 py-1.5 text-xs uppercase tracking-wide data-[state=active]:bg-cyan-400 data-[state=active]:text-black"
                  >
                    {tab}
                  </Tabs.Trigger>
                ))}
              </Tabs.List>

              <Tabs.Content value="preview">
                <div className="overflow-x-auto rounded-xl border">
                  <table className="min-w-full text-left text-xs">
                    <thead className="bg-slate-900/70">
                      <tr>
                        {columns.map((c) => (
                          <th key={c} className="px-3 py-2 font-medium text-cyan-200">
                            {c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(table?.sample_rows || []).slice(0, 10).map((row, idx) => (
                        <tr key={idx} className="border-t border-slate-800">
                          {columns.map((c) => (
                            <td key={`${idx}-${c}`} className="max-w-[200px] truncate px-3 py-2 text-slate-300">
                              {String(row[c] ?? "")}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Tabs.Content>

              <Tabs.Content value="pyspark">
                <SyntaxHighlighter
                  language="python"
                  style={oneDark}
                  customStyle={{ margin: 0, borderRadius: 12, maxHeight: 480, fontSize: "0.76rem" }}
                >
                  {result.pyspark_pipeline_code}
                </SyntaxHighlighter>
              </Tabs.Content>

              <Tabs.Content value="compliance">
                <article className="prose prose-invert max-w-none text-sm prose-headings:text-cyan-100">
                  <Markdown>{result.compliance_markdown}</Markdown>
                </article>
              </Tabs.Content>

              <Tabs.Content value="cost">
                <div className="grid gap-4 sm:grid-cols-2">
                  <Metric label="Template" value={result.template_id} />
                  <Metric label="Workers" value={String(result.cost_estimate.workers)} />
                  <Metric label="Runtime (hrs)" value={String(result.cost_estimate.runtime_hours)} />
                  <Metric label="Estimated USD" value={`$${result.cost_estimate.total_estimated_cost_usd}`} />
                </div>
              </Tabs.Content>

              <Tabs.Content value="quality">
                <div className="space-y-4 text-xs">
                  <div className="rounded-xl border bg-[#0f1625] p-3">
                    <p className="mb-2 text-cyan-200">Validation Metrics</p>
                    <pre className="overflow-x-auto whitespace-pre-wrap text-slate-300">
                      {JSON.stringify(result.validation_metrics ?? {}, null, 2)}
                    </pre>
                  </div>
                  <div className="rounded-xl border bg-[#0f1625] p-3">
                    <p className="mb-2 text-cyan-200">Data Quality Report</p>
                    <pre className="overflow-x-auto whitespace-pre-wrap text-slate-300">
                      {JSON.stringify(result.data_quality_report ?? {}, null, 2)}
                    </pre>
                  </div>
                </div>
              </Tabs.Content>
            </Tabs.Root>
          )}
        </div>
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border bg-[#0f1625] p-4">
      <p className="text-xs uppercase tracking-wide text-slate-400">{label}</p>
      <p className="mt-1 text-lg font-semibold">{value}</p>
    </div>
  );
}

function estimateDurationSeconds(rowCount: number, domain: "credit" | "healthcare", throughputRowsPerSec: number): number {
  const domainMultiplier = domain === "healthcare" ? 1.25 : 1.0;
  const base = Math.ceil((rowCount / Math.max(1000, throughputRowsPerSec)) * domainMultiplier);
  return Math.max(4, Math.min(180, base));
}

function formatDuration(totalSeconds: number): string {
  const s = Math.max(0, totalSeconds);
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return `${String(m).padStart(2, "0")}:${String(rem).padStart(2, "0")}`;
}
