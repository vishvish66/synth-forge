"use client";

import * as Accordion from "@radix-ui/react-accordion";
import { motion } from "framer-motion";
import {
  Activity,
  BadgeCheck,
  Database,
  FileCheck2,
  Lock,
  Menu,
  Rocket,
  ShieldCheck,
  Sparkles,
  X
} from "lucide-react";
import { type ReactNode, useState } from "react";

import { LiveDemo } from "@/components/live-demo";

const features = [
  "Referential integrity + statistical fidelity",
  "Domain-specific templates (Experian-style credit, Huron-style healthcare)",
  "One-click PySpark + Delta Lake + Unity Catalog code",
  "Built-in PII masking & compliance artifacts",
  "Kafka streaming templates",
  "Zero real data stored — fully private"
];

const faqs = [
  {
    q: "Does SynthForge store any real user data?",
    a: "No. The generation flow is designed to run in-memory and avoid logging row-level payloads."
  },
  {
    q: "Can I generate multi-table relational schemas?",
    a: "Yes. You can pass multiple related tables with foreign keys, and SynthForge preserves referential integrity."
  },
  {
    q: "Do you support Databricks-ready code?",
    a: "Yes. Every generation includes production-oriented Bronze/Silver/Gold PySpark code templates."
  },
  {
    q: "Which compliance frameworks are covered?",
    a: "SynthForge provides checklists and masking templates for GDPR, HIPAA, PCI DSS, SOC 2, and CCPA."
  },
  {
    q: "Can I add Kafka streaming templates?",
    a: "Yes. You can request producer and consumer templates in the generation request."
  },
  {
    q: "What domains are optimized today?",
    a: "Credit/fintech and healthcare claims are first-class domains in the current MVP."
  }
];

export default function Page() {
  const [open, setOpen] = useState(false);

  return (
    <main className="relative overflow-x-hidden">
      <header className="sticky top-0 z-50 border-b border-slate-800/70 bg-[#0b1019]/80 backdrop-blur-xl">
        <div className="container-x flex h-16 items-center justify-between">
          <a href="#" className="inline-flex items-center gap-2 font-semibold tracking-wide">
            <Sparkles className="h-5 w-5 text-cyan-300" />
            SynthForge
          </a>

          <nav className="hidden items-center gap-8 text-sm text-slate-300 md:flex">
            <a href="#features" className="hover:text-cyan-200">
              Features
            </a>
            <a href="#demo" className="hover:text-cyan-200">
              Demo
            </a>
            <a href="#pricing" className="hover:text-cyan-200">
              Pricing
            </a>
            <a href="#" className="hover:text-cyan-200">
              Docs
            </a>
            <a href="#" className="hover:text-cyan-200">
              Blog
            </a>
          </nav>

          <div className="hidden items-center gap-3 md:flex">
            <button className="rounded-lg px-3 py-2 text-sm text-slate-300 hover:text-white">Log in</button>
            <button className="rounded-lg bg-cyan-400 px-4 py-2 text-sm font-semibold text-black hover:bg-cyan-300">
              Start Free
            </button>
          </div>

          <button className="md:hidden" onClick={() => setOpen((v) => !v)} aria-label="Toggle menu">
            {open ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
          </button>
        </div>
        {open ? (
          <div className="container-x space-y-2 pb-4 md:hidden">
            {["features", "demo", "pricing"].map((item) => (
              <a key={item} href={`#${item}`} onClick={() => setOpen(false)} className="block py-1 text-slate-300">
                {item[0].toUpperCase() + item.slice(1)}
              </a>
            ))}
            <button className="mt-2 w-full rounded-lg bg-cyan-400 px-4 py-2 text-sm font-semibold text-black">
              Start Free
            </button>
          </div>
        ) : null}
      </header>

      <section className="container-x flex min-h-[92vh] flex-col justify-center py-20">
        <motion.p
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="mb-4 inline-flex w-fit items-center gap-2 rounded-full border border-cyan-400/40 bg-cyan-500/10 px-3 py-1 text-xs text-cyan-200"
        >
          AI Synthetic Data + Databricks Pipelines
        </motion.p>
        <motion.h1
          initial={{ opacity: 0, y: 14 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.45, delay: 0.05 }}
          className="max-w-5xl text-4xl font-semibold leading-tight tracking-tight sm:text-6xl"
        >
          Synthetic Data That Actually Behaves Like Real Data
        </motion.h1>
        <motion.p
          initial={{ opacity: 0, y: 14 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.45, delay: 0.1 }}
          className="mt-6 max-w-4xl text-lg text-slate-300"
        >
          Generate millions of high-fidelity rows for credit scoring or healthcare claims plus production-ready
          PySpark + Databricks pipelines. GDPR, HIPAA and PCI compliant by design. Built by a senior Data Engineer who
          shipped regulated pipelines at Experian and Huron Consulting.
        </motion.p>
        <div className="mt-9 flex flex-wrap gap-3">
          <a href="#demo" className="rounded-xl bg-cyan-400 px-5 py-3 font-semibold text-black hover:bg-cyan-300">
            Try Demo Free
          </a>
          <button className="rounded-xl border px-5 py-3 font-semibold text-slate-100 hover:bg-white/5">
            Watch 60s Video
          </button>
        </div>
        <div className="glass neon-ring mt-10 rounded-2xl p-5">
          <p className="mb-2 text-xs uppercase tracking-widest text-cyan-200">Live Pipeline Snapshot</p>
          <pre className="overflow-x-auto rounded-xl border bg-[#0d1320] p-4 font-[var(--font-plex-mono)] text-xs text-slate-200">
            <code>{`# Schema Input -> Synthetic Preview -> PySpark
domain = "credit"
rows = 5_000_000

spark.read.json("/tmp/synthforge/transactions.json") \\
  .write.format("delta").save("/Volumes/main/synthforge/bronze/transactions")

-- Compliance: GDPR / HIPAA / PCI / SOC2 / CCPA artifacts generated`}</code>
          </pre>
        </div>
      </section>

      <section className="border-y border-slate-800/80 bg-slate-900/30 py-4">
        <div className="container-x text-center text-sm text-cyan-100/90">
          Trusted by data teams at fintech & healthtech companies | Ex-Experian | HIPAA/GDPR/PCI ready
        </div>
      </section>

      <section className="container-x py-24">
        <h2 className="section-title">The Problem We Fix</h2>
        <div className="mt-10 grid gap-5 md:grid-cols-3">
          <Card icon={<Lock className="h-5 w-5 text-cyan-300" />} text="Real data is locked behind PII walls" />
          <Card icon={<Database className="h-5 w-5 text-cyan-300" />} text="Generic synthetic data breaks your pipelines" />
          <Card icon={<Rocket className="h-5 w-5 text-cyan-300" />} text="Weeks wasted waiting for compliant test data" />
        </div>
      </section>

      <section className="container-x py-12">
        <h2 className="section-title">How It Works</h2>
        <div className="mt-10 grid gap-5 md:grid-cols-4">
          {[
            "Paste schema or describe tables",
            "Choose domain (Credit or Healthcare) + row count",
            "Click Generate",
            "Get synthetic data + full PySpark/Databricks code + compliance report"
          ].map((step, i) => (
            <motion.div
              key={step}
              initial={{ opacity: 0, y: 14 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.35, delay: i * 0.07 }}
              className="glass rounded-2xl p-5"
            >
              <div className="mb-3 inline-flex h-8 w-8 items-center justify-center rounded-full bg-cyan-400/20 text-sm text-cyan-200">
                {i + 1}
              </div>
              <p className="text-sm text-slate-100">{step}</p>
            </motion.div>
          ))}
        </div>
      </section>

      <LiveDemo />

      <section id="features" className="container-x py-24">
        <h2 className="section-title">Key Features</h2>
        <div className="mt-10 grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {features.map((feature) => (
            <div key={feature} className="glass rounded-2xl p-5">
              <BadgeCheck className="mb-3 h-5 w-5 text-cyan-300" />
              <p className="text-sm text-slate-100">{feature}</p>
            </div>
          ))}
        </div>
      </section>

      <section id="pricing" className="container-x py-24">
        <h2 className="section-title">Pricing</h2>
        <div className="mt-10 grid gap-5 lg:grid-cols-3">
          <PricingCard tier="Free" price="$0" note="50k rows/month" />
          <PricingCard tier="Pro" price="$49/month" note="5M rows + full exports" featured />
          <PricingCard tier="Team" price="$199/month" note="Unlimited + custom templates + priority" />
        </div>
      </section>

      <section className="container-x py-20">
        <h2 className="section-title">Trusted By Builders</h2>
        <div className="mt-10 grid gap-5 md:grid-cols-2 lg:grid-cols-4">
          {[
            "“We cut synthetic test-data prep from two weeks to one afternoon.” — Senior DE at Fintech Startup",
            "“The generated Spark code plugged directly into our Databricks medallion stack.” — Analytics Lead, Healthtech",
            "“Finally synthetic data that keeps realistic fraud behavior.” — Risk Platform Engineer",
            "“Compliance report output saved us audit prep time.” — Head of Data Governance"
          ].map((quote) => (
            <div key={quote} className="glass rounded-2xl p-5 text-sm text-slate-200">
              {quote}
            </div>
          ))}
        </div>
      </section>

      <section className="container-x py-20">
        <h2 className="section-title">FAQ</h2>
        <Accordion.Root type="single" collapsible className="mt-8 space-y-3">
          {faqs.map((item, i) => (
            <Accordion.Item key={item.q} value={`q-${i}`} className="glass overflow-hidden rounded-xl">
              <Accordion.Header>
                <Accordion.Trigger className="w-full px-4 py-3 text-left text-sm font-medium hover:bg-white/5">
                  {item.q}
                </Accordion.Trigger>
              </Accordion.Header>
              <Accordion.Content className="px-4 pb-4 text-sm text-slate-300">{item.a}</Accordion.Content>
            </Accordion.Item>
          ))}
        </Accordion.Root>
      </section>

      <section className="container-x pb-20">
        <div className="glass neon-ring rounded-2xl p-8 text-center">
          <h2 className="text-3xl font-semibold">Ready to stop fighting with synthetic data?</h2>
          <a
            href="#demo"
            className="mt-6 inline-flex rounded-xl bg-cyan-400 px-6 py-3 font-semibold text-black hover:bg-cyan-300"
          >
            Start Free Trial
          </a>
        </div>
      </section>

      <footer className="border-t border-slate-800 py-8">
        <div className="container-x flex flex-col items-center justify-between gap-3 text-sm text-slate-400 md:flex-row">
          <p>© {new Date().getFullYear()} SynthForge. All rights reserved.</p>
          <div className="flex items-center gap-4">
            <a href="#">Docs</a>
            <a href="#">Privacy</a>
            <a href="#">Terms</a>
          </div>
          <p className="inline-flex items-center gap-1">
            Built with <Activity className="h-4 w-4 text-cyan-300" /> by a Data Engineer turning weekends into products
          </p>
        </div>
      </footer>
    </main>
  );
}

function Card({ icon, text }: { icon: ReactNode; text: string }) {
  return (
    <div className="glass rounded-2xl p-5">
      <div className="mb-3">{icon}</div>
      <p className="text-sm">{text}</p>
    </div>
  );
}

function PricingCard({
  tier,
  price,
  note,
  featured
}: {
  tier: string;
  price: string;
  note: string;
  featured?: boolean;
}) {
  return (
    <div className={`glass relative rounded-2xl p-6 ${featured ? "neon-ring" : ""}`}>
      {featured ? (
        <span className="absolute -top-3 left-5 rounded-full bg-cyan-300 px-3 py-1 text-xs font-semibold text-black">
          Most popular
        </span>
      ) : null}
      <p className="text-sm text-cyan-200">{tier}</p>
      <p className="mt-3 text-3xl font-semibold">{price}</p>
      <p className="mt-2 text-sm text-slate-300">{note}</p>
      <button className="mt-6 w-full rounded-xl border border-cyan-400/40 bg-cyan-500/10 px-4 py-2 text-sm font-semibold hover:bg-cyan-500/20">
        Start Free Trial
      </button>
      <div className="mt-4 space-y-2 text-xs text-slate-400">
        <p className="inline-flex items-center gap-2">
          <FileCheck2 className="h-4 w-4 text-cyan-300" />
          Compliance artifacts included
        </p>
        <p className="inline-flex items-center gap-2">
          <ShieldCheck className="h-4 w-4 text-cyan-300" />
          Private by default
        </p>
      </div>
    </div>
  );
}
