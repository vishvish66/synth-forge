import type { Metadata } from "next";
import { IBM_Plex_Mono, Space_Grotesk } from "next/font/google";
import type { ReactNode } from "react";

import "./globals.css";

const spaceGrotesk = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-space-grotesk"
});

const plexMono = IBM_Plex_Mono({
  subsets: ["latin"],
  variable: "--font-plex-mono",
  weight: ["400", "500", "600"]
});

export const metadata: Metadata = {
  title: "SynthForge | Synthetic Data + Databricks Pipelines",
  description:
    "Generate high-fidelity synthetic data and production-ready PySpark pipelines for regulated credit and healthcare teams.",
  openGraph: {
    title: "SynthForge | Synthetic Data + Databricks Pipelines",
    description:
      "AI-powered synthetic data generation with compliance-ready artifacts for GDPR, HIPAA, PCI, SOC 2, and CCPA.",
    type: "website",
    url: "https://synthforge.ai"
  },
  keywords: [
    "synthetic data",
    "databricks",
    "pyspark",
    "healthcare data",
    "credit risk",
    "gdpr",
    "hipaa",
    "pci"
  ]
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={`${spaceGrotesk.variable} ${plexMono.variable} font-[var(--font-space-grotesk)]`}>
        {children}
      </body>
    </html>
  );
}
