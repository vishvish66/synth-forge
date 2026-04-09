# SynthForge MVP

FastAPI backend + Next.js 15 landing page with live demo form connected to the backend.

## 1) Run Backend (FastAPI)
```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8000
```

Backend URLs:
- API docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`
- Generate: `POST http://127.0.0.1:8000/api/v1/generate`

## 2) Run Frontend (Next.js 15 + Tailwind v4)
```bash
cd frontend
copy .env.local.example .env.local
npm install
npm run dev
```

Frontend URL:
- `http://localhost:3000`

`NEXT_PUBLIC_BACKEND_URL` defaults to `http://127.0.0.1:8000` in `.env.local.example`.

## 3) UI Testing Flow
1. Open `http://localhost:3000`
2. Scroll to **Live Demo**
3. Click `Use Credit Example` or `Use Healthcare Example`
4. Set row count and prompt
5. Click **Generate Synthetic Data + Pipeline**
6. Review result tabs:
- Preview Data
- Generated PySpark Code
- Compliance Report
- Cost Estimate
7. Use download actions:
- `Download ZIP (All Tables)` for all generated tables
- `Download <table>.csv` for per-table CSV

Download notes:
- Files are served from in-memory artifacts only (no disk persistence by default).
- Artifacts expire after `ARTIFACT_TTL_MINUTES` (default: 60).

## Quality Gate (Run Before Push)
```bash
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
powershell -ExecutionPolicy Bypass -File .\scripts\quality_gate.ps1
```

### What It Runs
- Backend tests + coverage (`pytest`)
- Frontend production build (`npm run build`)

## Git Setup (Isolate to synth-forge only)
Important: if `git rev-parse --show-toplevel` returns `C:/Users/Admin`, you are inside a parent repo.

Run this from `c:\Users\Admin\Downloads\synth-forge`:
```bash
git init
git config core.hooksPath .githooks
git add .
git commit -m "chore: baseline synthforge backend+frontend with tests and quality gate"
```

Then add your remote:
```bash
git remote add origin <your-repo-url>
git branch -M main
git push -u origin main
```

## Vercel Deployment (Frontend)
1. Push this repo to GitHub.
2. In Vercel: `Add New -> Project -> Import repo`.
3. Set Root Directory to: `frontend`
4. Set Environment Variable:
   - `NEXT_PUBLIC_BACKEND_URL=<your-backend-url>`
5. Deploy.

CLI option:
```bash
cd frontend
npx vercel
```

## Frontend Stack
- Next.js 15 (App Router) + TypeScript
- Tailwind CSS v4
- Radix UI primitives (Tabs + Accordion)
- Framer Motion animations
- Lucide icons
- Markdown and syntax-highlighted code rendering
