$ErrorActionPreference = "Stop"

Write-Host "Running backend quality gate..."
.\.venv\Scripts\python -m pytest --cov=app --cov-report=term-missing

Write-Host "Running frontend build check..."
Push-Location frontend
try {
  npm run build
} finally {
  Pop-Location
}

Write-Host "Quality gate passed."
