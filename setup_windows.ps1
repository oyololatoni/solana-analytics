# setup_windows.ps1
# Solana Analytics Windows Setup Script

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "   Solana Analytics Windows Setup" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Check for Python
if (!(Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] Python was not found in your PATH." -ForegroundColor Red
    Write-Host "Please install Python 3.10+ from https://www.python.org/ and ensure 'Add Python to PATH' is checked."
    exit 1
}

$pythonVersion = python --version
Write-Host "[INFO] Found $pythonVersion" -ForegroundColor Gray

# 2. Create Virtual Environment
if (!(Test-Path "venv")) {
    Write-Host "[INFO] Creating Virtual Environment..." -ForegroundColor Yellow
    python -m venv venv
} else {
    Write-Host "[INFO] Virtual Environment already exists. Skipping creation." -ForegroundColor Gray
}

# 3. Install/Upgrade Dependencies
Write-Host "[INFO] Installing dependencies..." -ForegroundColor Yellow
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\pip.exe install -r requirements.txt

# 4. Environment Variable Helper
Write-Host "[INFO] Generating Windows Runner Scripts..." -ForegroundColor Yellow

$envLoader = @'
# Helper to load .env.local variables into session
if (Test-Path ".env.local") {
    Get-Content .env.local | Foreach-Object {
        if ($_ -match "^(?<name>[^#\s][^=]*)=(?<value>.*)$") {
            $name = $Matches.name.Trim()
            $value = $Matches.value.Trim().Trim("'").Trim('"')
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}
'@

# Generate run-server.ps1
$serverScript = @"
$envLoader
Write-Host "Starting API Server..." -ForegroundColor Green
.\venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
"@
$serverScript | Out-File -FilePath run-server.ps1 -Encoding utf8

# Generate run-worker.ps1
$workerScript = @"
$envLoader
Write-Host "Starting Orchestrator Worker..." -ForegroundColor Green
.\venv\Scripts\python.exe app/worker.py
"@
$workerScript | Out-File -FilePath run-worker.ps1 -Encoding utf8

Write-Host "`n==========================================" -ForegroundColor Green
Write-Host "   SETUP COMPLETE!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host "To start the system on Windows:"
Write-Host "1. Run .\run-server.ps1 for the Dashboard API"
Write-Host "2. Run .\run-worker.ps1 for the Data Orchestrator"
Write-Host "`nNOTE: Ensure your .env.local is present in this folder." -ForegroundColor Yellow
