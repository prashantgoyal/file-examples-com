# PowerShell helper to run schema inference over CSVs and save DDLs to ./ddls
param(
    [string[]]$Files = $(Get-ChildItem -Path . -Filter *.csv -Recurse | Select-Object -ExpandProperty FullName)
)

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "Python not found in PATH. Please install Python and ensure `python` is on PATH." -ForegroundColor Yellow
    exit 1
}

# create virtual deps if needed
python - <<'PY'
import sys
import subprocess
try:
    import pandas
    import dateutil
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "python-dateutil"]) 
PY

foreach ($f in $Files) {
    Write-Host "Processing $f"
    python infer_schema.py "${f}"
}

Write-Host "DDLs written to ./ddls" -ForegroundColor Green
