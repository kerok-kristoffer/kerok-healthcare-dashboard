Param(
  [string]$EnvFilePath = ".env"
)

$ErrorActionPreference = "Stop"

# Load .env if present
if (Test-Path $EnvFilePath) {
  Get-Content $EnvFilePath | ForEach-Object {
    if ($_ -match '^\s*#') { return }
    if ($_ -match '^\s*$') { return }
    $name, $value = $_ -split '=', 2
    $name  = $name.Trim()
    $value = $value.Trim()
    [System.Environment]::SetEnvironmentVariable($name, $value)
    Set-Item -Path Env:$name -Value $value | Out-Null
  }
}

# Create venv if missing
if (-not (Test-Path ".venv")) {
  py -3.11 -m venv .venv
}

# Activate venv
& ".\.venv\Scripts\Activate.ps1"

python -m pip install -r requirements.txt
streamlit run app.py
