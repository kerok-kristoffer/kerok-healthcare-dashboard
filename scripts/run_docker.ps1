Param(
  [string]$EnvFilePath = ".env"
)

$ErrorActionPreference = "Stop"

# Load .env into current shell env for convenience
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

# Path to your AWS creds dir on Windows
$awsDir = Join-Path $env:UserProfile ".aws"

# Run Docker (use --env-file for simplicity)
docker run --rm -p 8501:8501 `
  --env-file $EnvFilePath `
  -e AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN `
  -e AWS_PROFILE `
  -v "${awsDir}:/root/.aws:ro" `
  healthcare-dashboard:latest
