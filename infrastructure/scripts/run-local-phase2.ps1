$ErrorActionPreference = "Stop"

if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    if ($_ -match "^\s*#") { return }
    if ($_ -match "^\s*$") { return }
    $pair = $_.Split("=", 2)
    if ($pair.Length -eq 2) {
      [System.Environment]::SetEnvironmentVariable($pair[0], $pair[1])
    }
  }
}

$env:MOCK_STREAMS = if ($env:MOCK_STREAMS) { $env:MOCK_STREAMS } else { "true" }

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p fair-value-engine"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p signal-engine"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p paper-execution"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p analytics-engine"

Write-Host "Phase 2 services launched in separate terminals."

