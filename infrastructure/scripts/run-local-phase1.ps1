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

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p market-gateway-binance"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p market-gateway-polymarket"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p market-gateway-kalshi"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p market-gateway-deribit"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p recorder"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p normalizer"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cargo run -p metadata-service"

Write-Host "Phase 1 services launched in separate terminals."

