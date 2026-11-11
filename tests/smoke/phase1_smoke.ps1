$ErrorActionPreference = "Stop"

$targets = @(
  "http://localhost:7001/health",
  "http://localhost:7002/health",
  "http://localhost:7003/health",
  "http://localhost:7004/health",
  "http://localhost:7010/health",
  "http://localhost:7011/health",
  "http://localhost:7012/health"
)

foreach ($url in $targets) {
  $resp = Invoke-RestMethod -Method Get -Uri $url
  if (-not $resp) {
    throw "Empty response from $url"
  }
  Write-Host "OK $url"
}

$contracts = Invoke-RestMethod -Method Get -Uri "http://localhost:7012/contracts"
Write-Host "Contracts response: $($contracts | ConvertTo-Json -Depth 4)"

$stats = Invoke-RestMethod -Method Get -Uri "http://localhost:7010/stats"
Write-Host "Recorder stats: $($stats | ConvertTo-Json -Depth 4)"

Write-Host "Smoke test passed."

