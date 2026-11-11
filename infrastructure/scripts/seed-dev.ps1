param(
  [string]$PostgresHost = "localhost",
  [int]$PostgresPort = 5432,
  [string]$PostgresDb = "foundation",
  [string]$PostgresUser = "foundation",
  [string]$PostgresPassword = "foundation",
  [string]$ClickhouseHost = "localhost",
  [int]$ClickhousePort = 8123
)

$env:PGPASSWORD = $PostgresPassword
psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d $PostgresDb -f "infrastructure/seed/postgres_seed.sql"

$clickhouseSeed = Get-Content "infrastructure/seed/clickhouse_seed.sql" -Raw
Invoke-RestMethod -Method Post -Uri "http://${ClickhouseHost}:${ClickhousePort}/?query=$( [uri]::EscapeDataString($clickhouseSeed) )"

Write-Host "Seed completed"

