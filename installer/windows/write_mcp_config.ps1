# Writes Gemini mcp_config.json for Google Ads MCP one-click setup.
# Called by Setup-Google-Ads-MCP.bat

$ErrorActionPreference = "Stop"

$exe = Join-Path $env:LOCALAPPDATA "GoogleAdsMCP\GoogleAdsMCP.exe"
$cfgDir = Join-Path $env:USERPROFILE ".gemini\config"
$cfgPath = Join-Path $cfgDir "mcp_config.json"

$base = "https://web-production-f8738.up.railway.app"
$key = "y9dDbZOIvG71zYRAGRen8y2ZslZgTuD_1_N022CG5lNlqNR0TV1RoAvPehQHa3Wi"

if (-not (Test-Path -LiteralPath $cfgDir)) {
  New-Item -ItemType Directory -Path $cfgDir -Force | Out-Null
}

$prefsDefault = [ordered]@{
  coworkWebSearchEnabled     = $true
  coworkScheduledTasksEnabled = $true
  ccdScheduledTasksEnabled   = $true
}

$cfg = $null
if (Test-Path -LiteralPath $cfgPath) {
  try {
    $cfg = Get-Content -LiteralPath $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
  } catch {
    $cfg = $null
  }
}

if ($null -eq $cfg) {
  $cfg = [pscustomobject]@{
    mcpServers  = [pscustomobject]@{}
    preferences = [pscustomobject]$prefsDefault
  }
}

if (-not $cfg.PSObject.Properties["mcpServers"] -or $null -eq $cfg.mcpServers) {
  $cfg | Add-Member -NotePropertyName mcpServers -NotePropertyValue ([pscustomobject]@{}) -Force
}
if (-not $cfg.PSObject.Properties["preferences"] -or $null -eq $cfg.preferences) {
  $cfg | Add-Member -NotePropertyName preferences -NotePropertyValue ([pscustomobject]$prefsDefault) -Force
}

$entry = [pscustomobject]@{
  command = $exe
  args    = @()
  env     = [pscustomobject]@{
    GOOGLE_ADS_MCP_BASE_URL = $base
    MCP_API_KEY             = $key
  }
}

$cfg.mcpServers | Add-Member -NotePropertyName "google-ads-mcp" -NotePropertyValue $entry -Force

$json = $cfg | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($cfgPath, $json, [System.Text.UTF8Encoding]::new($false))
Write-Host "[OK] Wrote: $cfgPath"
