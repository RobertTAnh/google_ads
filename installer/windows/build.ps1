# Build GoogleAdsMCP.exe (PyInstaller onefile) + release folder for one-click Setup.
# Run from anywhere:
#   powershell -ExecutionPolicy Bypass -File "installer\windows\build.ps1"

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$OutDir = Join-Path $PSScriptRoot "release"
$WorkDir = Join-Path $PSScriptRoot "_build"
$Entry = Join-Path $RepoRoot "run_mcp_stdio.py"

Write-Host "Repo: $RepoRoot"
Write-Host "Entry: $Entry"

if (-not (Test-Path $Entry)) {
  throw "Missing run_mcp_stdio.py"
}

Push-Location $RepoRoot
try {
  Write-Host "Installing build deps (pyinstaller, mcp, httpx)..."
  python -m pip install -q --upgrade pip
  python -m pip install -q "pyinstaller>=6.0" "mcp>=1.2.0" "httpx>=0.27.0"

  if (Test-Path $WorkDir) { Remove-Item -Recurse -Force $WorkDir }
  if (Test-Path $OutDir) { Remove-Item -Recurse -Force $OutDir }
  New-Item -ItemType Directory -Path $OutDir | Out-Null

  Write-Host "Running PyInstaller (onefile)..."
  python -m PyInstaller `
    --noconfirm `
    --clean `
    --onefile `
    --name GoogleAdsMCP `
    --distpath $OutDir `
    --workpath (Join-Path $WorkDir "work") `
    --specpath (Join-Path $WorkDir "spec") `
    --collect-all mcp `
    --hidden-import mcp `
    --hidden-import mcp.server `
    --hidden-import mcp.server.fastmcp `
    --hidden-import httpx `
    --hidden-import anyio `
    --hidden-import pydantic `
    $Entry

  Copy-Item (Join-Path $PSScriptRoot "Setup-Google-Ads-MCP.bat") (Join-Path $OutDir "Setup-Google-Ads-MCP.bat") -Force
  Copy-Item (Join-Path $PSScriptRoot "write_mcp_config.ps1") (Join-Path $OutDir "write_mcp_config.ps1") -Force

  $Readme = @"
Google Ads MCP - one-click (Windows / Gemini)

1. Unzip this folder on the other Windows PC.
2. Keep ALL 3 files together:
   - GoogleAdsMCP.exe
   - Setup-Google-Ads-MCP.bat
   - write_mcp_config.ps1
3. Double-click: Setup-Google-Ads-MCP.bat
4. Restart Gemini / Antigravity.

Setup will:
- Install GoogleAdsMCP.exe to %LOCALAPPDATA%\GoogleAdsMCP\
- Write %USERPROFILE%\.gemini\config\mcp_config.json
  (Railway URL + MCP_API_KEY already included)

No Python needed on the target PC.
"@
  $readmePath = Join-Path $OutDir "README.txt"
  $Readme = $Readme -replace "`r`n", "`n" -replace "`n", "`r`n"
  [System.IO.File]::WriteAllText($readmePath, $Readme, [System.Text.UTF8Encoding]::new($false))

  Write-Host ""
  Write-Host "DONE. Release folder:"
  Write-Host "  $OutDir"
  Write-Host "Files:"
  Get-ChildItem $OutDir | ForEach-Object { Write-Host ("  - " + $_.Name) }
}
finally {
  Pop-Location
}
