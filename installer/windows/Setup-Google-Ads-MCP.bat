@echo off
setlocal EnableExtensions

REM One-click: copy MCP exe + write .gemini\config\mcp_config.json
REM Must be CRLF + ASCII so cmd.exe parses correctly.

set "INSTALL_DIR=%LOCALAPPDATA%\GoogleAdsMCP"
set "EXE_NAME=GoogleAdsMCP.exe"
set "SRC_EXE=%~dp0GoogleAdsMCP.exe"
set "PS1_WRITE=%~dp0write_mcp_config.ps1"

echo.
echo === Google Ads MCP Setup (Gemini) ===
echo.

if not exist "%SRC_EXE%" (
  echo [ERROR] Missing "%SRC_EXE%"
  echo Put Setup-Google-Ads-MCP.bat next to GoogleAdsMCP.exe
  echo.
  pause
  exit /b 1
)

if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
copy /Y "%SRC_EXE%" "%INSTALL_DIR%\%EXE_NAME%" >nul
if errorlevel 1 (
  echo [ERROR] Cannot copy exe to %INSTALL_DIR%
  pause
  exit /b 1
)
echo [OK] Installed: %INSTALL_DIR%\%EXE_NAME%

if not exist "%PS1_WRITE%" (
  echo [ERROR] Missing "%PS1_WRITE%"
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1_WRITE%"
if errorlevel 1 (
  echo [ERROR] Failed to write mcp_config.json
  pause
  exit /b 1
)

echo.
echo Done. Restart Gemini / Antigravity to load MCP.
echo.
pause
endlocal