@echo off
setlocal
cd /d "%~dp0"

if not exist "node_modules\electron\dist\electron.exe" (
  echo Dang cai Electron, cho mot luc...
  call npm install
  if errorlevel 1 (
    echo Cai dat that bai. Can Node.js: https://nodejs.org
    pause
    exit /b 1
  )
)

start "" "node_modules\electron\dist\electron.exe" .
exit
