# Chuyển 1 database Postgres (Railway) sang Neon bằng Docker, không cần cài pg_dump.
# Dùng:
#   .\scripts\migrate_pg_to_neon.ps1 -SourceUrl "<DATABASE_PUBLIC_URL của Railway>" -TargetUrl "<Neon direct URL, không phải -pooler>"
# Yêu cầu: Docker Desktop đang chạy, service Postgres trên Railway đang chạy (không bị pause).
param(
    [Parameter(Mandatory = $true)][string]$SourceUrl,
    [Parameter(Mandatory = $true)][string]$TargetUrl,
    [string]$Image = "postgres:18"
)

$ErrorActionPreference = "Stop"

if ($TargetUrl -match "-pooler\.") {
    throw "TargetUrl đang là pooler. Dùng direct connection string (app dùng advisory lock theo session)."
}

docker version --format "{{.Server.Version}}" | Out-Null

$dumpDir = Join-Path $env:TEMP "pg-neon-migrate"
New-Item -ItemType Directory -Force $dumpDir | Out-Null
$dumpFile = "dump_{0}.sql" -f (Get-Date -Format "yyyyMMdd_HHmmss")

Write-Host "1/3 Dump từ Railway..."
docker run --rm -e SRC="$SourceUrl" -v "${dumpDir}:/dump" $Image `
    sh -c "pg_dump --no-owner --no-acl --format=plain `"`$SRC`" > /dump/$dumpFile"
if ($LASTEXITCODE -ne 0) { throw "pg_dump thất bại." }

Write-Host "2/3 Restore vào Neon..."
docker run --rm -e DST="$TargetUrl" -v "${dumpDir}:/dump" $Image `
    sh -c "psql `"`$DST`" -v ON_ERROR_STOP=1 --single-transaction -f /dump/$dumpFile"
if ($LASTEXITCODE -ne 0) { throw "Restore thất bại (dump vẫn giữ ở $dumpDir\$dumpFile)." }

Write-Host "3/3 So số dòng từng bảng (nguồn vs đích)..."
$countSql = "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY relname;"
foreach ($pair in @(@("Railway", $SourceUrl), @("Neon", $TargetUrl))) {
    Write-Host "--- $($pair[0])"
    docker run --rm -e U="$($pair[1])" $Image sh -c "psql `"`$U`" -c 'ANALYZE;' -At >/dev/null && psql `"`$U`" -At -F ' | ' -c `"$countSql`""
}

Write-Host "Xong. File dump: $dumpDir\$dumpFile (xóa sau khi kiểm tra vì chứa dữ liệu thật)."
