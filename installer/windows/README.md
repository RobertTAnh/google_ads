# Google Ads MCP — Windows one-click (Gemini)

## Build (máy dev, có Python)

```powershell
powershell -ExecutionPolicy Bypass -File "installer\windows\build.ps1"
```

Ra thư mục `installer\windows\release\`:

- `GoogleAdsMCP.exe`
- `Setup-Google-Ads-MCP.bat`
- `README.txt`

Zip cả folder `release` rồi gửi sang máy khác.

## Máy đích (không cần Python)

1. Giải nén **cả thư mục** (giữ đủ 3 file: `.exe`, `.bat`, `write_mcp_config.ps1`)
2. Double-click **`Setup-Google-Ads-MCP.bat`**
3. Mở lại Gemini / Antigravity

Setup ghi:

`%USERPROFILE%\.gemini\config\mcp_config.json`

`command` trỏ tới `%LOCALAPPDATA%\GoogleAdsMCP\GoogleAdsMCP.exe` (không dùng path Python).

> Lưu ý: file `.bat` phải là CRLF. Nếu thấy lỗi kiểu `'et' is not recognized` thì đang dùng bản cũ (LF) — hãy lấy lại zip mới.
