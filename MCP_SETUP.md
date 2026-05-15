# Google Ads — HTTP API + MCP (Claude Desktop & Antigravity)

Repo này có **hai lớp**:

1. **HTTP API trên Flask** (`/mcp/v1/...`) — triển khai trên Railway (hoặc máy local). Dùng credential Google Ads như app web (`GOOGLE_ADS_MCC_CONFIGS`, `google-ads.yaml`, …). Bảo vệ bằng **`MCP_API_KEY`**.
2. **MCP stdio (Python)** — thư mục `mcp_server/`. Chạy trên máy bạn; các tool MCP gọi HTTP tới URL ở (1). Claude Desktop / Antigravity chỉ cần cấu hình chạy `python -m mcp_server` với đúng biến môi trường.

Luồng: **Claude / Antigravity → MCP local → HTTPS → Flask `/mcp/v1` → Google Ads API.**

---

## 1. Cấu hình server (Railway / .env)

Thêm biến (chuỗi ngẫu nhiên đủ dài, ví dụ 32+ ký tự):

```env
MCP_API_KEY=thay-bang-secret-cua-ban
```

Deploy lại. Kiểm tra công khai (không cần key):

`GET https://<host-cua-ban>/mcp/v1/health`  
→ `"mcp_data_routes_enabled": true` khi `MCP_API_KEY` đã set.

Các route dữ liệu cần header:

- `X-MCP-API-Key: <MCP_API_KEY>`  
  hoặc  
- `Authorization: Bearer <MCP_API_KEY>`

Query thường dùng:

| Mục đích | Method | Path | Query |
|----------|--------|------|--------|
| Tài khoản con dưới MCC | GET | `/mcp/v1/child_accounts` | `mcc_id` (tuỳ chọn nếu có MCC mặc định) |
| Danh sách chiến dịch | GET | `/mcp/v1/list_campaigns` | `customer_id`, `mcc_id?` |
| Performance theo campaign (hôm qua) | GET | `/mcp/v1/campaign_performance` | `customer_id`, `mcc_id?` |
| Performance cấp tài khoản (hôm qua) | GET | `/mcp/v1/customer_performance` | `customer_id`, `mcc_id?` |
| Performance theo keyword (hôm qua) | GET | `/mcp/v1/keyword_performance` | `customer_id`, `mcc_id?`, `limit?` (mặc định 500) |

`customer_id` / `mcc_id`: **10 chữ số** (có thể gõ dạng `123-456-7890`).

---

## 2. Cài dependency trên máy chạy MCP (local)

Từ thư mục repo:

```bash
pip install -r requirements.txt
```

Cần **Python 3.10+** (khớp với môi trường bạn dùng cho Claude / Antigravity).

---

## 3. Claude Desktop

File cấu hình (Windows):

`%APPDATA%\Claude\claude_desktop_config.json`  

(macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`)

Thêm (hoặc gộp vào `mcpServers`) mục ví dụ — **sửa `cwd` và URL** cho đúng máy bạn:

```json
{
  "mcpServers": {
    "google-ads-mcp": {
      "command": "python",
      "args": ["-m", "mcp_server"],
      "cwd": "D:\\1 Code App\\gg ads API",
      "env": {
        "GOOGLE_ADS_MCP_BASE_URL": "https://your-app.up.railway.app",
        "MCP_API_KEY": "thay-bang-secret-cua-ban"
      }
    }
  }
}
```

Khởi động lại Claude Desktop. Trong chat, thử nhờ Claude dùng tool **`ads_list_child_accounts`** rồi **`ads_campaign_performance_yesterday`** với `customer_id` thật.

---

## 4. Google Antigravity

Antigravity hỗ trợ thêm MCP server qua UI (MCP Store / cấu hình MCP tùy chỉnh — tên menu có thể đổi theo phiên bản). Nguyên tắc giống Claude Desktop:

- **Command:** `python` (hoặc đường dẫn đầy đủ tới `python.exe` nếu cần)
- **Arguments:** `-m`, `mcp_server`
- **Working directory:** thư mục gốc của repo này
- **Environment variables:**
  - `GOOGLE_ADS_MCP_BASE_URL` = URL gốc app (không có `/` cuối)
  - `MCP_API_KEY` = cùng giá trị với server

Sau khi lưu, mở lại phiên / agent và kiểm tra các tool tên `ads_*`.

Nếu Antigravity chỉ liệt kê MCP từ “store” chính thức: bạn vẫn có thể dùng **chỉ HTTP** từ extension/script riêng; còn khi có tùy chọn “Custom MCP” / “Add server”, dùng đúng block trên.

---

## 5. Tuỳ chọn: chỉ dùng HTTP (không cài MCP Python)

Một số client có MCP **`@modelcontextprotocol/server-fetch`** hoặc tool HTTP tương đương — khi đó cấu hình base URL và header `X-MCP-API-Key` rồi gọi trực tiếp các path `/mcp/v1/...`. Cách **`python -m mcp_server`** ổn định hơn vì có **tên tool + mô tả** rõ ràng cho model.

---

## 6. Bảo mật

- Không commit `MCP_API_KEY` lên git; chỉ đặt trên Railway và env local MCP.
- Luôn **HTTPS** cho production.
- Nếu lộ key: đổi `MCP_API_KEY` trên server và cập nhật lại mọi máy cấu hình MCP.
