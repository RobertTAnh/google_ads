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

- **`date_range`** (GAQL, chung cho các route metrics): `YESTERDAY` | `LAST_7_DAYS` | `LAST_14_DAYS` | `LAST_30_DAYS`. Mặc định `YESTERDAY` nếu bỏ qua. Giá trị hợp lệ xem thêm trong `GET /mcp/v1/health` → `allowed_date_ranges`.
- **`cpa`** trong JSON: `cost / conversions` khi có conversion; không có conversion thì `null`.

| Mục đích | Method | Path | Query |
|----------|--------|------|--------|
| Tài khoản con dưới MCC | GET | `/mcp/v1/child_accounts` | `mcc_id?` |
| Danh sách chiến dịch (metadata) | GET | `/mcp/v1/list_campaigns` | `customer_id`, `mcc_id?` |
| Metrics theo campaign (gộp kỳ + CPA) | GET | `/mcp/v1/campaign_performance` | `customer_id`, `mcc_id?`, `date_range?` |
| Metrics cấp tài khoản (gộp kỳ + CPA) | GET | `/mcp/v1/customer_performance` | `customer_id`, `mcc_id?`, `date_range?` |
| Top keyword theo cost (gộp kỳ) | GET | `/mcp/v1/keyword_performance` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 500) |
| Top cụm từ tìm kiếm thực tế (gộp kỳ) | GET | `/mcp/v1/search_term_performance` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 400) |
| Campaign + ngân sách ngày + metrics kỳ + CPA | GET | `/mcp/v1/campaign_budget_metrics` | `customer_id`, `mcc_id?`, `date_range?` |
| Quảng cáo (ad) + metrics kỳ | GET | `/mcp/v1/ad_performance` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 200) |
| Từ khóa phủ định (snapshot) | GET | `/mcp/v1/negative_keywords` | `customer_id`, `mcc_id?` (`date_range` không dùng) |
| Nhóm quảng cáo + metrics kỳ | GET | `/mcp/v1/ad_group_performance` | `customer_id`, `mcc_id?`, `date_range?` |
| Quality score lịch sử (keyword) | GET | `/mcp/v1/keyword_quality_score` | `customer_id`, `mcc_id?`, `date_range?` |
| Đối tượng (audience) + metrics kỳ | GET | `/mcp/v1/audience_performance` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 300) |
| Asset (asset group) + metrics kỳ | GET | `/mcp/v1/asset_performance` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 300) |
| Lịch sử thay đổi (change_event) | GET | `/mcp/v1/change_history` | `customer_id`, `mcc_id?`, `date_range?`, `limit?` (mặc định 500, tối đa 10000) |
| Tra MCC theo CID (chỉ map DB / ?mcc_id=) | GET | `/mcp/v1/resolve_mcc` | `customer_id`, `mcc_id?` |

`customer_id` / `mcc_id`: **10 chữ số** (có thể gõ dạng `123-456-7890`).

### Map CID → MCC (tự chọn MCC khi gọi MCP)

Khi server có **`DATABASE_URL`**, bảng `customer_mcc_map` lưu **CID tài khoản con → MCC**. Với mọi route MCP có `customer_id`, nếu **không** truyền `mcc_id` thì server sẽ:

1. Tra map trong DB  
2. Nếu không có → dùng **MCC mặc định** từ cấu hình (env / yaml)

Quản lý map: đăng nhập web → **Map CID → MCC** (`/cid-mcc-map`).

Khi server có `DATABASE_URL`, một luồng nền **`cid-mcc-sync`** (mặc định **mỗi 3600 giây**) lấy khóa Postgres advisory, rồi với **mỗi MCC** trong `GOOGLE_ADS_MCC_CONFIGS` (hoặc MCC mặc định từ yaml) gọi Google Ads API liệt kê tài khoản con: **chỉ upsert các CID đang bật**; sau đó **xóa** mọi dòng `customer_mcc_map` cùng `mcc_id` mà không còn trong lần quét (CID hết bật / biến mất khỏi cây MCC). **Label** tay trên web được giữ khi upsert. Biến môi trường:

- `CID_SYNC_ENABLED` — mặc định bật; đặt `0` / `false` / `off` để tắt.
- `CID_SYNC_INTERVAL_SECONDS` — mặc định `3600` (tối thiểu 60).

API kiểm tra (không dùng MCC mặc định): `GET /mcp/v1/resolve_mcc?customer_id=...` — tool MCP **`ads_resolve_mcc`**.

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

Khởi động lại Claude Desktop. Trong chat, thử nhờ Claude dùng **`ads_list_child_accounts`**, **`ads_campaign_performance`** (`date_range` = `LAST_7_DAYS` hoặc `LAST_30_DAYS`), **`ads_search_term_performance`**, **`ads_campaign_budget_metrics`** với `customer_id` thật.

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
