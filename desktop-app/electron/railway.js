const { net } = require("electron");

function normalizeCid(raw) {
  return String(raw || "").replace(/\D/g, "");
}

function buildUrl(baseUrl, pathname, params = {}) {
  const url = new URL(String(baseUrl || "").replace(/\/+$/, "") + pathname);
  for (const [key, value] of Object.entries(params)) {
    if (value == null) continue;
    const text = String(value).trim();
    if (!text) continue;
    url.searchParams.set(key, text);
  }
  return url.toString();
}

function dateFilterParams(dateFilter = {}) {
  const start = String(dateFilter.start_date || "").trim();
  const end = String(dateFilter.end_date || "").trim();
  if (start && end) {
    return { start_date: start, end_date: end };
  }
  return { date_range: String(dateFilter.date_range || "TODAY").trim() || "TODAY" };
}

function requestJson({ url, apiKey, method = "GET", jsonBody = null, timeoutMs = 180000 }) {
  return new Promise((resolve, reject) => {
    const request = net.request({ method, url });
    request.setHeader("Accept", "application/json");
    if (apiKey) request.setHeader("X-MCP-API-Key", apiKey);
    if (jsonBody != null) request.setHeader("Content-Type", "application/json");

    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        request.abort();
      } catch {
        /* ignore */
      }
      reject(new Error("Hết thời gian chờ khi gọi Railway."));
    }, timeoutMs);

    const chunks = [];
    request.on("response", (response) => {
      response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      response.on("end", () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        const body = Buffer.concat(chunks).toString("utf8");
        let json = null;
        try {
          json = body ? JSON.parse(body) : null;
        } catch {
          reject(new Error(`Railway trả dữ liệu không phải JSON (HTTP ${response.statusCode}).`));
          return;
        }
        resolve({ status: response.statusCode, json });
      });
      response.on("error", (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(err);
      });
    });
    request.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });
    if (jsonBody != null) request.write(JSON.stringify(jsonBody));
    request.end();
  });
}

async function mcpGet(settings, pathname, params = {}, { auth = true, method = "GET", jsonBody = null } = {}) {
  const baseUrl = String(settings.baseUrl || "").trim();
  const apiKey = String(settings.apiKey || "").trim();
  if (!baseUrl) {
    throw new Error("Chưa có địa chỉ Railway.");
  }
  if (auth && !apiKey) {
    throw new Error("Chưa có MCP API key. Mở Cài đặt để nhập key.");
  }
  const url = buildUrl(baseUrl, pathname, params);
  const { status, json } = await requestJson({
    url,
    apiKey: auth ? apiKey : "",
    method,
    jsonBody,
  });
  if (json && json.ok === false) {
    const err = new Error(json.error || `Railway báo lỗi (HTTP ${status}).`);
    err.status = status;
    err.payload = json;
    throw err;
  }
  if (status === 401) {
    throw new Error("Sai MCP API key. Kiểm tra lại trong Cài đặt.");
  }
  if (status < 200 || status >= 300) {
    throw new Error(json?.error || `Railway HTTP ${status}.`);
  }
  return json;
}

async function health(settings) {
  return mcpGet(settings, "/mcp/v1/health", {}, { auth: false });
}

async function ping(settings) {
  const result = await health(settings);
  try {
    await mcpGet(settings, "/mcp/v1/resolve_mcc", { customer_id: "0000000000" });
  } catch (err) {
    const msg = String(err.message || "");
    const status = Number(err.status || 0);
    if (status === 401 || /Unauthorized|Sai MCP/i.test(msg)) throw err;
  }
  return result;
}

async function resolveMcc(settings, customerId) {
  return mcpGet(settings, "/mcp/v1/resolve_mcc", { customer_id: customerId });
}

async function customerPerformance(settings, customerId, dateFilter, mccId = "") {
  return mcpGet(settings, "/mcp/v1/customer_performance", {
    customer_id: customerId,
    mcc_id: mccId,
    ...dateFilterParams(dateFilter),
  });
}

async function campaignPerformance(settings, customerId, dateFilter, mccId = "") {
  return mcpGet(settings, "/mcp/v1/campaign_performance", {
    customer_id: customerId,
    mcc_id: mccId,
    ...dateFilterParams(dateFilter),
  });
}

async function adGroupPerformance(settings, customerId, dateFilter, mccId = "") {
  return mcpGet(settings, "/mcp/v1/ad_group_performance", {
    customer_id: customerId,
    mcc_id: mccId,
    ...dateFilterParams(dateFilter),
  });
}

async function keywordPerformance(settings, customerId, dateFilter, mccId = "") {
  return mcpGet(settings, "/mcp/v1/keyword_performance", {
    customer_id: customerId,
    mcc_id: mccId,
    ...dateFilterParams(dateFilter),
  });
}

async function campaignBudgetMetrics(settings, customerId, dateFilter, mccId = "") {
  return mcpGet(settings, "/mcp/v1/campaign_budget_metrics", {
    customer_id: customerId,
    mcc_id: mccId,
    ...dateFilterParams(dateFilter),
  });
}

async function budgetAlerts(settings) {
  return mcpGet(settings, "/mcp/v1/budget_alerts");
}

async function addBudgetAlert(settings, { customerId, mccId = "", label = "" }) {
  return mcpGet(
    settings,
    "/mcp/v1/budget_alerts",
    {},
    {
      method: "POST",
      jsonBody: {
        customer_id: customerId,
        mcc_id: mccId,
        label,
      },
    }
  );
}

async function deleteBudgetAlert(settings, customerId) {
  return mcpGet(settings, "/mcp/v1/budget_alerts", { customer_id: customerId }, { method: "DELETE" });
}

async function listRecommendationAutoDismiss(settings, customerId = "") {
  return mcpGet(settings, "/mcp/v1/recommendation_auto_dismiss", {
    customer_id: customerId,
  });
}

async function setRecommendationAutoDismiss(
  settings,
  { customerId, mccId = "", label = "", active = true } = {}
) {
  return mcpGet(
    settings,
    "/mcp/v1/recommendation_auto_dismiss",
    {},
    {
      method: "POST",
      jsonBody: {
        customer_id: customerId,
        mcc_id: mccId,
        label,
        active: Boolean(active),
      },
    }
  );
}

async function deleteRecommendationAutoDismiss(settings, customerId) {
  return mcpGet(
    settings,
    "/mcp/v1/recommendation_auto_dismiss",
    { customer_id: customerId },
    { method: "DELETE" }
  );
}

async function budgetRunway(settings, customerId, mccId = "") {
  return mcpGet(settings, "/mcp/v1/budget_runway", {
    customer_id: customerId,
    mcc_id: mccId,
  });
}

async function listRecommendations(settings, customerId, mccId = "") {
  return mcpGet(settings, "/mcp/v1/recommendations", {
    customer_id: customerId,
    mcc_id: mccId,
  });
}

async function dismissRecommendations(settings, customerId, { mccId = "", dismissAll = false, resourceNames = [] } = {}) {
  return mcpGet(
    settings,
    "/mcp/v1/dismiss_recommendations",
    {},
    {
      method: "POST",
      jsonBody: {
        customer_id: customerId,
        mcc_id: mccId,
        dismiss_all: Boolean(dismissAll),
        resource_names: resourceNames,
      },
    }
  );
}

module.exports = {
  normalizeCid,
  health,
  ping,
  resolveMcc,
  customerPerformance,
  campaignPerformance,
  adGroupPerformance,
  keywordPerformance,
  campaignBudgetMetrics,
  budgetAlerts,
  addBudgetAlert,
  deleteBudgetAlert,
  budgetRunway,
  listRecommendations,
  dismissRecommendations,
  listRecommendationAutoDismiss,
  setRecommendationAutoDismiss,
  deleteRecommendationAutoDismiss,
};
