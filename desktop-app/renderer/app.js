const api = window.adsDesk;
const { DatePicker, toApiParams, periodDays } = window.AdsDatePicker;

const els = {
  setup: document.getElementById("setup-screen"),
  app: document.getElementById("app"),
  setupUrl: document.getElementById("setup-url"),
  setupKey: document.getElementById("setup-key"),
  setupAutostart: document.getElementById("setup-autostart"),
  setupSave: document.getElementById("setup-save"),
  setupError: document.getElementById("setup-error"),
  addForm: document.getElementById("add-form"),
  cidInput: document.getElementById("cid-input"),
  addError: document.getElementById("add-error"),
  accountSearch: document.getElementById("account-search"),
  accountList: document.getElementById("account-list"),
  accountName: document.getElementById("account-name"),
  accountCid: document.getElementById("account-cid"),
  emptyMain: document.getElementById("empty-main"),
  overview: document.getElementById("overview"),
  overviewKpis: document.getElementById("overview-kpis"),
  overviewStatus: document.getElementById("overview-status"),
  overviewTable: document.getElementById("overview-table"),
  backOverview: document.getElementById("back-overview"),
  detail: document.getElementById("account-detail"),
  kpiRow: document.getElementById("kpi-row"),
  autoDismiss: document.getElementById("auto-dismiss"),
  dismissStatus: document.getElementById("dismiss-status"),
  tabs: document.getElementById("tabs"),
  tableSearch: document.getElementById("table-search"),
  tableStatus: document.getElementById("table-status"),
  table: document.getElementById("data-table"),
  refreshBtn: document.getElementById("refresh-btn"),
  settingsBtn: document.getElementById("settings-btn"),
  settingsModal: document.getElementById("settings-modal"),
  settingsUrl: document.getElementById("settings-url"),
  settingsKey: document.getElementById("settings-key"),
  settingsAutostart: document.getElementById("settings-autostart"),
  settingsStatus: document.getElementById("settings-status"),
  settingsTest: document.getElementById("settings-test"),
  settingsCancel: document.getElementById("settings-cancel"),
  settingsSave: document.getElementById("settings-save"),
  toast: document.getElementById("toast"),
};

const state = {
  settings: null,
  view: "overview",
  selectedCid: null,
  tab: "campaigns",
  date: null,
  kpis: null,
  rows: [],
  overviewRows: [],
  budgetByCid: {},
  columns: [],
  filter: "",
  sortKey: "cost",
  sortDir: "desc",
  loading: false,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatCid(cid) {
  const digits = String(cid || "").replace(/\D/g, "");
  if (digits.length !== 10) return cid || "";
  return `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
}

function formatMoney(value) {
  const n = Number(value || 0);
  return new Intl.NumberFormat("vi-VN", {
    style: "currency",
    currency: "VND",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatNumber(value, digits = 0) {
  return new Intl.NumberFormat("vi-VN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(Number(value || 0));
}

function formatPct(value) {
  return `${formatNumber((Number(value) || 0) * 100, 2)}%`;
}

function formatDays(value) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${formatNumber(value, 1)} ngày`;
}

function daysTone(value) {
  if (value == null || Number.isNaN(Number(value))) return "";
  if (value < 3) return "danger";
  if (value < 4) return "warn";
  return "ok";
}

function isLowRunway(row) {
  return row?.days_remaining != null && Number(row.days_remaining) < 3;
}

function derived(row) {
  const clicks = Number(row.clicks || 0);
  const impressions = Number(row.impressions || 0);
  const cost = Number(row.cost || 0);
  const conversions = Number(row.conversions || 0);
  const cpa = row.cpa == null && conversions > 0 ? cost / conversions : row.cpa;
  return {
    ...row,
    ctr: impressions ? clicks / impressions : 0,
    cpc: clicks ? cost / clicks : 0,
    cpa,
  };
}

function showToast(message, isError = false) {
  els.toast.textContent = message;
  els.toast.classList.remove("hidden");
  els.toast.style.background = isError ? "#991b1b" : "#0f172a";
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => els.toast.classList.add("hidden"), 3200);
}

function setBusy(message) {
  els.tableStatus.textContent = message || "";
}

function sortedSidebarAccounts(accounts) {
  return [...accounts].sort((a, b) => {
    const aDays = state.budgetByCid[a.customerId]?.days_remaining;
    const bDays = state.budgetByCid[b.customerId]?.days_remaining;
    const aLow = aDays != null && aDays < 3;
    const bLow = bDays != null && bDays < 3;
    if (aLow !== bLow) return aLow ? -1 : 1;
    if (aLow && bLow) return aDays - bDays;
    return String(a.name || a.customerId).localeCompare(String(b.name || b.customerId), "vi");
  });
}

function renderAccounts() {
  const q = els.accountSearch.value.trim().toLowerCase();
  const accounts = sortedSidebarAccounts(state.settings?.accounts || []).filter((row) => {
    const hay = `${row.name} ${row.customerId} ${formatCid(row.customerId)}`.toLowerCase();
    return !q || hay.includes(q);
  });
  els.accountList.innerHTML = "";

  const home = document.createElement("div");
  home.className = `account-item home${state.view === "overview" ? " active" : ""}`;
  home.innerHTML = `<div class="meta"><div class="name">Tất cả tài khoản</div><div class="cid">Trang tổng quan</div></div>`;
  home.addEventListener("click", () => showOverview());
  els.accountList.appendChild(home);

  if (!accounts.length) {
    const empty = document.createElement("div");
    empty.className = "muted";
    empty.style.padding = "10px";
    empty.textContent = "Chưa ghim tài khoản. Nhập CID rồi bấm Thêm.";
    els.accountList.appendChild(empty);
    return;
  }
  for (const row of accounts) {
    const days = state.budgetByCid[row.customerId]?.days_remaining;
    const tone = daysTone(days);
    const item = document.createElement("div");
    item.className = `account-item${row.customerId === state.selectedCid && state.view === "detail" ? " active" : ""}`;
    item.innerHTML = `
      <div class="meta">
        <div class="name">${escapeHtml(row.name || "Chưa có tên")}</div>
        <div class="cid">${escapeHtml(formatCid(row.customerId))}</div>
      </div>
      <div>
        ${days != null ? `<div class="days ${tone}">${escapeHtml(formatDays(days))}</div>` : ""}
        <button class="remove" type="button" title="Xóa">✕</button>
      </div>
    `;
    item.addEventListener("click", () => selectAccount(row.customerId));
    item.querySelector(".remove").addEventListener("click", async (ev) => {
      ev.stopPropagation();
      await unwatchAccount(row.customerId, row.name);
    });
    els.accountList.appendChild(item);
  }
}

function showEmpty() {
  state.view = "overview";
  state.selectedCid = null;
  els.emptyMain.classList.remove("hidden");
  els.overview.classList.add("hidden");
  els.detail.classList.add("hidden");
  els.backOverview.classList.add("hidden");
  els.accountName.textContent = "Tất cả tài khoản";
  els.accountCid.textContent = "Thêm CID để bắt đầu";
  renderAccounts();
}

function showOverviewScreen() {
  state.view = "overview";
  state.selectedCid = null;
  els.emptyMain.classList.add("hidden");
  els.overview.classList.remove("hidden");
  els.detail.classList.add("hidden");
  els.backOverview.classList.add("hidden");
  els.accountName.textContent = "Tất cả tài khoản";
  els.accountCid.textContent = "Chỉ số kỳ đã chọn • ngân sách còn lại từ cảnh báo ngân sách";
  renderAccounts();
}

function sortOverviewRows(rows) {
  return [...rows].sort((a, b) => {
    const aLow = isLowRunway(a);
    const bLow = isLowRunway(b);
    if (aLow !== bLow) return aLow ? -1 : 1;
    if (aLow && bLow) return Number(a.days_remaining) - Number(b.days_remaining);
    return Number(b.cost || 0) - Number(a.cost || 0);
  });
}

function renderOverviewKpis(rows) {
  const low = rows.filter(isLowRunway).length;
  const cost = rows.reduce((sum, row) => sum + Number(row.cost || 0), 0);
  const clicks = rows.reduce((sum, row) => sum + Number(row.clicks || 0), 0);
  const conv = rows.reduce((sum, row) => sum + Number(row.conversions || 0), 0);
  const cards = [
    ["Tài khoản", formatNumber(rows.length)],
    ["Chi phí kỳ", formatMoney(cost)],
    ["Click", formatNumber(clicks)],
    ["Chuyển đổi", formatNumber(conv, 2)],
    ["Còn dưới 3 ngày", String(low)],
    ["NS còn lại", formatMoney(rows.reduce((sum, row) => sum + Number(row.remaining_budget || 0), 0))],
  ];
  els.overviewKpis.innerHTML = cards
    .map(([label, value]) => `<article class="kpi"><label>${label}</label><b>${value}</b></article>`)
    .join("");
}

function renderOverviewTable(rows) {
  const thead = els.overviewTable.querySelector("thead");
  const tbody = els.overviewTable.querySelector("tbody");
  thead.innerHTML = `<tr>
    <th>Tài khoản</th>
    <th>CID</th>
    <th class="num">Chi phí</th>
    <th class="num">Click</th>
    <th class="num">Hiện</th>
    <th class="num">CTR</th>
    <th class="num">Conv</th>
    <th class="num">CPA</th>
    <th class="num">NS/ngày</th>
    <th class="num">NS còn lại</th>
    <th class="num">Ngày còn</th>
    <th class="col-actions"></th>
  </tr>`;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="12">Chưa có tài khoản để hiển thị.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const derivedRow = derived(row);
      const tone = daysTone(row.days_remaining);
      return `<tr class="${isLowRunway(row) ? "row-low" : ""}" data-cid="${escapeHtml(row.customerId)}">
        <td class="name-cell">${escapeHtml(row.name || "Chưa có tên")}</td>
        <td class="cid-cell">${escapeHtml(formatCid(row.customerId))}</td>
        <td class="num">${formatMoney(derivedRow.cost)}</td>
        <td class="num">${formatNumber(derivedRow.clicks)}</td>
        <td class="num">${formatNumber(derivedRow.impressions)}</td>
        <td class="num">${formatPct(derivedRow.ctr)}</td>
        <td class="num">${formatNumber(derivedRow.conversions, 2)}</td>
        <td class="num">${formatMoney(derivedRow.cpa || 0)}</td>
        <td class="num">${row.daily_budget == null ? "—" : formatMoney(row.daily_budget)}</td>
        <td class="num">${row.remaining_budget == null ? "—" : formatMoney(row.remaining_budget)}</td>
        <td class="num"><span class="badge ${tone}">${escapeHtml(formatDays(row.days_remaining))}</span></td>
        <td class="col-actions"><button type="button" class="row-delete" title="Xóa khỏi theo dõi">✕</button></td>
      </tr>`;
    })
    .join("");
  tbody.querySelectorAll("tr[data-cid]").forEach((tr) => {
    tr.addEventListener("click", () => selectAccount(tr.dataset.cid));
    tr.querySelector(".row-delete")?.addEventListener("click", async (ev) => {
      ev.stopPropagation();
      const row = rows.find((item) => item.customerId === tr.dataset.cid);
      await unwatchAccount(tr.dataset.cid, row?.name);
    });
  });
}

async function unwatchAccount(customerId, name) {
  const label = name || formatCid(customerId);
  if (!confirm(`Xóa ${label} khỏi danh sách theo dõi trên Railway?`)) return;
  try {
    state.settings = await api.removeAccount(customerId);
    showToast(`Đã xóa ${formatCid(customerId)} trên Railway`);
    if (state.selectedCid === customerId) state.selectedCid = null;
    await showOverview();
  } catch (err) {
    showToast(err.message || "Không xóa được tài khoản.", true);
  }
}

async function showOverview() {
  showOverviewScreen();
  await loadOverview();
}

async function loadOverview() {
  showOverviewScreen();
  state.loading = true;
  els.overviewStatus.textContent = "Đang tải chỉ số và ngân sách còn lại...";
  try {
    const data = await api.fetchOverview(toApiParams(state.date));
    const rows = sortOverviewRows(Array.isArray(data?.rows) ? data.rows : []);
    state.overviewRows = rows;
    state.budgetByCid = Object.fromEntries(rows.map((row) => [row.customerId, row]));
    renderOverviewKpis(rows);
    renderOverviewTable(rows);
    renderAccounts();
    const low = rows.filter(isLowRunway).length;
    els.overviewStatus.textContent = low
      ? `${rows.length} tài khoản • ${low} tài khoản còn dưới 3 ngày (đưa lên đầu)`
      : `${rows.length} tài khoản`;
    if (!rows.length) {
      els.overview.classList.add("hidden");
      els.emptyMain.classList.remove("hidden");
    }
  } catch (err) {
    els.overviewStatus.textContent = err.message || "Không tải được trang tổng quan.";
    showToast(err.message || "Không tải được trang tổng quan.", true);
  } finally {
    state.loading = false;
  }
}

function currentAccount() {
  return (state.settings?.accounts || []).find((row) => row.customerId === state.selectedCid);
}

function renderKpis() {
  const row = state.kpis ? derived(state.kpis) : null;
  const cards = [
    ["Chi phí", row ? formatMoney(row.cost) : "—"],
    ["Click", row ? formatNumber(row.clicks) : "—"],
    ["Hiển thị", row ? formatNumber(row.impressions) : "—"],
    ["CTR", row ? formatPct(row.ctr) : "—"],
    ["Chuyển đổi", row ? formatNumber(row.conversions, 2) : "—"],
    ["CPA", row ? formatMoney(row.cpa || 0) : "—"],
  ];
  els.kpiRow.innerHTML = cards
    .map(([label, value]) => `<article class="kpi"><label>${label}</label><b>${value}</b></article>`)
    .join("");
}

const TAB_COLUMNS = {
  campaigns: [
    { key: "campaign_name", label: "Chiến dịch" },
    { key: "clicks", label: "Click", num: true },
    { key: "impressions", label: "Hiện", num: true },
    { key: "ctr", label: "CTR", num: true },
    { key: "cost", label: "Chi phí", num: true },
    { key: "conversions", label: "Conv", num: true },
    { key: "cpa", label: "CPA", num: true },
    { key: "cpc", label: "CPC", num: true },
  ],
  adgroups: [
    { key: "ad_group_name", label: "Nhóm quảng cáo" },
    { key: "campaign_name", label: "Chiến dịch" },
    { key: "clicks", label: "Click", num: true },
    { key: "impressions", label: "Hiện", num: true },
    { key: "ctr", label: "CTR", num: true },
    { key: "cost", label: "Chi phí", num: true },
    { key: "conversions", label: "Conv", num: true },
    { key: "cpa", label: "CPA", num: true },
  ],
  keywords: [
    { key: "keyword_text", label: "Từ khóa" },
    { key: "match_type", label: "Khớp" },
    { key: "ad_group_name", label: "Nhóm QC" },
    { key: "campaign_name", label: "Chiến dịch" },
    { key: "clicks", label: "Click", num: true },
    { key: "impressions", label: "Hiện", num: true },
    { key: "cost", label: "Chi phí", num: true },
    { key: "conversions", label: "Conv", num: true },
    { key: "cpa", label: "CPA", num: true },
  ],
  budget: [
    { key: "campaign_name", label: "Chiến dịch" },
    { key: "daily_budget", label: "NS/ngày", num: true },
    { key: "cost", label: "Chi phí kỳ", num: true },
    { key: "budget_pct", label: "% NS", num: true },
    { key: "clicks", label: "Click", num: true },
    { key: "conversions", label: "Conv", num: true },
    { key: "cpa", label: "CPA", num: true },
    { key: "status", label: "Trạng thái" },
  ],
};

function formatCell(key, row) {
  if (key === "cost" || key === "cpa" || key === "cpc" || key === "daily_budget") return formatMoney(row[key] || 0);
  if (key === "ctr" || key === "budget_pct") return formatPct(row[key] || 0);
  if (key === "clicks" || key === "impressions") return formatNumber(row[key]);
  if (key === "conversions") return formatNumber(row[key], 2);
  if (key === "status") return `<span class="badge">${escapeHtml(row.status || "")}</span>`;
  if (key === "match_type") return escapeHtml(String(row.match_type || "").replace(/_/g, " "));
  return escapeHtml(row[key] || "");
}

function decorateBudget(row) {
  const days = periodDays(state.date);
  const expected = Number(row.daily_budget || 0) * days;
  return {
    ...derived(row),
    budget_pct: expected ? Number(row.cost || 0) / expected : 0,
  };
}

function decorateRow(row) {
  return state.tab === "budget" ? decorateBudget(row) : derived(row);
}

function visibleRows() {
  const q = state.filter.trim().toLowerCase();
  const rows = state.rows.map(decorateRow).filter((row) => {
    if (!q) return true;
    return JSON.stringify(row).toLowerCase().includes(q);
  });
  const key = state.sortKey;
  rows.sort((a, b) => {
    const va = a[key];
    const vb = b[key];
    const na = Number(va);
    const nb = Number(vb);
    let cmp = 0;
    if (!Number.isNaN(na) && !Number.isNaN(nb) && va !== "" && vb !== "") cmp = na - nb;
    else cmp = String(va || "").localeCompare(String(vb || ""), "vi");
    return state.sortDir === "asc" ? cmp : -cmp;
  });
  return rows;
}

function renderTable() {
  const columns = TAB_COLUMNS[state.tab];
  const thead = els.table.querySelector("thead");
  const tbody = els.table.querySelector("tbody");
  thead.innerHTML = `<tr>${columns
    .map(
      (col) =>
        `<th class="${col.num ? "num" : ""}" data-key="${col.key}">${col.label}${
          state.sortKey === col.key ? (state.sortDir === "asc" ? " ↑" : " ↓") : ""
        }</th>`
    )
    .join("")}</tr>`;
  thead.querySelectorAll("th").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.dataset.key;
      if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      else {
        state.sortKey = key;
        state.sortDir = colIsNum(key) ? "desc" : "asc";
      }
      renderTable();
    });
  });

  const rows = visibleRows();
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="${columns.length}">Không có dữ liệu trong kỳ đã chọn.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map(
      (row) =>
        `<tr>${columns
          .map((col) => `<td class="${col.num ? "num" : ""}">${formatCell(col.key, row)}</td>`)
          .join("")}</tr>`
    )
    .join("");
}

function colIsNum(key) {
  return ["clicks", "impressions", "ctr", "cost", "conversions", "cpa", "cpc", "daily_budget", "budget_pct"].includes(
    key
  );
}

async function selectAccount(customerId) {
  const fromOverview = state.overviewRows.find((row) => row.customerId === customerId);
  if (fromOverview && !(state.settings?.accounts || []).some((row) => row.customerId === customerId)) {
    state.settings = await api.upsertAccount({
      customerId,
      name: fromOverview.name,
      mccId: fromOverview.mccId,
    });
  }
  state.view = "detail";
  state.selectedCid = customerId;
  const acc = currentAccount() || fromOverview;
  els.emptyMain.classList.add("hidden");
  els.overview.classList.add("hidden");
  els.detail.classList.remove("hidden");
  els.backOverview.classList.remove("hidden");
  els.accountName.textContent = acc?.name || "Đang tải tên tài khoản...";
  els.accountCid.textContent = formatCid(customerId);
  els.autoDismiss.checked = Boolean(acc?.autoDismissRecommendations);
  els.dismissStatus.textContent = acc?.autoDismissRecommendations
    ? "Đã bật quét hàng ngày lúc 07:00 trên Railway."
    : "";
  renderAccounts();
  refreshAutoDismissStatus();
  await loadAll();
}

async function refreshAutoDismissStatus() {
  if (!state.selectedCid) return;
  try {
    const data = await api.listRecommendationAutoDismiss(state.selectedCid);
    const row = Array.isArray(data?.rows) ? data.rows[0] : null;
    if (!row) return;
    const enabled = Boolean(row.active);
    els.autoDismiss.checked = enabled;
    if (!enabled) {
      els.dismissStatus.textContent = "Đã tắt quét hàng ngày.";
      return;
    }
    const count = Number(row.last_dismissed_count || 0);
    const when = row.last_run_date || "";
    if (when) {
      els.dismissStatus.textContent = count
        ? `Quét hàng ngày 07:00. Lần gần nhất ${when}: bỏ qua ${count} đề xuất.`
        : `Quét hàng ngày 07:00. Lần gần nhất ${when}: không có đề xuất.`;
    } else {
      els.dismissStatus.textContent = "Đã bật quét hàng ngày lúc 07:00 trên Railway.";
    }
  } catch {
    /* endpoint chưa deploy hoặc lỗi mạng — giữ text local */
  }
}

async function dismissRecommendationsNow() {
  const acc = currentAccount();
  if (!acc?.autoDismissRecommendations || !state.selectedCid) return;
  els.dismissStatus.textContent = "Đang bỏ qua đề xuất lần đầu...";
  try {
    const result = await api.dismissRecommendations(state.selectedCid, { dismissAll: true });
    const count = Number(result?.result?.dismissed_count || 0);
    els.dismissStatus.textContent = count
      ? `Đã bật quét hàng ngày 07:00. Lần này bỏ qua ${count} đề xuất.`
      : "Đã bật quét hàng ngày 07:00. Hiện không có đề xuất.";
  } catch (err) {
    els.dismissStatus.textContent = err.message || "Đã bật quét hàng ngày, nhưng dismiss lần này lỗi.";
  }
}

async function loadAll() {
  if (!state.selectedCid) return;
  const dateFilter = toApiParams(state.date);
  state.loading = true;
  setBusy("Đang lấy dữ liệu từ Railway...");
  try {
    const [customer, table] = await Promise.all([
      api.fetchMetrics("customer", state.selectedCid, dateFilter),
      api.fetchMetrics(state.tab, state.selectedCid, dateFilter),
    ]);
    const kpi = Array.isArray(customer?.rows) ? customer.rows[0] : null;
    state.kpis = kpi;
    state.rows = Array.isArray(table?.rows) ? table.rows : [];
    if (kpi?.customer_name) {
      const acc = currentAccount();
      if (acc && acc.name !== kpi.customer_name) {
        state.settings = await api.renameAccount(state.selectedCid, kpi.customer_name);
        renderAccounts();
      }
      els.accountName.textContent = kpi.customer_name;
    }
    renderKpis();
    renderTable();
    setBusy(`${state.rows.length} dòng • kỳ ${state.date.start === state.date.end ? state.date.start : `${state.date.start} → ${state.date.end}`}`);
  } catch (err) {
    state.kpis = null;
    state.rows = [];
    renderKpis();
    renderTable();
    setBusy(err.message || "Không lấy được dữ liệu.");
    showToast(err.message || "Không lấy được dữ liệu.", true);
  } finally {
    state.loading = false;
  }
}

function openSettings() {
  els.settingsUrl.value = state.settings.baseUrl || "";
  els.settingsKey.value = state.settings.apiKey || "";
  els.settingsAutostart.checked = state.settings.openAtLogin !== false;
  els.settingsStatus.textContent = "";
  els.settingsModal.classList.remove("hidden");
}

function closeSettings() {
  els.settingsModal.classList.add("hidden");
}

function showApp(settings) {
  state.settings = settings;
  els.setup.classList.add("hidden");
  els.app.classList.remove("hidden");
  renderAccounts();
  showOverview();
}

function showSetup(settings) {
  state.settings = settings;
  els.setup.classList.remove("hidden");
  els.app.classList.add("hidden");
  els.setupUrl.value = settings.baseUrl || "";
  els.setupKey.value = settings.apiKey || "";
  els.setupAutostart.checked = settings.openAtLogin !== false;
}

els.addForm.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  els.addError.textContent = "";
  const cid = els.cidInput.value.trim();
  try {
    els.addError.textContent = "Đang kiểm tra CID trên Railway...";
    state.settings = await api.addAccount(cid);
    els.cidInput.value = "";
    els.addError.textContent = "";
    const added = state.settings.accounts.at(-1);
    showToast(`Đã thêm ${formatCid(added.customerId)}`);
    await showOverview();
  } catch (err) {
    els.addError.textContent = err.message || "Không thêm được tài khoản.";
  }
});

els.accountSearch.addEventListener("input", renderAccounts);
els.tableSearch.addEventListener("input", () => {
  state.filter = els.tableSearch.value;
  renderTable();
});

els.tabs.addEventListener("click", (ev) => {
  const btn = ev.target.closest("[data-tab]");
  if (!btn) return;
  state.tab = btn.dataset.tab;
  els.tabs.querySelectorAll(".tab").forEach((el) => el.classList.toggle("active", el === btn));
  state.sortKey = "cost";
  state.sortDir = "desc";
  loadAll();
});

els.autoDismiss.addEventListener("change", async () => {
  if (!state.selectedCid) return;
  const enabled = els.autoDismiss.checked;
  try {
    state.settings = await api.setAccountOption(state.selectedCid, {
      autoDismissRecommendations: enabled,
    });
    if (enabled) await dismissRecommendationsNow();
    else els.dismissStatus.textContent = "Đã tắt quét hàng ngày.";
  } catch (err) {
    els.autoDismiss.checked = !enabled;
    showToast(err.message || "Không lưu được tùy chọn đề xuất.", true);
  }
});

els.refreshBtn.addEventListener("click", () => {
  if (state.view === "detail" && state.selectedCid) loadAll();
  else showOverview();
});
els.backOverview.addEventListener("click", () => showOverview());
els.settingsBtn.addEventListener("click", openSettings);
els.settingsCancel.addEventListener("click", closeSettings);
els.settingsModal.addEventListener("click", (ev) => {
  if (ev.target === els.settingsModal) closeSettings();
});
els.settingsSave.addEventListener("click", async () => {
  try {
    state.settings = await api.saveSettings({
      baseUrl: els.settingsUrl.value.trim(),
      apiKey: els.settingsKey.value.trim(),
      openAtLogin: els.settingsAutostart.checked,
    });
    els.settingsStatus.className = "form-error ok";
    els.settingsStatus.textContent = "Đã lưu.";
    closeSettings();
    if (state.view === "detail" && state.selectedCid) loadAll();
    else showOverview();
  } catch (err) {
    els.settingsStatus.className = "form-error";
    els.settingsStatus.textContent = err.message;
  }
});
els.settingsTest.addEventListener("click", async () => {
  els.settingsStatus.textContent = "Đang kiểm tra...";
  try {
    await api.saveSettings({
      baseUrl: els.settingsUrl.value.trim(),
      apiKey: els.settingsKey.value.trim(),
      openAtLogin: els.settingsAutostart.checked,
    });
    const result = await api.testConnection();
    els.settingsStatus.className = "form-error ok";
    els.settingsStatus.textContent = result?.ok ? "Kết nối Railway thành công." : "Railway phản hồi nhưng chưa sẵn sàng.";
  } catch (err) {
    els.settingsStatus.className = "form-error";
    els.settingsStatus.textContent = err.message;
  }
});

els.setupSave.addEventListener("click", async () => {
  els.setupError.textContent = "";
  try {
    const settings = await api.saveSettings({
      baseUrl: els.setupUrl.value.trim(),
      apiKey: els.setupKey.value.trim(),
      openAtLogin: els.setupAutostart.checked,
    });
    await api.testConnection();
    showApp(settings);
  } catch (err) {
    els.setupError.textContent = err.message || "Không kết nối được Railway.";
  }
});

window.__adsDatePicker = new DatePicker(document.getElementById("date-picker-root"), {
  initial: { preset: "TODAY" },
  onChange(next) {
    state.date = next;
    if (state.view === "detail" && state.selectedCid) loadAll();
    else showOverview();
  },
});
state.date = window.__adsDatePicker.getValue();

(async function init() {
  const settings = await api.getState();
  if (settings.needsSetup) showSetup(settings);
  else showApp(settings);
})();
