const { app, BrowserWindow, ipcMain, shell } = require("electron");
const fs = require("fs");
const path = require("path");
const { loadState, saveState } = require("./store");
const railway = require("./railway");

let mainWindow = null;
let state = null;

function startupScriptPath() {
  return path.join(
    app.getPath("appData"),
    "Microsoft",
    "Windows",
    "Start Menu",
    "Programs",
    "Startup",
    "AdsManager.vbs"
  );
}

function vbsRunLine(exePath, appArg) {
  const cmd = appArg ? `"${exePath}" "${appArg}"` : `"${exePath}"`;
  return `WshShell.Run "${cmd.replace(/"/g, '""')}", 1, False`;
}

function applyOpenAtLogin(enabled) {
  const on = Boolean(enabled);
  if (process.platform !== "win32") {
    app.setLoginItemSettings({ openAtLogin: on });
    return;
  }

  // Gỡ bản registry cũ (lỗi khi đường dẫn có khoảng trắng).
  app.setLoginItemSettings({
    openAtLogin: false,
    path: process.execPath,
    args: [],
  });

  const scriptPath = startupScriptPath();
  if (!on) {
    try {
      fs.unlinkSync(scriptPath);
    } catch {
      /* ignore */
    }
    return;
  }

  const appRoot = path.join(__dirname, "..");
  const launchCmd = app.isPackaged
    ? vbsRunLine(process.execPath)
    : vbsRunLine(process.execPath, appRoot);
  const vbs = `Set WshShell = CreateObject("WScript.Shell")\r\n${launchCmd}\r\n`;
  fs.mkdirSync(path.dirname(scriptPath), { recursive: true });
  fs.writeFileSync(scriptPath, vbs, "ascii");
}

function safeApplyOpenAtLogin(enabled) {
  try {
    applyOpenAtLogin(enabled);
  } catch (err) {
    console.error("Khởi động cùng Windows:", err);
  }
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1380,
    height: 860,
    minWidth: 1100,
    minHeight: 680,
    backgroundColor: "#f3f4f6",
    autoHideMenuBar: true,
    show: false,
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  mainWindow.once("ready-to-show", () => mainWindow.show());
  mainWindow.on("closed", () => {
    mainWindow = null;
  });
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });
  mainWindow.loadFile(path.join(__dirname, "..", "renderer", "index.html"));
}

function publicState() {
  return {
    baseUrl: state.baseUrl,
    apiKey: state.apiKey,
    openAtLogin: state.openAtLogin,
    accounts: state.accounts,
    needsSetup: !state.apiKey,
  };
}

async function syncAutoDismissToRailway() {
  const enabled = (state?.accounts || []).filter((row) => row.autoDismissRecommendations);
  if (!enabled.length || !state?.apiKey) return;
  for (const row of enabled) {
    try {
      await railway.setRecommendationAutoDismiss(state, {
        customerId: row.customerId,
        mccId: row.mccId || "",
        label: row.name || "",
        active: true,
      });
    } catch (err) {
      console.error("Đăng ký quét dismiss hàng ngày:", row.customerId, err);
    }
  }
}

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on("second-instance", () => {
    if (!mainWindow) return;
    if (mainWindow.isMinimized()) mainWindow.restore();
    mainWindow.show();
    mainWindow.focus();
  });

  app.whenReady().then(() => {
    app.setAppUserModelId("com.fago.ads-manager");
    state = loadState();
    safeApplyOpenAtLogin(state.openAtLogin);
    createWindow();
    syncAutoDismissToRailway().catch((err) => {
      console.error("Đồng bộ auto-dismiss lên Railway:", err);
    });
  });
}

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});

ipcMain.handle("get-state", async () => publicState());

ipcMain.handle("save-settings", async (_event, patch) => {
  state = saveState({
    ...state,
    baseUrl: patch?.baseUrl ?? state.baseUrl,
    apiKey: patch?.apiKey ?? state.apiKey,
    openAtLogin: patch?.openAtLogin ?? state.openAtLogin,
  });
  applyOpenAtLogin(state.openAtLogin);
  return publicState();
});

ipcMain.handle("set-open-at-login", async (_event, enabled) => {
  state = saveState({ ...state, openAtLogin: Boolean(enabled) });
  applyOpenAtLogin(state.openAtLogin);
  return publicState();
});

ipcMain.handle("test-connection", async () => {
  return railway.ping(state);
});

ipcMain.handle("add-account", async (_event, rawCid) => {
  const customerId = railway.normalizeCid(rawCid);
  if (customerId.length !== 10) {
    throw new Error("CID phải đủ 10 chữ số.");
  }
  if (state.accounts.some((row) => row.customerId === customerId)) {
    throw new Error("Tài khoản này đã có trong danh sách.");
  }

  let name = "";
  let mccId = "";
  try {
    const resolved = await railway.resolveMcc(state, customerId);
    mccId = String(resolved?.mcc_customer_id || "").replace(/\D/g, "");
  } catch {
    /* MCC map có thể chưa có; vẫn thử lấy metrics */
  }

  const mccCandidates = Array.from(new Set([mccId, ...(state.knownMccIds || [])].filter(Boolean)));
  let lastError = null;
  let found = false;
  const tryList = mccCandidates.length ? mccCandidates : [""];
  for (const candidate of tryList) {
    try {
      const perf = await railway.customerPerformance(
        state,
        customerId,
        { date_range: "LAST_30_DAYS" },
        candidate
      );
      const row = Array.isArray(perf?.rows) ? perf.rows[0] : null;
      name = String(row?.customer_name || "").trim();
      mccId = candidate || String(perf?.mcc_customer_id || "").replace(/\D/g, "") || mccId;
      found = true;
      break;
    } catch (err) {
      lastError = err;
    }
  }
  if (!found && lastError && !mccId) {
    throw lastError;
  }

  if (mccId) {
    try {
      await railway.addBudgetAlert(state, { customerId, mccId, label: name });
    } catch {
      /* vẫn giữ local nếu Railway chưa nhận CID */
    }
  }

  state = saveState({
    ...state,
    accounts: [
      ...state.accounts,
      {
          customerId,
          name,
          mccId,
          addedAt: new Date().toISOString(),
          autoDismissRecommendations: false,
        },
    ],
  });
  return publicState();
});

ipcMain.handle("remove-account", async (_event, rawCid) => {
  const customerId = railway.normalizeCid(rawCid);
  if (customerId.length !== 10) {
    throw new Error("CID phải đủ 10 chữ số.");
  }
  await railway.deleteBudgetAlert(state, customerId);
  try {
    await railway.deleteRecommendationAutoDismiss(state, customerId);
  } catch {
    /* Railway có thể chưa có endpoint cũ */
  }
  state = saveState({
    ...state,
    accounts: state.accounts.filter((row) => row.customerId !== customerId),
  });
  return publicState();
});

ipcMain.handle("rename-account", async (_event, payload) => {
  const customerId = railway.normalizeCid(payload?.customerId);
  const name = String(payload?.name || "").trim();
  state = saveState({
    ...state,
    accounts: state.accounts.map((row) =>
      row.customerId === customerId ? { ...row, name: name || row.name } : row
    ),
  });
  return publicState();
});

ipcMain.handle("upsert-account", async (_event, payload) => {
  const customerId = railway.normalizeCid(payload?.customerId);
  if (customerId.length !== 10) {
    throw new Error("CID phải đủ 10 chữ số.");
  }
  const name = String(payload?.name || "").trim();
  const mccId = String(payload?.mccId || "").replace(/\D/g, "");
  const existing = state.accounts.find((row) => row.customerId === customerId);
  if (existing) {
    state = saveState({
      ...state,
      accounts: state.accounts.map((row) =>
        row.customerId === customerId
          ? {
              ...row,
              name: name || row.name,
              mccId: mccId || row.mccId,
              autoDismissRecommendations:
                payload?.autoDismissRecommendations == null
                  ? row.autoDismissRecommendations
                  : Boolean(payload.autoDismissRecommendations),
            }
          : row
      ),
    });
  } else {
    state = saveState({
      ...state,
      accounts: [
        ...state.accounts,
        {
          customerId,
          name,
          mccId,
          addedAt: new Date().toISOString(),
          autoDismissRecommendations: false,
        },
      ],
    });
  }
  return publicState();
});

ipcMain.handle("set-account-option", async (_event, payload) => {
  const customerId = railway.normalizeCid(payload?.customerId);
  if (customerId.length !== 10) {
    throw new Error("Thiếu CID tài khoản.");
  }
  const exists = state.accounts.find((row) => row.customerId === customerId);
  if (!exists) {
    throw new Error("Tài khoản chưa có trong danh sách.");
  }
  const enabled =
    payload?.autoDismissRecommendations == null
      ? Boolean(exists.autoDismissRecommendations)
      : Boolean(payload.autoDismissRecommendations);
  if (enabled) {
    await railway.setRecommendationAutoDismiss(state, {
      customerId,
      mccId: exists.mccId || "",
      label: exists.name || "",
      active: true,
    });
  } else {
    try {
      await railway.setRecommendationAutoDismiss(state, {
        customerId,
        mccId: exists.mccId || "",
        label: exists.name || "",
        active: false,
      });
    } catch {
      await railway.deleteRecommendationAutoDismiss(state, customerId);
    }
  }
  state = saveState({
    ...state,
    accounts: state.accounts.map((row) =>
      row.customerId === customerId
        ? {
            ...row,
            autoDismissRecommendations: enabled,
          }
        : row
    ),
  });
  return publicState();
});

ipcMain.handle("list-recommendation-auto-dismiss", async (_event, payload) => {
  const customerId = railway.normalizeCid(payload?.customerId);
  return railway.listRecommendationAutoDismiss(state, customerId);
});

ipcMain.handle("dismiss-recommendations", async (_event, payload) => {
  const customerId = railway.normalizeCid(payload?.customerId);
  if (customerId.length !== 10) {
    throw new Error("Thiếu CID tài khoản.");
  }
  const account = state.accounts.find((row) => row.customerId === customerId);
  return railway.dismissRecommendations(state, customerId, {
    mccId: account?.mccId || "",
    dismissAll: payload?.dismissAll !== false,
    resourceNames: Array.isArray(payload?.resourceNames) ? payload.resourceNames : [],
  });
});

async function mapPool(items, limit, mapper) {
  const out = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const index = next;
      next += 1;
      out[index] = await mapper(items[index], index);
    }
  }
  const workers = Array.from({ length: Math.min(Math.max(limit, 1), items.length || 1) }, () => worker());
  await Promise.all(workers);
  return out;
}

function pickMetrics(payload) {
  const row = Array.isArray(payload?.rows) ? payload.rows[0] : null;
  return {
    name: String(row?.customer_name || ""),
    clicks: Number(row?.clicks || 0),
    impressions: Number(row?.impressions || 0),
    cost: Number(row?.cost || 0),
    conversions: Number(row?.conversions || 0),
    cpa: row?.cpa == null ? null : Number(row.cpa),
  };
}

ipcMain.handle("fetch-overview", async (_event, payload) => {
  const dateFilter = payload?.dateFilter || { date_range: "TODAY" };
  let alerts = [];
  let runwayApiEnabled = true;
  try {
    const data = await railway.budgetAlerts(state);
    alerts = Array.isArray(data?.rows) ? data.rows : [];
  } catch (err) {
    const msg = String(err.message || "");
    if (Number(err.status) === 404 || /HTTP 404/.test(msg)) {
      runwayApiEnabled = false;
    }
    alerts = [];
  }

  const alertByCid = new Map();
  for (const row of alerts) {
    const cid = railway.normalizeCid(row.customer_id);
    if (cid.length === 10) alertByCid.set(cid, row);
  }

  const accounts = new Map();
  for (const row of state.accounts || []) {
    accounts.set(row.customerId, {
      customerId: row.customerId,
      name: row.name || "",
      mccId: row.mccId || "",
      fromLocal: true,
    });
  }
  for (const row of alerts) {
    const cid = railway.normalizeCid(row.customer_id);
    if (cid.length !== 10) continue;
    const current = accounts.get(cid);
    if (current) {
      accounts.set(cid, {
        ...current,
        name: current.name || row.label || "",
        mccId: current.mccId || railway.normalizeCid(row.mcc_id),
      });
    } else {
      accounts.set(cid, {
        customerId: cid,
        name: row.label || "",
        mccId: railway.normalizeCid(row.mcc_id),
        fromLocal: false,
      });
    }
  }

  const list = Array.from(accounts.values());
  const rows = await mapPool(list, 4, async (account) => {
    let metrics = {
      name: account.name,
      clicks: 0,
      impressions: 0,
      cost: 0,
      conversions: 0,
      cpa: null,
    };
    let error = "";
    try {
      const perf = await railway.customerPerformance(
        state,
        account.customerId,
        dateFilter,
        account.mccId
      );
      metrics = { ...metrics, ...pickMetrics(perf) };
      if (!metrics.name) metrics.name = account.name;
    } catch (err) {
      error = err.message || "Không lấy được chỉ số.";
    }

    let runway = alertByCid.get(account.customerId) || null;
    if (runwayApiEnabled && (!runway || runway.days_remaining == null)) {
      try {
        runway = await railway.budgetRunway(state, account.customerId, account.mccId);
      } catch (err) {
        const msg = String(err.message || "");
        if (Number(err.status) === 404 || /HTTP 404/.test(msg)) {
          runwayApiEnabled = false;
        }
      }
    }

    return {
      customerId: account.customerId,
      name: metrics.name || account.name || "",
      mccId: account.mccId || "",
      fromLocal: account.fromLocal,
      clicks: metrics.clicks,
      impressions: metrics.impressions,
      cost: metrics.cost,
      conversions: metrics.conversions,
      cpa: metrics.cpa,
      daily_budget: runway?.daily_budget ?? null,
      remaining_budget: runway?.remaining_budget ?? null,
      days_remaining: runway?.days_remaining ?? null,
      budget_status: runway?.status || "",
      last_check_at: runway?.last_check_at || "",
      error,
    };
  });

  return { ok: true, rows };
});

ipcMain.handle("fetch-metrics", async (_event, payload) => {
  const kind = String(payload?.kind || "");
  const customerId = railway.normalizeCid(payload?.customerId);
  const dateFilter = payload?.dateFilter || { date_range: "TODAY" };
  if (customerId.length !== 10) {
    throw new Error("Thiếu CID tài khoản.");
  }

  const handlers = {
    customer: railway.customerPerformance,
    campaigns: railway.campaignPerformance,
    adgroups: railway.adGroupPerformance,
    keywords: railway.keywordPerformance,
    budget: railway.campaignBudgetMetrics,
  };
  const handler = handlers[kind];
  if (!handler) {
    throw new Error(`Loại dữ liệu không hỗ trợ: ${kind}`);
  }
  const account = state.accounts.find((row) => row.customerId === customerId);
  return handler(state, customerId, dateFilter, account?.mccId || "");
});
