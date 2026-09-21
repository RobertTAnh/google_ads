const fs = require("fs");
const path = require("path");
const { app } = require("electron");

const DEFAULT_BASE_URL = "https://web-production-f8738.up.railway.app";

function storePath() {
  return path.join(app.getPath("userData"), "ads-manager-settings.json");
}

function parentEnvPath() {
  return path.join(__dirname, "..", "..", ".env");
}

function parseDotEnv(text) {
  const out = {};
  const src = String(text || "");
  let i = 0;
  while (i < src.length) {
    while (i < src.length && /[ \t\r\n]/.test(src[i])) i += 1;
    if (src[i] === "#") {
      while (i < src.length && src[i] !== "\n") i += 1;
      continue;
    }
    const keyStart = i;
    while (i < src.length && src[i] !== "=" && src[i] !== "\n") i += 1;
    if (src[i] !== "=") {
      while (i < src.length && src[i] !== "\n") i += 1;
      continue;
    }
    const key = src.slice(keyStart, i).trim();
    i += 1;
    while (src[i] === " " || src[i] === "\t") i += 1;
    let value = "";
    if (src[i] === "'" || src[i] === '"') {
      const quote = src[i];
      i += 1;
      const start = i;
      while (i < src.length && src[i] !== quote) i += 1;
      value = src.slice(start, i);
      if (src[i] === quote) i += 1;
    } else {
      const start = i;
      while (i < src.length && src[i] !== "\n") i += 1;
      value = src.slice(start, i).trim();
    }
    if (key) out[key] = value;
  }
  return out;
}

function readParentEnv() {
  try {
    const raw = fs.readFileSync(parentEnvPath(), "utf8");
    return parseDotEnv(raw);
  } catch {
    return {};
  }
}

function listMccIdsFromEnv() {
  const env = {
    ...readParentEnv(),
    ...process.env,
  };
  const raw = String(env.GOOGLE_ADS_MCC_CONFIGS || "").trim();
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Object.keys(parsed.mccs || {})
      .map((id) => String(id).replace(/\D/g, ""))
      .filter((id) => id.length === 10);
  } catch {
    return [];
  }
}

function defaultState() {
  const env = {
    ...readParentEnv(),
    ...process.env,
  };
  return {
    baseUrl: String(env.GOOGLE_ADS_MCP_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, ""),
    apiKey: String(env.MCP_API_KEY || "").trim(),
    openAtLogin: true,
    knownMccIds: listMccIdsFromEnv(),
    accounts: [],
  };
}

function normalizeState(raw) {
  const fallback = defaultState();
  const data = raw && typeof raw === "object" ? raw : {};
  const accounts = Array.isArray(data.accounts)
    ? data.accounts
        .map((row) => ({
          customerId: String(row?.customerId || "").replace(/\D/g, ""),
          name: String(row?.name || ""),
          mccId: String(row?.mccId || "").replace(/\D/g, ""),
          addedAt: String(row?.addedAt || ""),
          autoDismissRecommendations: row?.autoDismissRecommendations === true,
        }))
        .filter((row) => row.customerId.length === 10)
    : [];
  const knownMccIds = Array.from(
    new Set(
      [...(Array.isArray(data.knownMccIds) ? data.knownMccIds : []), ...fallback.knownMccIds]
        .map((id) => String(id || "").replace(/\D/g, ""))
        .filter((id) => id.length === 10)
    )
  );
  return {
    baseUrl: String(data.baseUrl || fallback.baseUrl).replace(/\/+$/, "") || fallback.baseUrl,
    apiKey: String(data.apiKey ?? fallback.apiKey).trim(),
    openAtLogin: data.openAtLogin !== false,
    knownMccIds,
    accounts,
  };
}

function loadState() {
  try {
    const raw = fs.readFileSync(storePath(), "utf8");
    return normalizeState(JSON.parse(raw));
  } catch {
    return defaultState();
  }
}

function saveState(state) {
  const next = normalizeState(state);
  fs.mkdirSync(path.dirname(storePath()), { recursive: true });
  fs.writeFileSync(storePath(), JSON.stringify(next, null, 2), "utf8");
  return next;
}

module.exports = {
  DEFAULT_BASE_URL,
  loadState,
  saveState,
  normalizeState,
};
