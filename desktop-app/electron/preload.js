const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("adsDesk", {
  getState: () => ipcRenderer.invoke("get-state"),
  saveSettings: (patch) => ipcRenderer.invoke("save-settings", patch),
  setOpenAtLogin: (enabled) => ipcRenderer.invoke("set-open-at-login", enabled),
  testConnection: () => ipcRenderer.invoke("test-connection"),
  addAccount: (cid) => ipcRenderer.invoke("add-account", cid),
  removeAccount: (cid) => ipcRenderer.invoke("remove-account", cid),
  renameAccount: (customerId, name) => ipcRenderer.invoke("rename-account", { customerId, name }),
  upsertAccount: (row) => ipcRenderer.invoke("upsert-account", row),
  setAccountOption: (customerId, patch) => ipcRenderer.invoke("set-account-option", { customerId, ...patch }),
  listRecommendationAutoDismiss: (customerId) =>
    ipcRenderer.invoke("list-recommendation-auto-dismiss", { customerId }),
  dismissRecommendations: (customerId, opts) =>
    ipcRenderer.invoke("dismiss-recommendations", { customerId, ...(opts || {}) }),
  fetchOverview: (dateFilter) => ipcRenderer.invoke("fetch-overview", { dateFilter }),
  fetchMetrics: (kind, customerId, dateFilter) =>
    ipcRenderer.invoke("fetch-metrics", { kind, customerId, dateFilter }),
});
