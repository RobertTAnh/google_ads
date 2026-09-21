(function (global) {
  const TZ = "Asia/Ho_Chi_Minh";
  const WEEKDAYS = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"];
  const PRESETS = [
    { id: "CUSTOM", label: "Tùy chỉnh" },
    { id: "TODAY", label: "Hôm nay" },
    { id: "YESTERDAY", label: "Hôm qua" },
    { id: "THIS_WEEK", label: "Tuần này (Thứ 2 – Hôm nay)" },
    { id: "LAST_7_DAYS", label: "7 ngày qua" },
    { id: "LAST_WEEK", label: "Tuần trước (Thứ 2 – CN)" },
    { id: "LAST_14_DAYS", label: "14 ngày qua" },
    { id: "THIS_MONTH", label: "Tháng này" },
    { id: "LAST_30_DAYS", label: "30 ngày qua" },
  ];

  function pad(n) {
    return String(n).padStart(2, "0");
  }

  function todayYmd() {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());
  }

  function parseYmd(ymd) {
    const [y, m, d] = String(ymd).split("-").map(Number);
    return { y, m, d };
  }

  function toYmd(y, m, d) {
    return `${y}-${pad(m)}-${pad(d)}`;
  }

  function addDays(ymd, days) {
    const { y, m, d } = parseYmd(ymd);
    const dt = new Date(Date.UTC(y, m - 1, d + days));
    return toYmd(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
  }

  function weekdayMon0(ymd) {
    const { y, m, d } = parseYmd(ymd);
    const js = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
    return (js + 6) % 7;
  }

  function daysInMonth(y, m) {
    return new Date(Date.UTC(y, m, 0)).getUTCDate();
  }

  function cmp(a, b) {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  function formatVi(ymd) {
    const { y, m, d } = parseYmd(ymd);
    return `${d} thg ${m}, ${y}`;
  }

  function formatViShort(ymd) {
    const { m, d } = parseYmd(ymd);
    return `${d} thg ${m}`;
  }

  function resolvePreset(preset, today = todayYmd()) {
    if (preset === "TODAY") return { start: today, end: today };
    if (preset === "YESTERDAY") {
      const y = addDays(today, -1);
      return { start: y, end: y };
    }
    if (preset === "LAST_7_DAYS") return { start: addDays(today, -7), end: addDays(today, -1) };
    if (preset === "LAST_14_DAYS") return { start: addDays(today, -14), end: addDays(today, -1) };
    if (preset === "LAST_30_DAYS") return { start: addDays(today, -30), end: addDays(today, -1) };
    if (preset === "THIS_WEEK") {
      const back = weekdayMon0(today);
      return { start: addDays(today, -back), end: today };
    }
    if (preset === "LAST_WEEK") {
      const back = weekdayMon0(today) + 7;
      const start = addDays(today, -back);
      return { start, end: addDays(start, 6) };
    }
    if (preset === "THIS_MONTH") {
      const { y, m } = parseYmd(today);
      return { start: toYmd(y, m, 1), end: today };
    }
    return { start: today, end: today };
  }

  function toApiParams(state) {
    const preset = state.preset;
    if (["TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS"].includes(preset)) {
      return { date_range: preset };
    }
    return { start_date: state.start, end_date: state.end };
  }

  function periodDays(state) {
    const start = Date.parse(`${state.start}T00:00:00Z`);
    const end = Date.parse(`${state.end}T00:00:00Z`);
    return Math.max(1, Math.round((end - start) / 86400000) + 1);
  }

  function triggerLabel(state) {
    const preset = PRESETS.find((p) => p.id === state.preset);
    const title = preset && preset.id !== "CUSTOM" ? preset.label.split(" (")[0] : "Tùy chỉnh";
    const sub =
      state.start === state.end
        ? formatVi(state.start)
        : `${formatViShort(state.start)} – ${formatVi(state.end)}`;
    return { title, sub };
  }

  class DatePicker {
    constructor(root, { initial, onChange }) {
      this.root = root;
      this.onChange = onChange;
      const today = todayYmd();
      const resolved = resolvePreset(initial?.preset || "TODAY", today);
      this.state = {
        preset: initial?.preset || "TODAY",
        start: initial?.start || resolved.start,
        end: initial?.end || resolved.end,
      };
      this.open = false;
      this.view = parseYmd(this.state.end);
      this.pickStep = 0;
      this.render();
    }

    getValue() {
      return { ...this.state };
    }

    close() {
      this.open = false;
      this.render();
    }

    applyPreset(preset) {
      const next = resolvePreset(preset);
      this.state = { preset, ...next };
      this.view = parseYmd(next.end);
      this.pickStep = 0;
      this.open = false;
      this.render();
      this.onChange?.(this.getValue());
    }

    applyCustom(start, end) {
      const ordered = cmp(start, end) <= 0 ? { start, end } : { start: end, end: start };
      this.state = { preset: "CUSTOM", ...ordered };
      this.render();
      this.onChange?.(this.getValue());
    }

    render() {
      const label = triggerLabel(this.state);
      this.root.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.style.position = "relative";

      const trigger = document.createElement("button");
      trigger.type = "button";
      trigger.className = "date-trigger";
      trigger.innerHTML = `<div><strong>${label.title}</strong><span>${label.sub}</span></div>`;
      trigger.addEventListener("click", (ev) => {
        ev.stopPropagation();
        this.open = !this.open;
        this.render();
      });
      wrap.appendChild(trigger);

      if (this.open) {
        wrap.appendChild(this.renderPopover());
      }
      this.root.appendChild(wrap);
    }

    renderPopover() {
      const pop = document.createElement("div");
      pop.className = "date-popover";
      pop.addEventListener("click", (ev) => ev.stopPropagation());

      const presets = document.createElement("div");
      presets.className = "date-presets";
      for (const item of PRESETS) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = item.label;
        btn.className = this.state.preset === item.id ? "active" : "";
        btn.addEventListener("click", () => {
          if (item.id === "CUSTOM") {
            this.state = { ...this.state, preset: "CUSTOM" };
            this.pickStep = 0;
            this.render();
            return;
          }
          this.applyPreset(item.id);
        });
        presets.appendChild(btn);
      }

      const calendar = document.createElement("div");
      calendar.className = "date-calendar";
      calendar.appendChild(this.renderFields());
      calendar.appendChild(this.renderMonth());
      pop.append(presets, calendar);
      return pop;
    }

    renderFields() {
      const wrap = document.createElement("div");
      wrap.className = "date-fields";
      wrap.innerHTML = `
        <label>Ngày bắt đầu*
          <input id="dp-start" type="date" value="${this.state.start}" />
        </label>
        <div class="dash">–</div>
        <label>Ngày kết thúc*
          <input id="dp-end" type="date" value="${this.state.end}" />
        </label>
      `;
      wrap.querySelector("#dp-start").addEventListener("change", (ev) => {
        this.applyCustom(ev.target.value, this.state.end);
      });
      wrap.querySelector("#dp-end").addEventListener("change", (ev) => {
        this.applyCustom(this.state.start, ev.target.value);
      });
      return wrap;
    }

    renderMonth() {
      const { y, m } = this.view;
      const box = document.createElement("div");
      const nav = document.createElement("div");
      nav.className = "cal-nav";
      nav.innerHTML = `
        <button type="button" data-nav="-1">&lt;</button>
        <strong>Tháng ${m} ${y}</strong>
        <button type="button" data-nav="1">&gt;</button>
      `;
      nav.querySelectorAll("button").forEach((btn) => {
        btn.addEventListener("click", () => {
          const delta = Number(btn.dataset.nav);
          let month = m + delta;
          let year = y;
          if (month < 1) {
            month = 12;
            year -= 1;
          }
          if (month > 12) {
            month = 1;
            year += 1;
          }
          this.view = { y: year, m: month, d: 1 };
          this.render();
        });
      });

      const grid = document.createElement("div");
      grid.className = "cal-grid";
      for (const day of WEEKDAYS) {
        const span = document.createElement("span");
        span.textContent = day;
        grid.appendChild(span);
      }

      const firstWeekday = weekdayMon0(toYmd(y, m, 1));
      const count = daysInMonth(y, m);
      const today = todayYmd();
      const leading = firstWeekday;
      const prevMonth = m === 1 ? 12 : m - 1;
      const prevYear = m === 1 ? y - 1 : y;
      const prevCount = daysInMonth(prevYear, prevMonth);

      const cells = [];
      for (let i = leading; i > 0; i -= 1) {
        cells.push({ ymd: toYmd(prevYear, prevMonth, prevCount - i + 1), muted: true });
      }
      for (let d = 1; d <= count; d += 1) {
        cells.push({ ymd: toYmd(y, m, d), muted: false });
      }
      while (cells.length < 42) {
        const extra = cells.length - (leading + count) + 1;
        const nextMonth = m === 12 ? 1 : m + 1;
        const nextYear = m === 12 ? y + 1 : y;
        cells.push({ ymd: toYmd(nextYear, nextMonth, extra), muted: true });
      }

      for (const cell of cells) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = String(parseYmd(cell.ymd).d);
        if (cell.muted) btn.classList.add("muted");
        if (cell.ymd === today) btn.classList.add("today");
        if (cell.ymd === this.state.start) btn.classList.add("start");
        if (cell.ymd === this.state.end) btn.classList.add("end");
        if (cmp(cell.ymd, this.state.start) >= 0 && cmp(cell.ymd, this.state.end) <= 0) {
          btn.classList.add("in-range");
        }
        btn.addEventListener("click", () => {
          if (this.pickStep === 0) {
            this.state = { preset: "CUSTOM", start: cell.ymd, end: cell.ymd };
            this.pickStep = 1;
            this.render();
            return;
          }
          this.pickStep = 0;
          this.applyCustom(this.state.start, cell.ymd);
          this.open = false;
          this.render();
        });
        grid.appendChild(btn);
      }

      box.append(nav, grid);
      return box;
    }
  }

  document.addEventListener("click", () => {
    if (global.__adsDatePicker) global.__adsDatePicker.close();
  });

  global.AdsDatePicker = {
    DatePicker,
    toApiParams,
    periodDays,
    todayYmd,
    resolvePreset,
  };
})(window);
