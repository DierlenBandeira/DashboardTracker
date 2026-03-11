const API_BASE = "https://dashboardtracker-pb4e.onrender.com";
const endpoint = `${API_BASE}/status`;

const MIN_VISIBLE_POINTS = 72;
const BASE_PX_PER_POINT = 26;
const MIN_PX_PER_POINT = 2;
const MAX_CANVAS_CSS_WIDTH = 16000;
const MAX_DEVICE_PIXEL_RATIO = 1.5;
const STEP_SEC = 10;

const STATE_BLOCK_SERIES = new Set(["motor", "ar_cond", "freio"]);

const DEFAULT_PALETTE = {
  velocidade: "#1f77b4",
  rpm: "#ff7f0e",
  consumido: "#2ca02c",
  pct_acelerado: "#d62728",
  altitude: "#9467bd",

  motor: "#0f766e",
  temperatura_motor: "#dc2626",
  ar_cond: "#0891b2",
  freio: "#7c3aed",
  arla: "#ca8a04",
  consumido_delta: "#16a34a",
  peso_total: "#6b7280",
};

let PALETTE = { ...DEFAULT_PALETTE };

const BASE_SERIES = [
  { key: "velocidade", label: "Velocidade" },
  { key: "rpm", label: "RPM" },
  { key: "consumido", label: "Consumido (Δ)" },
  { key: "pct_acelerado", label: "% Acelerado" },
  { key: "altitude", label: "Altitude" },
];

const REPLAY_EXTRA_SERIES = [
  { key: "motor", label: "Motor*" },
  { key: "temperatura_motor", label: "Temperatura do Motor" },
  { key: "ar_cond", label: "Ar Cond." },
  { key: "freio", label: "Freio" },
  { key: "arla", label: "Arla" },
  { key: "consumido_delta", label: "Consumido Delta*" },
  { key: "peso_total", label: "Peso Total*" },
];

const SERIES_VISIBILITY = {
  velocidade: true,
  rpm: true,
  consumido: true,
  pct_acelerado: true,
  altitude: true,

  motor: false,
  temperatura_motor: false,
  ar_cond: false,
  freio: false,
  arla: false,
  consumido_delta: false,
  peso_total: false,
};

function apiUrl(path) {
  return `${API_BASE}${path}`;
}

function getAllSeries() {
  return replayMode ? [...BASE_SERIES, ...REPLAY_EXTRA_SERIES] : [...BASE_SERIES];
}

function getVisibleSeries() {
  return getAllSeries()
    .map((s) => ({ ...s, color: PALETTE[s.key] }))
    .filter((serie) => SERIES_VISIBILITY[serie.key]);
}

function getAllSeriesWithColors() {
  return getAllSeries().map((s) => ({ ...s, color: PALETTE[s.key] }));
}

function getStateBlockBandLayout(HIT) {
  const visibleBlockSeries = HIT.series.filter((s) => STATE_BLOCK_SERIES.has(s.key));

  if (!visibleBlockSeries.length) {
    return {
      visibleBlockSeries,
      singleMode: false,
      getBandTop: () => HIT.M.t,
      getBandHeight: () => 12,
      getBandCenterY: () => HIT.M.t + 6,
    };
  }

  if (visibleBlockSeries.length === 1) {
    return {
      visibleBlockSeries,
      singleMode: true,
      getBandTop: () => HIT.M.t,
      getBandHeight: () => HIT.plotH,
      getBandCenterY: () => HIT.M.t + HIT.plotH / 2,
    };
  }

  const topOffset = 26;
  const rowGap = 18;
  const bandHeight = 12;

  return {
    visibleBlockSeries,
    singleMode: false,
    getBandTop: (seriesKey) => {
      const idx = visibleBlockSeries.findIndex((s) => s.key === seriesKey);
      return HIT.M.t + topOffset + idx * rowGap;
    },
    getBandHeight: () => bandHeight,
    getBandCenterY: (seriesKey) => {
      const idx = visibleBlockSeries.findIndex((s) => s.key === seriesKey);
      return HIT.M.t + topOffset + idx * rowGap + bandHeight / 2;
    },
  };
}

function renderLegend() {
  const legend = document.getElementById("chartLegend");
  if (!legend) return;

  legend.innerHTML = "";

  getAllSeriesWithColors().forEach((serie) => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `legend-chip legend-${serie.key}`;
    if (!SERIES_VISIBILITY[serie.key]) item.classList.add("is-off");

    item.innerHTML = `
      <span class="legend-dot" style="background:${serie.color}"></span>
      <span>${serie.label}</span>
    `;

    item.addEventListener("click", () => {
      SERIES_VISIBILITY[serie.key] = !SERIES_VISIBILITY[serie.key];
      renderLegend();
      drawChart();
    });

    legend.appendChild(item);
  });
}

function formatTimestampLabel(msgTm) {
  if (!msgTm) return "-";
  const d = new Date(msgTm * 1000);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${dd}/${mm} ${hh}:${mi}:${ss}`;
}

function formatTimestampFull(msgTm) {
  if (!msgTm) return "-";
  const d = new Date(msgTm * 1000);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${dd}/${mm}/${yyyy} ${hh}:${mi}:${ss}`;
}

function excelColToIndex(col) {
  let n = 0;
  const s = String(col).toUpperCase().trim();
  for (let i = 0; i < s.length; i++) {
    n = n * 26 + (s.charCodeAt(i) - 64);
  }
  return n - 1;
}

const IMPORT_COLS = {
  id: excelColToIndex("A"),
  tm: excelColToIndex("C"),
  motor: excelColToIndex("F"),
  rpm: excelColToIndex("J"),
  temperatura_motor: excelColToIndex("AC"),
  velocidade: excelColToIndex("AE"),
  ar_cond: excelColToIndex("AF"),
  pct_acelerado: excelColToIndex("AI"),
  freio: excelColToIndex("AJ"),
  consumido_raw: excelColToIndex("AN"),
  arla_raw: excelColToIndex("AP"),
  altitude: excelColToIndex("AW"),
  consumido_delta_raw: excelColToIndex("AY"),
  peso_total: excelColToIndex("BG"),
};

let lastConsumidoRaw = null;
let lastMsgTm = null;
let samples = [];

const POLL_MS = 10000;
let pollProgressTimer = null;
let pollProgressStartedAt = Date.now();

let commentsByMsgTm = {};
let selectedIdx = null;
let selectedMsgTm = null;
let autoScrollToEnd = true;

let yCfg = {
  velocidade: { min: 0, max: 150 },
  rpm: { min: 400, max: 2500 },
  consumido: { min: 0, max: 1 },
  pct_acelerado: { min: 0, max: 400 },
  altitude: { min: 0, max: 1200 },

  motor: { min: 0, max: 1 },
  temperatura_motor: { min: 60, max: 130 },
  ar_cond: { min: 0, max: 1 },
  freio: { min: 0, max: 1 },
  arla: { min: 0, max: 5 },
  consumido_delta: { min: 0, max: 5 },
  peso_total: { min: 0, max: 40000 },
};

let HIT = null;
let HOVER = {
  idx: null,
  x: null,
  y: null,
  seriesKey: null,
};

let unitsCache = [];

let replayMode = false;
let replayRows = [];
let replayIndex = 0;
let replayTimer = null;
const REPLAY_STEP_MS = 2000;

let isDrawing = false;
let lastScrollEventAt = 0;
let pointerDownInfo = null;

function formatAge(sec) {
  if (sec == null || !Number.isFinite(sec)) return "-";
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  const rem = sec % 60;
  if (min < 60) return `${min}min ${rem}s`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return `${h}h ${m}min`;
}

function badgeHtml(text, cls) {
  return `<span class="badge ${cls}"><span class="badge-dot"></span>${text}</span>`;
}

function toggleConfigPanel() {
  const wrap = document.getElementById("configPanelWrap");
  const btn = document.getElementById("toggleConfigBtn");
  const text = document.getElementById("toggleConfigText");
  const isOpen = wrap.classList.contains("open");

  if (isOpen) {
    wrap.classList.remove("open");
    btn.classList.remove("open");
    btn.setAttribute("aria-expanded", "false");
    text.textContent = "Mostrar configurações";
  } else {
    wrap.classList.add("open");
    btn.classList.add("open");
    btn.setAttribute("aria-expanded", "true");
    text.textContent = "Ocultar configurações";
  }
}

function startPollProgress() {
  pollProgressStartedAt = Date.now();
  if (pollProgressTimer) clearInterval(pollProgressTimer);

  const bar = document.getElementById("pollProgressBar");
  if (!bar) return;

  bar.style.width = "0%";

  pollProgressTimer = setInterval(() => {
    const elapsed = Date.now() - pollProgressStartedAt;
    const pct = Math.min(100, (elapsed / POLL_MS) * 100);
    bar.style.width = pct + "%";
  }, 80);
}

function restartPollProgress() {
  const bar = document.getElementById("pollProgressBar");
  if (!bar) return;

  bar.style.width = "100%";

  setTimeout(() => {
    bar.style.transition = "none";
    bar.style.width = "0%";

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        bar.style.transition = "width 0.15s linear";
        startPollProgress();
      });
    });
  }, 120);
}

async function postJson(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
  return j;
}

window.updateSid = async function () {
  const sid = document.getElementById("sidInput")?.value?.trim();
  if (!sid) return alert("Informe o SID.");
  try {
    await postJson(apiUrl("/set_sid"), { sid });
    alert("SID atualizado.");
    await refreshUnits();
  } catch (e) {
    alert("Erro ao atualizar SID: " + e);
  }
};

function extractNumericValue(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  let s = String(value).trim();
  if (!s) return null;

  s = s.replace(/\u00A0/g, " ");
  s = s.replace(/\s+/g, " ").trim();

  const match = s.match(/-?\d+(?:[.,]\d+)?/);
  if (!match) return null;

  let num = match[0];

  if (num.includes(",") && num.includes(".")) {
    if (num.lastIndexOf(",") > num.lastIndexOf(".")) {
      num = num.replace(/\./g, "").replace(",", ".");
    } else {
      num = num.replace(/,/g, "");
    }
  } else if (num.includes(",")) {
    num = num.replace(",", ".");
  }

  const parsed = Number(num);
  return Number.isFinite(parsed) ? parsed : null;
}

function toNum(x) {
  return extractNumericValue(x);
}

function parseBrazilDateTime(text) {
  if (!text) return null;
  const s = String(text).trim();

  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [, dd, mm, yyyy, hh, mi, ss = "00"] = m;
    return new Date(
      Number(yyyy),
      Number(mm) - 1,
      Number(dd),
      Number(hh),
      Number(mi),
      Number(ss)
    );
  }

  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;

  return null;
}

function normalizeOnOff(value) {
  const s = String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
  if (!s) return null;

  if (s.includes("desligado")) return 0;
  if (s.includes("ligado")) return 1;

  return null;
}

function normalizeBrake(value) {
  const s = String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
  if (!s) return null;

  if (s.includes("desacionado")) return 0;
  if (s.includes("acionado")) return 1;

  return null;
}

function formatValueForTip(key, value) {
  if (value == null) return "-";

  if (key === "motor" || key === "ar_cond") {
    return Number(value) === 1 ? "Ligado" : "Desligado";
  }

  if (key === "freio") {
    return Number(value) === 1 ? "Acionado" : "Desacionado";
  }

  return Number(value).toFixed(2);
}

function pushSample(fields, msgTm) {
  const values = {};
  const curConsumido = toNum(fields?.consumido?.raw);

  let deltaConsumido = 0;
  if (curConsumido === null) {
    deltaConsumido = null;
  } else if (lastConsumidoRaw === null) {
    deltaConsumido = 0;
  } else {
    const d = curConsumido - lastConsumidoRaw;
    deltaConsumido = Number.isFinite(d) && d > 0 ? d : 0;
  }

  lastConsumidoRaw = curConsumido === null ? lastConsumidoRaw : curConsumido;

  for (const s of BASE_SERIES) {
    if (s.key === "consumido") values[s.key] = deltaConsumido;
    else values[s.key] = toNum(fields?.[s.key]?.raw);
  }

  if (replayMode) {
    for (const s of REPLAY_EXTRA_SERIES) values[s.key] = null;
  }

  samples.push({
    t: Date.now(),
    msgTm: msgTm ?? null,
    values,
    isGap: false,
  });
}

function pushReplaySample(row) {
  samples.push({
    t: Date.now(),
    msgTm: row.msgTm ?? null,
    values: {
      velocidade: row.velocidade,
      altitude: row.altitude,
      pct_acelerado: row.pct_acelerado,
      consumido: row.consumido,
      rpm: row.rpm,

      motor: row.motor,
      temperatura_motor: row.temperatura_motor,
      ar_cond: row.ar_cond,
      freio: row.freio,
      arla: row.arla,
      consumido_delta: row.consumido_delta,
      peso_total: row.peso_total,
    },
    isGap: false,
  });

  if (row.msgTm != null) lastMsgTm = row.msgTm;
  if (row.consumidoRaw != null) lastConsumidoRaw = row.consumidoRaw;
  if (row.comment && row.msgTm != null) commentsByMsgTm[row.msgTm] = row.comment;
}

function pushGap() {
  let lastValues = {};
  const allSeries = getAllSeries();

  if (samples.length > 0) {
    lastValues = { ...samples[samples.length - 1].values };
  } else {
    for (const s of allSeries) lastValues[s.key] = null;
  }

  samples.push({
    t: Date.now(),
    msgTm: null,
    values: lastValues,
    isGap: true,
  });
}

function getSampleAtVisualIndex(idx) {
  const totalSlots = Math.max(samples.length, MIN_VISIBLE_POINTS);
  const slotOffset = totalSlots - samples.length;
  const sampleIdx = idx - slotOffset;
  if (sampleIdx < 0 || sampleIdx >= samples.length) return null;
  return samples[sampleIdx];
}

function formatMsgTmToDateTime(msgTm) {
  return formatTimestampFull(msgTm);
}

window.downloadData = function () {
  if (!samples.length) {
    alert("Não há dados para exportar.");
    return;
  }

  const currentUnitId = window.__currentItemId ?? "";

  const rows = samples
    .filter((s) => s && !s.isGap && s.msgTm != null)
    .map((s, index) => ({
      ID: index + 1,
      tm: formatMsgTmToDateTime(s.msgTm),
      Velocidade: s.values?.velocidade ?? "",
      Altitude: s.values?.altitude ?? "",
      "% Acelerado": s.values?.pct_acelerado ?? "",
      "Consumido (Δ)": s.values?.consumido ?? "",
      RPM: s.values?.rpm ?? "",
      Motor: s.values?.motor ?? "",
      "Temperatura Motor": s.values?.temperatura_motor ?? "",
      "Ar Cond": s.values?.ar_cond ?? "",
      Freio: s.values?.freio ?? "",
      Arla: s.values?.arla ?? "",
      "Consumido Delta": s.values?.consumido_delta ?? "",
      "Peso Total": s.values?.peso_total ?? "",
      Comentarios: commentsByMsgTm[s.msgTm] ?? "",
    }));

  if (!rows.length) {
    alert("Não há leituras válidas para exportar.");
    return;
  }

  const ws = XLSX.utils.json_to_sheet(rows);
  ws["!cols"] = [
    { wch: 8 },
    { wch: 22 },
    { wch: 14 },
    { wch: 14 },
    { wch: 16 },
    { wch: 16 },
    { wch: 12 },
    { wch: 10 },
    { wch: 18 },
    { wch: 10 },
    { wch: 10 },
    { wch: 10 },
    { wch: 18 },
    { wch: 14 },
    { wch: 30 },
  ];

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Dados");

  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");

  const fileName = `monitoramento_${currentUnitId || "unidade"}_${y}${m}${d}_${hh}${mi}.xlsx`;
  XLSX.writeFile(wb, fileName);
};

async function refreshUnits() {
  const sel = document.getElementById("unitSelect");
  if (!sel) return;
  sel.innerHTML = `<option>Carregando...</option>`;

  try {
    const r = await fetch(apiUrl("/units"), { cache: "no-store" });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || "HTTP " + r.status);

    unitsCache = j.units || [];
    if (!unitsCache.length) {
      sel.innerHTML = `<option>Nenhuma unidade</option>`;
      return;
    }

    const currentId = window.__currentItemId || null;

    sel.innerHTML = unitsCache
      .map((u) => {
        const selected = currentId && Number(u.id) === Number(currentId) ? "selected" : "";
        return `<option value="${u.id}" ${selected}>${u.name}</option>`;
      })
      .join("");
  } catch (e) {
    sel.innerHTML = `<option>Erro ao carregar: ${e}</option>`;
  }
}

document.addEventListener("change", async (ev) => {
  if (ev.target && ev.target.id === "unitSelect") {
    const itemId = Number(ev.target.value);
    if (!Number.isFinite(itemId)) return;
    try {
      await postJson(apiUrl("/set_unit"), { itemId });
      window.__currentItemId = itemId;
      clearChart();
    } catch (e) {
      alert("Erro ao trocar unidade: " + e);
    }
  }
});

function buildSeriesArrays() {
  const allSeries = getAllSeries();
  const seriesData = {};
  for (const s of allSeries) seriesData[s.key] = [];

  for (const smp of samples) {
    for (const s of allSeries) {
      seriesData[s.key].push(smp.values?.[s.key] ?? null);
    }
  }
  return seriesData;
}

function buildGapFlags() {
  return samples.map((smp) => !!smp.isGap);
}

function getEffectivePxPerPoint(totalPoints) {
  if (totalPoints <= 0) return BASE_PX_PER_POINT;

  const naturalWidth = totalPoints * BASE_PX_PER_POINT;
  if (naturalWidth <= MAX_CANVAS_CSS_WIDTH) return BASE_PX_PER_POINT;

  const compressed = MAX_CANVAS_CSS_WIDTH / totalPoints;
  return Math.max(MIN_PX_PER_POINT, compressed);
}

function getSafeDevicePixelRatio() {
  const dpr = window.devicePixelRatio || 1;
  return Math.min(dpr, MAX_DEVICE_PIXEL_RATIO);
}

function setupCanvas(canvas) {
  const wrap = document.getElementById("chartWrap");
  const dpr = getSafeDevicePixelRatio();

  const visibleWidth = wrap ? Math.max(1, Math.floor(wrap.clientWidth)) : 1200;
  const totalPoints = Math.max(samples.length, MIN_VISIBLE_POINTS);

  const pxPerPoint = getEffectivePxPerPoint(totalPoints);
  const desiredCssWidth = Math.max(visibleWidth, Math.ceil(totalPoints * pxPerPoint));
  const cssW = Math.min(desiredCssWidth, MAX_CANVAS_CSS_WIDTH);
  const cssH = 560;

  if (canvas.style.width !== `${cssW}px`) canvas.style.width = cssW + "px";
  if (canvas.style.height !== `${cssH}px`) canvas.style.height = cssH + "px";

  const pxW = Math.floor(cssW * dpr);
  const pxH = Math.floor(cssH * dpr);

  if (canvas.width !== pxW || canvas.height !== pxH) {
    canvas.width = pxW;
    canvas.height = pxH;
  }

  const ctx = canvas.getContext("2d", { alpha: false });
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  return {
    ctx,
    W: cssW,
    H: cssH,
    dpr,
    pxPerPoint,
    totalPoints,
  };
}

function getYRangeFor(key, dataArr) {
  const cfg = yCfg[key] || {};
  const hasMin = cfg.min !== null && cfg.min !== undefined && cfg.min !== "";
  const hasMax = cfg.max !== null && cfg.max !== undefined && cfg.max !== "";

  if (hasMin && hasMax) {
    const min = Number(cfg.min);
    const max = Number(cfg.max);
    if (Number.isFinite(min) && Number.isFinite(max) && min !== max) {
      return { min, max, auto: false };
    }
  }

  let min = Infinity;
  let max = -Infinity;
  for (const v of dataArr) {
    if (v === null) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (min === Infinity) return { min: 0, max: 1, auto: true };
  if (min === max) return { min: min - 1, max: max + 1, auto: true };

  const pad = (max - min) * 0.12;
  return { min: min - pad, max: max + pad, auto: true };
}

function clamp(v, a, b) {
  return Math.max(a, Math.min(b, v));
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showTip(x, y, html) {
  const tip = document.getElementById("tip");
  const chartWrap = document.getElementById("chartWrap");

  tip.innerHTML = html;
  tip.style.display = "block";

  const tw = tip.offsetWidth;
  const th = tip.offsetHeight;

  const scrollLeft = chartWrap.scrollLeft;
  const visibleLeft = scrollLeft;
  const visibleRight = scrollLeft + chartWrap.clientWidth;

  let left = x + 12;
  let top = y + 12;

  const minL = visibleLeft + 6;
  const maxL = visibleRight - tw - 6;
  const maxT = chartWrap.clientHeight - th - 6;

  left = clamp(left, minL, Math.max(minL, maxL));
  top = clamp(top, 6, Math.max(6, maxT));

  tip.style.left = left + "px";
  tip.style.top = top + "px";
}

function hideTip(force = false) {
  if (!force && selectedIdx !== null) return;
  document.getElementById("tip").style.display = "none";
}

function openCommentPopover(idx, px, py) {
  const sample = getSampleAtVisualIndex(idx);
  if (!sample || sample.msgTm == null) return;

  selectedIdx = idx;
  selectedMsgTm = sample.msgTm;

  const pop = document.getElementById("commentPopover");
  const input = document.getElementById("commentInput");
  const timeLabel = document.getElementById("commentTimeLabel");
  const chartWrap = document.getElementById("chartWrap");

  timeLabel.textContent = `Instante: ${formatTimestampFull(sample.msgTm)}`;
  input.value = commentsByMsgTm[selectedMsgTm] || "";

  pop.classList.remove("hidden");

  const pw = 320;
  const ph = 210;

  const scrollLeft = chartWrap.scrollLeft;
  const visibleLeft = scrollLeft;
  const visibleRight = scrollLeft + chartWrap.clientWidth;

  let left = px + 14;
  let top = py + 14;

  if (left + pw > visibleRight - 8) left = px - pw - 14;
  if (top + ph > chartWrap.clientHeight - 8) top = py - ph - 14;

  left = Math.max(visibleLeft + 8, left);
  top = Math.max(8, top);

  pop.style.left = left + "px";
  pop.style.top = top + "px";

  drawChart();
  setTimeout(() => input.focus(), 0);
}

function closeCommentPopover() {
  selectedIdx = null;
  selectedMsgTm = null;
  document.getElementById("commentPopover").classList.add("hidden");
  drawChart();
  hideTip(true);
}

function saveComment() {
  if (selectedMsgTm == null) return;
  const text = document.getElementById("commentInput").value.trim();

  if (text) commentsByMsgTm[selectedMsgTm] = text;
  else delete commentsByMsgTm[selectedMsgTm];

  if (replayMode) {
    const row = replayRows.find((r) => r.msgTm === selectedMsgTm);
    if (row) row.comment = text || "";
  }

  drawChart();
  closeCommentPopover();
}

function removeComment() {
  if (selectedMsgTm == null) return;
  delete commentsByMsgTm[selectedMsgTm];

  if (replayMode) {
    const row = replayRows.find((r) => r.msgTm === selectedMsgTm);
    if (row) row.comment = "";
  }

  drawChart();
  closeCommentPopover();
}

function maybeScrollChartToEnd(force = false) {
  const wrap = document.getElementById("chartWrap");
  if (!wrap) return;

  if (force || autoScrollToEnd) {
    requestAnimationFrame(() => {
      wrap.scrollLeft = Math.max(0, wrap.scrollWidth - wrap.clientWidth);
    });
  }
}

function preserveScrollDuringDraw() {
  const wrap = document.getElementById("chartWrap");
  if (!wrap) return () => {};

  const previousLeft = wrap.scrollLeft;
  const shouldKeepManualPosition = !autoScrollToEnd;

  return () => {
    if (shouldKeepManualPosition) {
      wrap.scrollLeft = previousLeft;
    }
  };
}

function drawChart() {
  if (isDrawing) return;
  isDrawing = true;

  try {
    const canvas = document.getElementById("chart");
    if (!canvas) return;

    const restoreScroll = preserveScrollDuringDraw();
    const { ctx, W, H, pxPerPoint } = setupCanvas(canvas);
    restoreScroll();

    ctx.clearRect(0, 0, W, H);

    const seriesData = buildSeriesArrays();
    const gapFlags = buildGapFlags();
    const visibleSeries = getVisibleSeries();

    const totalSlots = Math.max(samples.length, MIN_VISIBLE_POINTS);
    const slotOffset = totalSlots - samples.length;

    const M = { l: 20, r: 20, t: 12, b: 42 };
    const plotW = W - M.l - M.r;
    const plotH = H - M.t - M.b;

    const xAt = (i) => {
      if (totalSlots <= 1) return M.l;
      return M.l + (plotW * (i / (totalSlots - 1)));
    };

    const yAtNorm = (n) => M.t + plotH * (1 - n);

    HIT = {
      mode: "single_norm",
      W,
      H,
      M,
      plotW,
      plotH,
      xAt,
      series: [],
      count: samples.length,
      totalSlots,
      slotOffset,
    };

    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, W, H);

    ctx.strokeStyle = "#f0f0f0";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const yy = M.t + (plotH * i) / 4;
      ctx.beginPath();
      ctx.moveTo(M.l, yy);
      ctx.lineTo(W - M.r, yy);
      ctx.stroke();
    }

    let inGap = false;
    let gapStart = null;

    for (let i = 0; i < samples.length; i++) {
      const isGap = gapFlags[i];

      if (isGap && !inGap) {
        inGap = true;
        gapStart = i;
      }

      const isLast = i === samples.length - 1;
      if (inGap && (!isGap || isLast)) {
        const gapEnd = isGap && isLast ? i : i - 1;
        const x1 = xAt(slotOffset + gapStart);
        const x2 = xAt(slotOffset + gapEnd);
        const gapCount = gapEnd - gapStart + 1;
        const gapSeconds = gapCount * STEP_SEC;

        if (gapSeconds >= 15) {
          const width = Math.max(6, x2 - x1);
          ctx.fillStyle = "rgba(220, 38, 38, 0.10)";
          ctx.fillRect(x1, M.t, width, plotH);

          ctx.strokeStyle = "rgba(220, 38, 38, 0.25)";
          ctx.lineWidth = 1;
          ctx.strokeRect(x1, M.t, width, plotH);

          if (width > 70) {
            ctx.fillStyle = "rgba(153, 27, 27, 0.9)";
            ctx.font = "12px Arial";
            ctx.textAlign = "left";
            ctx.textBaseline = "alphabetic";
            ctx.fillText("Falha de sinal", x1 + 8, M.t + 18);
          }
        }

        inGap = false;
        gapStart = null;
      }
    }

    if (!visibleSeries.length) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "600 16px Arial";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("Nenhuma série selecionada", W / 2, H / 2);

      document.getElementById("chartMeta").innerText =
        `Leituras: ${samples.length} • base visual mínima: ${MIN_VISIBLE_POINTS} pontos • comentários: ${Object.keys(commentsByMsgTm).length}`;
      return;
    }

    for (const s of visibleSeries) {
      const arr = seriesData[s.key];
      const { min, max } = getYRangeFor(s.key, arr);
      HIT.series.push({ key: s.key, label: s.label, color: s.color, arr, min, max });
    }

    const stateBandLayout = getStateBlockBandLayout(HIT);

    for (const s of visibleSeries) {
      const hitSeries = HIT.series.find((it) => it.key === s.key);
      if (!hitSeries) continue;

      const arr = hitSeries.arr;
      const min = hitSeries.min;
      const max = hitSeries.max;
      const denom = (max - min) || 1;

      if (STATE_BLOCK_SERIES.has(s.key)) {
        let blockStart = null;

        for (let i = 0; i < arr.length; i++) {
          const v = arr[i];
          const isOn = Number(v) === 1;
          const isLast = i === arr.length - 1;

          if (isOn && blockStart === null) {
            blockStart = i;
          }

          if (blockStart !== null && (!isOn || isLast)) {
            const blockEnd = isOn && isLast ? i : i - 1;
            const x1 = xAt(slotOffset + blockStart);
            const x2 = xAt(slotOffset + blockEnd);
            const width = Math.max(6, x2 - x1 + 2);

            const bandTop = stateBandLayout.getBandTop(s.key);
            const bandHeight = stateBandLayout.getBandHeight(s.key);

            ctx.fillStyle = s.color + "33";
            ctx.fillRect(x1, bandTop, width, bandHeight);

            ctx.strokeStyle = s.color;
            ctx.lineWidth = 1;
            ctx.strokeRect(x1, bandTop, width, bandHeight);

            if (!stateBandLayout.singleMode && width > 48) {
              ctx.fillStyle = s.color;
              ctx.font = "11px Arial";
              ctx.textAlign = "left";
              ctx.textBaseline = "middle";
              ctx.fillText(s.label, x1 + 6, bandTop + bandHeight / 2);
            }

            blockStart = null;
          }
        }

        continue;
      }

      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;
      ctx.beginPath();

      let started = false;
      for (let i = 0; i < arr.length; i++) {
        const v = arr[i];
        if (v === null || v === undefined) {
          started = false;
          continue;
        }

        let n = (v - min) / denom;
        if (!Number.isFinite(n)) {
          started = false;
          continue;
        }

        n = Math.max(0, Math.min(1, n));
        const x = xAt(slotOffset + i);
        const y = yAtNorm(n);

        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      }

      ctx.stroke();

      const pointStep =
        samples.length > 12000 ? 24 :
        samples.length > 8000 ? 16 :
        samples.length > 5000 ? 10 :
        samples.length > 3000 ? 6 :
        samples.length > 1500 ? 3 : 1;

      const pointRadius =
        pxPerPoint <= 3 ? 0 :
        pxPerPoint <= 5 ? 1 :
        2;

      if (pointRadius > 0) {
        ctx.fillStyle = s.color;
        for (let i = 0; i < arr.length; i += pointStep) {
          const v = arr[i];
          if (v === null || v === undefined) continue;

          let n = (v - min) / denom;
          if (!Number.isFinite(n)) continue;
          n = Math.max(0, Math.min(1, n));

          ctx.beginPath();
          ctx.arc(xAt(slotOffset + i), yAtNorm(n), pointRadius, 0, Math.PI * 2);
          ctx.fill();
        }
      }
    }

    for (let i = 0; i < samples.length; i++) {
      const sample = samples[i];
      if (!sample || sample.isGap || sample.msgTm == null) continue;
      if (!commentsByMsgTm[sample.msgTm]) continue;

      const x = xAt(slotOffset + i);
      const y = M.t + 14;

      ctx.beginPath();
      ctx.fillStyle = selectedMsgTm === sample.msgTm ? "#2563eb" : "#111827";
      ctx.arc(x, y, 7, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = "#ffffff";
      ctx.font = "bold 10px Arial";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("i", x, y + 0.5);
    }

    if (HOVER && HOVER.idx !== null && HOVER.x !== null) {
      ctx.strokeStyle = "rgba(0,0,0,0.18)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(HOVER.x, M.t);
      ctx.lineTo(HOVER.x, M.t + plotH);
      ctx.stroke();

      if (HOVER.y !== null) {
        ctx.strokeStyle = "rgba(0,0,0,0.12)";
        ctx.beginPath();
        ctx.moveTo(M.l, HOVER.y);
        ctx.lineTo(W - M.r, HOVER.y);
        ctx.stroke();
      }
    }

    const axisTickCount = Math.min(6, totalSlots);
    ctx.fillStyle = "#6b7280";
    ctx.font = "11px Arial";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";

    for (let k = 0; k < axisTickCount; k++) {
      const visualIdx =
        axisTickCount === 1
          ? totalSlots - 1
          : Math.round((k / (axisTickCount - 1)) * (totalSlots - 1));

      const sample = getSampleAtVisualIndex(visualIdx);
      const x = xAt(visualIdx);
      const y = M.t + plotH + 8;

      ctx.strokeStyle = "#d1d5db";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, M.t + plotH);
      ctx.lineTo(x, M.t + plotH + 4);
      ctx.stroke();

      const label = sample?.msgTm ? formatTimestampLabel(sample.msgTm) : "";
      if (label) ctx.fillText(label, x, y);
    }

    document.getElementById("chartMeta").innerText =
      `Leituras: ${samples.length} • base visual mínima: ${MIN_VISIBLE_POINTS} pontos • densidade adaptativa ativa • comentários: ${Object.keys(commentsByMsgTm).length}`;
  } finally {
    isDrawing = false;
  }
}

function getMousePositionOnChart(ev) {
  const chartWrap = document.getElementById("chartWrap");
  const wrapRect = chartWrap.getBoundingClientRect();

  return {
    mx: ev.clientX - wrapRect.left + chartWrap.scrollLeft,
    my: ev.clientY - wrapRect.top,
    wrapRect,
    chartWrap,
  };
}

function findNearestRenderablePoint(ev) {
  if (!HIT || HIT.mode !== "single_norm" || HIT.count <= 0 || !HIT.series.length) return null;

  const { mx, my } = getMousePositionOnChart(ev);
  const { M, plotW, xAt, totalSlots, slotOffset } = HIT;

  const t = (mx - M.l) / plotW;
  const visualIdx = Math.round(clamp(t, 0, 1) * (totalSlots - 1));
  const idx = visualIdx - slotOffset;

  if (idx < 0 || idx >= samples.length) return null;

  let best = null;
  const stateBandLayout = getStateBlockBandLayout(HIT);

  for (const s of HIT.series) {
    const v = s.arr[idx];
    if (v === null || v === undefined) continue;

    let px = xAt(visualIdx);
    let py;

    if (STATE_BLOCK_SERIES.has(s.key)) {
      py = stateBandLayout.getBandCenterY(s.key);
    } else {
      const denom = (s.max - s.min) || 1;
      let n = (v - s.min) / denom;
      if (!Number.isFinite(n)) continue;
      n = Math.max(0, Math.min(1, n));
      py = M.t + HIT.plotH * (1 - n);
    }

    const dx = mx - px;
    const dy = my - py;
    const dist2 = dx * dx + dy * dy;

    if (!best || dist2 < best.dist2) {
      best = { series: s, v, px, py, dist2, visualIdx, idx, mx, my };
    }
  }

  return best;
}

function onMove(ev) {
  const best = findNearestRenderablePoint(ev);

  if (!best) {
    if (HOVER.idx !== null || HOVER.seriesKey !== null) {
      HOVER = { idx: null, x: null, y: null, seriesKey: null };
      drawChart();
    }
    hideTip();
    return;
  }

  const sameHover =
    HOVER.idx === best.visualIdx &&
    HOVER.seriesKey === best.series.key &&
    HOVER.x === best.px &&
    HOVER.y === best.py;

  if (!sameHover) {
    HOVER = {
      idx: best.visualIdx,
      x: best.px,
      y: best.py,
      seriesKey: best.series.key,
    };
    drawChart();
  }

  const hoveredSample = getSampleAtVisualIndex(best.visualIdx);
  const comment = hoveredSample?.msgTm != null ? commentsByMsgTm[hoveredSample.msgTm] : null;
  const timeLabel = hoveredSample?.msgTm != null ? formatTimestampFull(hoveredSample.msgTm) : "Sem timestamp";

  const html = `
    <div><b>${best.series.label}</b></div>
    <div style="margin-top:4px;">Valor: <b>${escapeHtml(formatValueForTip(best.series.key, best.v))}</b></div>
    <div style="margin-top:4px;opacity:.9;">Timestamp: ${timeLabel}</div>
    ${
      comment
        ? `<div style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.15);"><b>Comentário:</b><br>${escapeHtml(comment)}</div>`
        : `<div style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.15);opacity:.85;">Clique no ponto para adicionar comentário</div>`
    }
  `;

  showTip(best.px, best.py, html);
}

function onLeave() {
  HOVER = { idx: null, x: null, y: null, seriesKey: null };
  drawChart();
  hideTip();
}

function onChartClick(ev) {
  if (Date.now() - lastScrollEventAt < 180) return;
  if (pointerDownInfo?.moved) return;

  const best = findNearestRenderablePoint(ev);
  if (!best) return;

  const sample = getSampleAtVisualIndex(best.visualIdx);
  if (!sample || sample.isGap || sample.msgTm == null) return;

  const CLICK_RADIUS = 12;
  const MAX_X_DISTANCE = 10;

  const dx = Math.abs(best.mx - best.px);
  if (dx > MAX_X_DISTANCE) return;
  if (best.dist2 > CLICK_RADIUS * CLICK_RADIUS) return;

  ev.preventDefault();

  const { wrapRect, chartWrap } = getMousePositionOnChart(ev);
  openCommentPopover(
    best.visualIdx,
    ev.clientX - wrapRect.left + chartWrap.scrollLeft,
    ev.clientY - wrapRect.top
  );
}

window.clearChart = function () {
  samples = [];
  lastConsumidoRaw = null;
  lastMsgTm = null;
  HOVER = { idx: null, x: null, y: null, seriesKey: null };
  closeCommentPopover();
  drawChart();
  maybeScrollChartToEnd(true);
};

function renderYConfig() {
  const wrap = document.getElementById("yConfig");
  if (!wrap) return;

  wrap.innerHTML = getAllSeriesWithColors()
    .map((s) => {
      const cfg = yCfg[s.key] || {};
      const vmin = cfg.min ?? "";
      const vmax = cfg.max ?? "";
      return `
      <div class="card">
        <div class="k">${s.label}</div>
        <div class="cfgrow">
          <input id="ymin_${s.key}" placeholder="Y min (auto)" value="${vmin}">
          <input id="ymax_${s.key}" placeholder="Y max (auto)" value="${vmax}">
        </div>
      </div>
    `;
    })
    .join("");
}

function ensureColorConfigContainer() {
  const configPanel = document.getElementById("configPanel");
  if (!configPanel) return null;

  let title = document.getElementById("colorConfigTitle");
  let container = document.getElementById("colorConfig");

  if (!title) {
    title = document.createElement("div");
    title.id = "colorConfigTitle";
    title.className = "config-section-title";
    title.style.marginTop = "16px";
    title.textContent = "Cores das séries";
    configPanel.appendChild(title);
  }

  if (!container) {
    container = document.createElement("div");
    container.id = "colorConfig";
    container.className = "cfggrid";
    configPanel.appendChild(container);
  }

  return container;
}

function renderColorConfig() {
  const container = ensureColorConfigContainer();
  if (!container) return;

  container.innerHTML = getAllSeriesWithColors()
    .map(
      (s) => `
    <div class="card">
      <div class="k">${s.label}</div>
      <div class="cfgrow">
        <input type="color" id="color_${s.key}" value="${PALETTE[s.key] || "#000000"}" style="min-width:56px;width:56px;padding:4px;height:40px;">
        <input id="colorhex_${s.key}" value="${PALETTE[s.key] || "#000000"}" placeholder="#000000" style="min-width:120px;">
      </div>
    </div>
  `
    )
    .join("");

  getAllSeriesWithColors().forEach((s) => {
    const colorInput = document.getElementById(`color_${s.key}`);
    const hexInput = document.getElementById(`colorhex_${s.key}`);
    if (!colorInput || !hexInput) return;

    colorInput.addEventListener("input", () => {
      hexInput.value = colorInput.value;
      PALETTE[s.key] = colorInput.value;
      renderLegend();
      drawChart();
    });

    hexInput.addEventListener("change", () => {
      const v = String(hexInput.value || "").trim();
      if (/^#[0-9a-fA-F]{6}$/.test(v)) {
        colorInput.value = v;
        PALETTE[s.key] = v;
        renderLegend();
        drawChart();
      } else {
        hexInput.value = PALETTE[s.key];
      }
    });
  });
}

window.applyYConfig = function () {
  for (const s of getAllSeriesWithColors()) {
    const minv = document.getElementById("ymin_" + s.key)?.value?.trim();
    const maxv = document.getElementById("ymax_" + s.key)?.value?.trim();
    yCfg[s.key] = {
      min: minv === "" ? null : Number(minv),
      max: maxv === "" ? null : Number(maxv),
    };
  }
  drawChart();
};

window.resetYConfig = function () {
  yCfg = {
    velocidade: { min: 0, max: 150 },
    rpm: { min: 400, max: 2500 },
    consumido: { min: 0, max: 1 },
    pct_acelerado: { min: 0, max: 400 },
    altitude: { min: 0, max: 1200 },

    motor: { min: 0, max: 1 },
    temperatura_motor: { min: 60, max: 130 },
    ar_cond: { min: 0, max: 1 },
    freio: { min: 0, max: 1 },
    arla: { min: 0, max: 5 },
    consumido_delta: { min: 0, max: 5 },
    peso_total: { min: 0, max: 40000 },
  };
  renderYConfig();
  drawChart();
};

window.resetSeriesColors = function () {
  PALETTE = { ...DEFAULT_PALETTE };
  renderLegend();
  renderColorConfig();
  drawChart();
};

function setReplayStatus(text) {
  const el = document.getElementById("replayStatus");
  if (el) el.textContent = text;
}

function updateReplayPanelInfo() {
  document.getElementById("replayTotalRows").textContent = replayRows.length;
  document.getElementById("replayCurrentIndex").textContent = replayIndex;
  document.getElementById("replayCurrentTm").textContent =
    replayIndex > 0 && replayRows[replayIndex - 1] ? formatMsgTmToDateTime(replayRows[replayIndex - 1].msgTm) : "-";
}

function openReplayPanel(fileName) {
  const panel = document.getElementById("replayPanel");
  panel.classList.remove("hidden");
  document.getElementById("replayFileName").textContent = fileName || "-";
  updateReplayPanelInfo();
}

function resetReplayState() {
  if (replayTimer) {
    clearInterval(replayTimer);
    replayTimer = null;
  }
  replayRows = [];
  replayIndex = 0;
}

function setReplayModeEnabled(enabled) {
  replayMode = enabled;
  renderLegend();
  renderYConfig();
  renderColorConfig();
}

function normalizeReplayRowFromArray(row) {
  const id = row[IMPORT_COLS.id];
  const tmText = row[IMPORT_COLS.tm];
  const dt = parseBrazilDateTime(tmText);
  const msgTm = dt ? Math.floor(dt.getTime() / 1000) : null;

  return {
    id: id ?? null,
    msgTm,

    velocidade: toNum(row[IMPORT_COLS.velocidade]),
    altitude: toNum(row[IMPORT_COLS.altitude]),
    pct_acelerado: toNum(row[IMPORT_COLS.pct_acelerado]),
    rpm: toNum(row[IMPORT_COLS.rpm]),

    motor: normalizeOnOff(row[IMPORT_COLS.motor]),
    temperatura_motor: toNum(row[IMPORT_COLS.temperatura_motor]),
    ar_cond: normalizeOnOff(row[IMPORT_COLS.ar_cond]),
    freio: normalizeBrake(row[IMPORT_COLS.freio]),
    peso_total: toNum(row[IMPORT_COLS.peso_total]),

    consumido: 0,
    consumidoRaw: toNum(row[IMPORT_COLS.consumido_raw]),

    arla: 0,
    arlaRaw: toNum(row[IMPORT_COLS.arla_raw]),

    consumido_delta: 0,
    consumidoDeltaRaw: toNum(row[IMPORT_COLS.consumido_delta_raw]),

    comment: "",
  };
}

window.triggerReplayUpload = function () {
  const input = document.getElementById("replayFileInput");
  if (!input) return;
  input.value = "";
  input.click();
};

async function loadReplayFile(file) {
  if (!file) return;

  if (typeof XLSX === "undefined") {
    alert("Biblioteca XLSX não carregada.");
    return;
  }

  const buffer = await file.arrayBuffer();
  const workbook = XLSX.read(buffer, { type: "array" });
  const firstSheetName = workbook.SheetNames[0];
  if (!firstSheetName) {
    alert("Arquivo sem planilhas.");
    return;
  }

  const sheet = workbook.Sheets[firstSheetName];

  const rows = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
  });

  if (!rows.length) {
    alert("Arquivo sem linhas de dados.");
    return;
  }

  const nonEmptyRows = rows.filter(
    (row) => Array.isArray(row) && row.some((cell) => String(cell ?? "").trim() !== "")
  );

  if (!nonEmptyRows.length) {
    alert("Arquivo sem linhas válidas.");
    return;
  }

  let startIndex = 0;
  for (let i = 0; i < nonEmptyRows.length; i++) {
    const row = nonEmptyRows[i];
    const maybeTm = parseBrazilDateTime(row[IMPORT_COLS.tm]);
    const maybeId = toNum(row[IMPORT_COLS.id]);
    if (maybeTm || maybeId != null) {
      startIndex = i;
      break;
    }
  }

  const dataRows = nonEmptyRows.slice(startIndex);
  const parsed = [];

  for (const row of dataRows) {
    const parsedRow = normalizeReplayRowFromArray(row);
    if (parsedRow.msgTm == null) continue;
    parsed.push(parsedRow);
  }

  if (!parsed.length) {
    alert("Nenhuma linha válida encontrada no modelo novo.");
    return;
  }

  parsed.sort((a, b) => (a.msgTm ?? 0) - (b.msgTm ?? 0));

  let previousConsumidoRaw = null;
  let previousArlaRaw = null;
  let previousConsumidoDeltaRaw = null;

  for (const row of parsed) {
    if (row.consumidoRaw == null) {
      row.consumido = null;
    } else if (previousConsumidoRaw == null) {
      row.consumido = 0;
    } else {
      const delta = row.consumidoRaw - previousConsumidoRaw;
      row.consumido = Number.isFinite(delta) && delta > 0 ? delta : 0;
    }
    if (row.consumidoRaw != null) previousConsumidoRaw = row.consumidoRaw;

    if (row.arlaRaw == null) {
      row.arla = null;
    } else if (previousArlaRaw == null) {
      row.arla = 0;
    } else {
      const delta = row.arlaRaw - previousArlaRaw;
      row.arla = Number.isFinite(delta) && delta > 0 ? delta : 0;
    }
    if (row.arlaRaw != null) previousArlaRaw = row.arlaRaw;

    if (row.consumidoDeltaRaw == null) {
      row.consumido_delta = null;
    } else if (previousConsumidoDeltaRaw == null) {
      row.consumido_delta = 0;
    } else {
      const delta = row.consumidoDeltaRaw - previousConsumidoDeltaRaw;
      row.consumido_delta = Number.isFinite(delta) && delta > 0 ? delta : 0;
    }
    if (row.consumidoDeltaRaw != null) previousConsumidoDeltaRaw = row.consumidoDeltaRaw;
  }

  resetReplayState();
  setReplayModeEnabled(true);
  replayRows = parsed;
  replayIndex = 0;

  samples = [];
  lastConsumidoRaw = null;
  lastMsgTm = null;
  commentsByMsgTm = {};
  HOVER = { idx: null, x: null, y: null, seriesKey: null };
  closeCommentPopover();

  openReplayPanel(file.name);
  setReplayStatus("Arquivo carregado. Pronto para reprodução.");
  drawChart();
  maybeScrollChartToEnd(true);
}

window.startReplay = function () {
  if (!replayMode || !replayRows.length) return;
  if (replayIndex >= replayRows.length) {
    setReplayStatus("Replay concluído.");
    return;
  }
  if (replayTimer) return;

  setReplayStatus("Reproduzindo...");

  replayTimer = setInterval(() => {
    if (replayIndex >= replayRows.length) {
      clearInterval(replayTimer);
      replayTimer = null;
      setReplayStatus("Replay concluído.");
      return;
    }

    const row = replayRows[replayIndex];
    pushReplaySample(row);
    replayIndex += 1;

    updateReplayPanelInfo();
    drawChart();
    maybeScrollChartToEnd();
  }, REPLAY_STEP_MS);
};

window.pauseReplay = function () {
  if (replayTimer) {
    clearInterval(replayTimer);
    replayTimer = null;
  }
  if (replayMode) setReplayStatus("Replay pausado.");
};

window.showAllReplayData = function () {
  if (!replayMode || !replayRows.length) return;

  if (replayTimer) {
    clearInterval(replayTimer);
    replayTimer = null;
  }

  samples = [];
  lastConsumidoRaw = null;
  lastMsgTm = null;
  HOVER = { idx: null, x: null, y: null, seriesKey: null };
  autoScrollToEnd = true;

  const total = replayRows.length;

  if (total <= 5000) {
    for (let i = 0; i < total; i++) {
      pushReplaySample(replayRows[i]);
    }
  } else {
    const step = Math.ceil(total / 5000);
    for (let i = 0; i < total; i += step) {
      pushReplaySample(replayRows[i]);
    }
  }

  replayIndex = replayRows.length;
  updateReplayPanelInfo();
  drawChart();

  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      maybeScrollChartToEnd(true);
    });
  });

  setReplayStatus("Visualização completa carregada.");
};

window.stopReplayMode = function () {
  resetReplayState();
  setReplayModeEnabled(false);

  const panel = document.getElementById("replayPanel");
  panel.classList.add("hidden");

  samples = [];
  lastConsumidoRaw = null;
  lastMsgTm = null;
  commentsByMsgTm = {};
  HOVER = { idx: null, x: null, y: null, seriesKey: null };
  closeCommentPopover();
  drawChart();

  setReplayStatus("Aguardando arquivo");
};

async function tick() {
  if (replayMode) return;

  try {
    const r = await fetch(endpoint, { cache: "no-store" });
    const j = await r.json();

    const meta = document.getElementById("meta");
    const statusBadges = document.getElementById("statusBadges");

    const ok = !!j.ok;
    const err = j.error;

    const data = j?.data || {};
    const f = data?.fields || {};
    const itemId = data?.item_id;
    const msgTm = data?.msg_tm;
    const stale = data?.stale;
    const age = data?.age_sec;
    const refreshed = data?.snapshot_refreshed;

    window.__currentItemId = itemId;
    document.getElementById("unitNow").innerText = itemId ?? "-";

    let addedNewPoint = false;

    if (ok) {
      if (stale) {
        pushGap();
        drawChart();
        addedNewPoint = true;
      } else if (msgTm != null && msgTm !== lastMsgTm) {
        if (lastMsgTm != null) {
          const gapSec = Math.max(0, msgTm - lastMsgTm);
          const missingSteps = Math.floor(gapSec / STEP_SEC) - 1;
          for (let i = 0; i < missingSteps; i++) pushGap();
        }

        lastMsgTm = msgTm;
        pushSample(f, msgTm);
        drawChart();
        addedNewPoint = true;
      }
    }

    if (addedNewPoint) maybeScrollChartToEnd();

    const ts = j.last_update_epoch ? new Date(j.last_update_epoch * 1000).toLocaleString() : "-";

    if (ok) {
      document.getElementById("statusConsulta").innerText = ts;
      document.getElementById("statusTelemetria").innerText = formatAge(age);
      document.getElementById("statusTelemetriaSub").innerText =
        age != null ? `Último dado recebido há ${formatAge(age)}` : "Sem informação";
      document.getElementById("statusSnapshot").innerText = refreshed ? "Novo" : "Mantido";

      const badges = [];
      badges.push(badgeHtml("Backend OK", "badge-ok"));
      badges.push(replayMode ? badgeHtml("Modo replay", "badge-warn") : "");
      badges.push(stale ? badgeHtml("Unidade sem sinal", "badge-bad") : badgeHtml("Unidade online", "badge-ok"));
      badges.push(refreshed ? badgeHtml("Snapshot novo", "badge-ok") : badgeHtml("Snapshot mantido", "badge-warn"));
      statusBadges.innerHTML = badges.filter(Boolean).join("");

      meta.innerHTML = `Consulta do servidor em ${ts}`;
    } else {
      document.getElementById("statusConsulta").innerText = ts;
      document.getElementById("statusTelemetria").innerText = "-";
      document.getElementById("statusTelemetriaSub").innerText = "Falha ao obter status";
      document.getElementById("statusSnapshot").innerText = "-";
      statusBadges.innerHTML = [
        badgeHtml("Erro no backend", "badge-bad"),
        replayMode ? badgeHtml("Modo replay", "badge-warn") : "",
      ].filter(Boolean).join("");
      meta.innerHTML = `<span class="bad">ERRO</span> • ${err ?? "sem detalhe"} • ${ts}`;
    }
  } catch (e) {
    document.getElementById("meta").innerHTML = `<span class="bad">ERRO</span> • ${e}`;
    const statusBadges = document.getElementById("statusBadges");
    if (statusBadges) {
      statusBadges.innerHTML = [
        badgeHtml("Erro de comunicação", "badge-bad"),
        replayMode ? badgeHtml("Modo replay", "badge-warn") : "",
      ].filter(Boolean).join("");
    }
  }
}

document.addEventListener("click", (ev) => {
  const pop = document.getElementById("commentPopover");
  const canvas = document.getElementById("chart");
  if (selectedIdx === null) return;

  const clickedInsidePopover = pop.contains(ev.target);
  const clickedCanvas = ev.target === canvas;

  if (!clickedInsidePopover && !clickedCanvas) {
    closeCommentPopover();
  }
});

const replayFileInput = document.getElementById("replayFileInput");
replayFileInput.addEventListener("change", async (ev) => {
  const file = ev.target.files?.[0];
  try {
    await loadReplayFile(file);
  } catch (e) {
    alert(`Erro ao ler arquivo: ${e.message || e}`);
  }
});

const chartWrapEl = document.getElementById("chartWrap");

chartWrapEl.addEventListener("scroll", () => {
  lastScrollEventAt = Date.now();
  const nearEnd = chartWrapEl.scrollLeft + chartWrapEl.clientWidth >= chartWrapEl.scrollWidth - 40;
  autoScrollToEnd = nearEnd;
});

chartWrapEl.addEventListener("mousedown", (ev) => {
  pointerDownInfo = {
    x: ev.clientX,
    y: ev.clientY,
    moved: false,
  };
});

chartWrapEl.addEventListener("mousemove", (ev) => {
  if (pointerDownInfo) {
    const dx = Math.abs(ev.clientX - pointerDownInfo.x);
    const dy = Math.abs(ev.clientY - pointerDownInfo.y);
    if (dx > 5 || dy > 5) {
      pointerDownInfo.moved = true;
    }
  }
});

window.addEventListener("mouseup", () => {
  setTimeout(() => {
    pointerDownInfo = null;
  }, 0);
});

function ensureConfigActionButtons() {
  const yConfigWrap = document.getElementById("yConfig");
  if (!yConfigWrap) return;

  let actions = document.getElementById("extraConfigActions");
  if (actions) return;

  actions = document.createElement("div");
  actions.id = "extraConfigActions";
  actions.className = "row";
  actions.style.marginTop = "10px";
  actions.innerHTML = `
    <button type="button" onclick="resetSeriesColors()">Reset cores</button>
  `;

  yConfigWrap.parentElement.appendChild(actions);
}

renderYConfig();
renderLegend();
renderColorConfig();
ensureConfigActionButtons();
drawChart();
refreshUnits();
maybeScrollChartToEnd(true);

tick();
startPollProgress();

setInterval(async () => {
  await tick();
  restartPollProgress();
}, POLL_MS);

window.addEventListener("resize", () => {
  const wrap = document.getElementById("chartWrap");
  const previousLeft = wrap ? wrap.scrollLeft : 0;
  const keepManual = !autoScrollToEnd;

  drawChart();

  if (wrap) {
    if (keepManual) {
      wrap.scrollLeft = previousLeft;
    } else {
      maybeScrollChartToEnd();
    }
  }
});

const canvas = document.getElementById("chart");

chartWrapEl.addEventListener("mousemove", onMove);
chartWrapEl.addEventListener("mouseleave", onLeave);
canvas.addEventListener("click", onChartClick);