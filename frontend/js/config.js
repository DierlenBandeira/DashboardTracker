const API_BASE = (
  window.__BACKEND_BASE__ ||
  window.__API_BASE__ ||
  window.location.origin
).replace(/\/$/, "");

function apiUrl(path) {
  return `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

const endpoint = apiUrl("/status");

function apiUrl(path) {
  return `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

const MIN_VISIBLE_POINTS = 72;
const BASE_PX_PER_POINT = 26;
const MIN_PX_PER_POINT = 2;
const MAX_CANVAS_CSS_WIDTH = 16000;
const MAX_DEVICE_PIXEL_RATIO = 1.5;
const STEP_SEC = 10;
const POLL_MS = 10000;
const REPLAY_STEP_MS = 2000;

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

const DEFAULT_SERIES_VISIBILITY = {
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

function excelColToIndex(col) {
  let n = 0;
  const s = String(col).toUpperCase().trim();
  for (let i = 0; i < s.length; i++) {
    n = n * 26 + (s.charCodeAt(i) - 64);
  }
  return n - 1;
}

const DEFAULT_Y_CFG = {
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

const DEFAULT_TRANSIT_MANAGE_CFG = {
  velocidade: {
    enabled: true,
    rule: "below",
    value: 25,
  },
  rpm: {
    enabled: true,
    rule: "outside_range",
    min: 800,
    max: 1800,
  },
  pct_acelerado: {
    enabled: true,
    rule: "gte",
    value: 50.01,
  },
};
