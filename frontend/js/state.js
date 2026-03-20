// state.js
let PALETTE = { ...DEFAULT_PALETTE };
let SERIES_VISIBILITY = { ...DEFAULT_SERIES_VISIBILITY };

let pollProgressTimer = null;
let pollProgressStartedAt = Date.now();

let yCfg = JSON.parse(JSON.stringify(DEFAULT_Y_CFG));
var transitManageCfg = JSON.parse(JSON.stringify(DEFAULT_TRANSIT_MANAGE_CFG));

let unitsCache = [];

/**
 * Registro global de instâncias de gráfico.
 * - live: gráfico principal em tempo real
 * - replay cards: gráficos adicionais de comparação
 */
let chartInstances = [];
let chartInstanceSeq = 0;
let activeChartInstanceId = null;

/**
 * Factory de estado por instância.
 * Tudo que antes era global e pertencia ao gráfico agora mora aqui.
 */
function createChartState(overrides = {}) {
  return {
    id: overrides.id || `chart-instance-${++chartInstanceSeq}`,
    mode: overrides.mode || "live", // live | replay
    title: overrides.title || "",
    fileName: overrides.fileName || "",

    samples: Array.isArray(overrides.samples) ? overrides.samples : [],
    lastConsumidoRaw:
      overrides.lastConsumidoRaw !== undefined ? overrides.lastConsumidoRaw : null,
    lastMsgTm: overrides.lastMsgTm !== undefined ? overrides.lastMsgTm : null,

    commentsByMsgTm: overrides.commentsByMsgTm
      ? { ...overrides.commentsByMsgTm }
      : {},

    selectedIdx:
      overrides.selectedIdx !== undefined ? overrides.selectedIdx : null,
    selectedMsgTm:
      overrides.selectedMsgTm !== undefined ? overrides.selectedMsgTm : null,

    autoScrollToEnd:
      overrides.autoScrollToEnd !== undefined ? !!overrides.autoScrollToEnd : true,

    HIT: overrides.HIT || null,
    HOVER: overrides.HOVER || {
      idx: null,
      x: null,
      y: null,
      seriesKey: null,
    },

    replayRows: Array.isArray(overrides.replayRows) ? overrides.replayRows : [],
    replayIndex:
      overrides.replayIndex !== undefined ? overrides.replayIndex : 0,
    replayTimer: overrides.replayTimer || null,

    isDrawing: !!overrides.isDrawing,
    lastScrollEventAt:
      overrides.lastScrollEventAt !== undefined ? overrides.lastScrollEventAt : 0,
    pointerDownInfo: overrides.pointerDownInfo || null,

    dom: {
      wrap: overrides.dom?.wrap || null,
      canvas: overrides.dom?.canvas || null,
      tip: overrides.dom?.tip || null,
      meta: overrides.dom?.meta || null,
      popover: overrides.dom?.popover || null,
      commentInput: overrides.dom?.commentInput || null,
      commentTimeLabel: overrides.dom?.commentTimeLabel || null,
      status: overrides.dom?.status || null,
      current: overrides.dom?.current || null,
    },
  };
}

function registerChartInstance(instance) {
  if (!instance || !instance.id) return null;

  const existingIdx = chartInstances.findIndex((it) => it.id === instance.id);
  if (existingIdx >= 0) {
    chartInstances[existingIdx] = instance;
  } else {
    chartInstances.push(instance);
  }

  if (!activeChartInstanceId) {
    activeChartInstanceId = instance.id;
  }

  return instance;
}

function unregisterChartInstance(instanceId) {
  const idx = chartInstances.findIndex((it) => it.id === instanceId);
  if (idx === -1) return;

  const instance = chartInstances[idx];
  if (instance?.state?.replayTimer) {
    clearInterval(instance.state.replayTimer);
    instance.state.replayTimer = null;
  }

  chartInstances.splice(idx, 1);

  if (activeChartInstanceId === instanceId) {
    activeChartInstanceId = chartInstances.length
      ? chartInstances[chartInstances.length - 1].id
      : null;
  }
}

function getChartInstanceById(instanceId) {
  return chartInstances.find((it) => it.id === instanceId) || null;
}

function getActiveChartInstance() {
  if (activeChartInstanceId) {
    const active = getChartInstanceById(activeChartInstanceId);
    if (active) return active;
  }

  return chartInstances.length ? chartInstances[0] : null;
}

function setActiveChartInstance(instanceId) {
  if (!instanceId) return;
  const exists = getChartInstanceById(instanceId);
  if (exists) activeChartInstanceId = instanceId;
}

function getLiveChartInstance() {
  return chartInstances.find((it) => it.state?.mode === "live") || null;
}

function getReplayChartInstances() {
  return chartInstances.filter((it) => it.state?.mode === "replay");
}

function destroyAllReplayInstances() {
  const replayInstances = getReplayChartInstances();

  replayInstances.forEach((instance) => {
    if (instance?.destroy) {
      instance.destroy();
    } else {
      unregisterChartInstance(instance.id);
    }
  });
}

function bindDomToState(state, domRefs = {}) {
  if (!state) return;

  state.dom.wrap = domRefs.wrap || null;
  state.dom.canvas = domRefs.canvas || null;
  state.dom.tip = domRefs.tip || null;
  state.dom.meta = domRefs.meta || null;
  state.dom.popover = domRefs.popover || null;
  state.dom.commentInput = domRefs.commentInput || null;
  state.dom.commentTimeLabel = domRefs.commentTimeLabel || null;
  state.dom.status = domRefs.status || null;
  state.dom.current = domRefs.current || null;
}

function clearInstanceReplayTimer(state) {
  if (!state?.replayTimer) return;
  clearInterval(state.replayTimer);
  state.replayTimer = null;
}

function resetInstanceHover(state) {
  if (!state) return;
  state.HOVER = { idx: null, x: null, y: null, seriesKey: null };
}

function resetInstanceSelection(state) {
  if (!state) return;
  state.selectedIdx = null;
  state.selectedMsgTm = null;
}

function resetChartStateForInstance(state) {
  if (!state) return;

  state.samples = [];
  state.lastConsumidoRaw = null;
  state.lastMsgTm = null;
  state.HIT = null;
  resetInstanceHover(state);
  resetInstanceSelection(state);
}

function resetReplayStateForInstance(state) {
  if (!state) return;

  state.replayRows = [];
  state.replayIndex = 0;
  clearInstanceReplayTimer(state);
}

function resetInteractiveStateForInstance(state) {
  if (!state) return;

  state.commentsByMsgTm = {};
  resetInstanceSelection(state);
  resetInstanceHover(state);
  state.autoScrollToEnd = true;
  state.isDrawing = false;
  state.lastScrollEventAt = 0;
  state.pointerDownInfo = null;
}

function resetFullInstanceState(state) {
  if (!state) return;

  resetChartStateForInstance(state);
  resetReplayStateForInstance(state);
  resetInteractiveStateForInstance(state);
}

function getSeriesForMode(mode) {
  return mode === "replay"
    ? [...BASE_SERIES, ...REPLAY_EXTRA_SERIES]
    : [...BASE_SERIES];
}

function getAllSeries(instanceOrMode = null) {
  if (typeof instanceOrMode === "string") {
    return getSeriesForMode(instanceOrMode);
  }

  if (instanceOrMode?.state?.mode) {
    return getSeriesForMode(instanceOrMode.state.mode);
  }

  if (instanceOrMode?.mode) {
    return getSeriesForMode(instanceOrMode.mode);
  }

  const active = getActiveChartInstance();
  return getSeriesForMode(active?.state?.mode || "live");
}

function getVisibleSeries(instanceOrMode = null) {
  return getAllSeries(instanceOrMode)
    .map((s) => ({ ...s, color: PALETTE[s.key] }))
    .filter((serie) => SERIES_VISIBILITY[serie.key]);
}

function getAllSeriesWithColors(instanceOrMode = null) {
  return getAllSeries(instanceOrMode).map((s) => ({
    ...s,
    color: PALETTE[s.key],
  }));
}

/**
 * Helpers de compatibilidade temporária.
 * Ainda existem partes antigas do sistema esperando essas funções.
 */
function resetChartState() {
  const active = getActiveChartInstance();
  if (!active) return;
  resetChartStateForInstance(active.state);
}

function resetReplayCollections() {
  const active = getActiveChartInstance();
  if (!active) return;
  resetReplayStateForInstance(active.state);
}

/**
 * Utilitários de importação do replay
 */
function normalizeHeaderName(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/^﻿/, "")
    .trim()
    .toLowerCase();
}

const IMPORT_HEADER_ALIASES = {
  id: ["1 a 0"],
  tm: ["Hora"],
  velocidade: ["Velocidade*"],
  altitude: ["Altitude*"],
  pct_acelerado: ["Acelerador*"],
  rpm: ["RPM*"],
  motor: ["Motor*"],
  temperatura_motor: ["Temperatura do Motor*"],
  ar_cond: ["Ar Condicionado*"],
  freio: ["Freio*"],
  peso_total: ["Peso Total*"],
  consumido_raw: ["Consumido*"],
  arla_raw: ["Arla*"],
  consumido_delta_raw: ["Consumido Delta*"],
  comment: [],
  odometro: ["Odômetro*"],
};

function buildHeaderIndexMap(headerRow) {
  const map = {};

  headerRow.forEach((cell, idx) => {
    const normalized = normalizeHeaderName(cell);
    if (normalized) map[normalized] = idx;
  });

  return map;
}

function resolveImportCols(headerIndexMap) {
  const resolved = {};

  for (const [field, aliases] of Object.entries(IMPORT_HEADER_ALIASES)) {
    let foundIndex = null;

    for (const alias of aliases) {
      const normalizedAlias = normalizeHeaderName(alias);
      if (headerIndexMap[normalizedAlias] != null) {
        foundIndex = headerIndexMap[normalizedAlias];
        break;
      }
    }

    resolved[field] = foundIndex;
  }

  return resolved;
}

function findReplayHeaderRow(rows) {
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (!Array.isArray(row)) continue;

    const headerMap = buildHeaderIndexMap(row);
    const hasHora = headerMap[normalizeHeaderName("Hora")] != null;

    if (hasHora) return i;
  }

  return -1;
}

function getCell(row, idx) {
  if (idx == null || idx < 0) return "";
  return row[idx];
}

/**
 * Instância principal viva.
 * Os próximos arquivos vão usar isso para manter o gráfico atual funcionando
 * e também permitir cards adicionais de replay.
 */
const MAIN_CHART_STATE = createChartState({
  id: "main-live-chart",
  mode: "live",
  title: "Gráfico principal",
});

let mainChartInstance = {
  id: MAIN_CHART_STATE.id,
  state: MAIN_CHART_STATE,
};

registerChartInstance(mainChartInstance);
setActiveChartInstance(MAIN_CHART_STATE.id);