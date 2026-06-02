(function () {
  const POLL_INTERVAL_MS = 1500;
  const POLL_TIMEOUT_MS = 30 * 60 * 1000;
  const FRONTEND_BASE = (window.__FRONTEND_BASE__ || window.location.origin).replace(/\/$/, "");
  const BACKEND_BASE = (
    window.__BACKEND_BASE__ ||
    window.__API_BASE__ ||
    window.location.origin
  ).replace(/\/$/, "");

  const state = {
    originalFileName: "",
    exportFileName: "",
    sheetName: "",
    headers: [],
    rows: [],
    totalRows: 0,
    jobId: "",
    currentFile: null,
    analysisApplied: false,
    isProcessing: false,
    processingStartedAt: null,
    progressRowCount: 0,
    activePollToken: 0
  };

  function apiUrl(path) {
    return `${BACKEND_BASE}${path.startsWith("/") ? path : `/${path}`}`;
  }

  function frontendUrl(path) {
    return `${FRONTEND_BASE}${path.startsWith("/") ? path : `/${path}`}`;
  }

  function previewApiBody(rawText) {
    return String(rawText ?? "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 180);
  }

  async function parseApiResponse(response) {
    const rawText = await response.text();
    const contentType = (response.headers.get("Content-Type") || "").toLowerCase();
    const bodyPreview = previewApiBody(rawText);

    if (!contentType.includes("application/json")) {
      throw new Error(
        `Resposta inválida do servidor (${response.status}). Esperado JSON, recebido: ${bodyPreview || "[vazio]"}`
      );
    }

    try {
      return JSON.parse(rawText);
    } catch (error) {
      throw new Error(
        `JSON inválido retornado pela API (${response.status}). Retorno: ${bodyPreview || "[vazio]"}`
      );
    }
  }

  const els = {
    input: document.getElementById("treatmentFileInput"),
    btnSelect: document.getElementById("btnSelectFile"),
    btnAddAnalysis: document.getElementById("btnAddAnalysisColumns"),
    btnExport: document.getElementById("btnExportFile"),
    btnClear: document.getElementById("btnClearFile"),
    fileName: document.getElementById("fileNameValue"),
    rowCount: document.getElementById("rowCountValue"),
    columnCount: document.getElementById("columnCountValue"),
    meta: document.getElementById("treatmentMeta"),
    badge: document.getElementById("statusBadge"),
    previewInfo: document.getElementById("previewInfo"),
    head: document.getElementById("previewHead"),
    body: document.getElementById("previewBody"),
    previewTable: document.getElementById("previewTable"),
    tableWrap: document.getElementById("treatmentTableWrap"),
    topScroll: document.getElementById("treatmentTopScroll"),
    topScrollInner: document.getElementById("treatmentTopScrollInner"),
    configsRoot: document.getElementById("treatment-configs-root")
  };

  if (
    !els.input ||
    !els.btnSelect ||
    !els.btnAddAnalysis ||
    !els.btnExport ||
    !els.btnClear ||
    !els.fileName ||
    !els.rowCount ||
    !els.columnCount ||
    !els.meta ||
    !els.badge ||
    !els.previewInfo ||
    !els.head ||
    !els.body ||
    !els.previewTable
  ) {
    console.error("Treatment: elementos da página não encontrados.");
    return;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function updateStatus(text, type) {
    els.badge.className = "badge";

    if (type === "ok") {
      els.badge.classList.add("badge-ok");
    } else if (type === "warn") {
      els.badge.classList.add("badge-warn");
    } else {
      els.badge.classList.add("badge-bad");
    }

    els.badge.innerHTML = `<span class="badge-dot"></span>${escapeHtml(text)}`;
  }

  function updateTopScrollbarWidth() {
    if (!els.topScrollInner || !els.previewTable) return;
    els.topScrollInner.style.width = `${els.previewTable.scrollWidth}px`;
  }

  function clearTable() {
    els.head.innerHTML = `
      <tr>
        <th>Tabela vazia</th>
      </tr>
    `;

    els.body.innerHTML = `
      <tr>
        <td class="table-empty">Carregue um arquivo para visualizar os dados tratados.</td>
      </tr>
    `;

    els.previewInfo.textContent = "A tabela aparecerá aqui depois do upload.";
    requestAnimationFrame(updateTopScrollbarWidth);
  }

  function getInputNumber(id, fallback) {
    const el = document.getElementById(id);
    if (!el) return fallback;

    const raw = String(el.value ?? "").replace(",", ".");
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
  }

  function getBackendTreatmentConfig() {
    return {
      bestRpmMin: getInputNumber("bestRpmMin", 1100),
      bestRpmMax: getInputNumber("bestRpmMax", 1900),
      speedLowMax: getInputNumber("speedLowMax", 10),
      speedMediumMax: getInputNumber("speedMediumMax", 15),
      rpmUsefulStart: getInputNumber("rpmIntenseLow", 900),
      rpmLightStart: getInputNumber("rpmLightMin", 1100),
      rpmUsefulEnd: getInputNumber("rpmLightMax", 1900),
      accelLightMax: getInputNumber("accelLightMax", 30),
      accelMediumMax: getInputNumber("accelMediumMax", 60),
      brakeMediumMin: getInputNumber("brakeMediumMin", 2),
      brakeIntenseMin: getInputNumber("brakeIntenseMin", 4)
    };
  }

  function byId(id) {
    return document.getElementById(id);
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function roundToStep(value, step) {
    const precision = String(step).includes(".")
      ? String(step).split(".")[1].length
      : 0;
    return Number((Math.round(value / step) * step).toFixed(precision));
  }

  function parseConfigNumber(id, fallback) {
    const el = byId(id);
    if (!el) return fallback;
    const value = Number(String(el.value ?? "").replace(",", "."));
    return Number.isFinite(value) ? value : fallback;
  }

  function setInputValue(id, value, decimals = null) {
    const el = byId(id);
    if (!el) return;
    el.value = decimals === null ? String(value) : Number(value).toFixed(decimals);
  }

  function setText(id, value) {
    const el = byId(id);
    if (el) el.textContent = String(value);
  }

  function formatNumber(value, decimals = 0) {
    return Number(value).toLocaleString("pt-BR", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals
    });
  }

  function formatSmart(value, maxDecimals = 2) {
    return Number(value).toLocaleString("pt-BR", {
      minimumFractionDigits: 0,
      maximumFractionDigits: maxDecimals
    });
  }

  function pct(value, max) {
    return `${clamp((value / max) * 100, 0, 100)}%`;
  }

  function updateRangePart(id, leftValue, rightValue, max) {
    const el = byId(id);
    if (!el) return;
    const left = clamp((leftValue / max) * 100, 0, 100);
    const right = clamp((rightValue / max) * 100, 0, 100);
    el.style.left = `${left}%`;
    el.style.width = `${Math.max(0, right - left)}%`;
  }

  function positionHandle(id, value, max) {
    const el = byId(id);
    if (el) el.style.left = pct(value, max);
  }

  function valueFromPointer(event, track, max, step) {
    const rect = track.getBoundingClientRect();
    const ratio = rect.width ? (event.clientX - rect.left) / rect.width : 0;
    return roundToStep(clamp(ratio, 0, 1) * max, step);
  }

  function bindDrag(trackId, handleId, onValue) {
    const track = byId(trackId);
    const handle = byId(handleId);
    if (!track || !handle) return;

    const move = (event) => {
      event.preventDefault();
      onValue(event, track);
    };

    const stop = () => {
      handle.classList.remove("is-active");
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
      window.removeEventListener("pointercancel", stop);
    };

    handle.addEventListener("pointerdown", (event) => {
      handle.classList.add("is-active");
      handle.setPointerCapture?.(event.pointerId);
      move(event);
      window.addEventListener("pointermove", move);
      window.addEventListener("pointerup", stop, { once: true });
      window.addEventListener("pointercancel", stop, { once: true });
    });
  }

  function initSpeedConfig() {
    const lowInput = byId("speedLowMax");
    const mediumInput = byId("speedMediumMax");
    if (!lowInput || !mediumInput || !byId("speedRangeSlider")) return;

    const max = 30;
    const step = 0.1;

    const render = () => {
      let low = clamp(parseConfigNumber("speedLowMax", 10), 0, max);
      let medium = clamp(parseConfigNumber("speedMediumMax", 15), 0, max);
      if (low > medium) low = medium;

      setInputValue("speedLowMax", low);
      setInputValue("speedMediumMax", medium);
      setInputValue("speedHighMin", medium);

      positionHandle("speedLowHandle", low, max);
      positionHandle("speedMediumHandle", medium, max);
      updateRangePart("speedSegLow", 0, low, max);
      updateRangePart("speedSegMedium", low, medium, max);
      updateRangePart("speedSegHigh", medium, max, max);

      setText("speedLowPreview", formatSmart(low, 1));
      setText("speedMediumFromPreview", formatSmart(low, 1));
      setText("speedMediumToPreview", formatSmart(medium, 1));
      setText("speedHighLegendPreview", formatSmart(medium, 1));
      setText("speedHighPreview", formatSmart(medium, 1));
    };

    lowInput.addEventListener("input", render);
    mediumInput.addEventListener("input", render);
    bindDrag("speedRangeSlider", "speedLowHandle", (event, track) => {
      const medium = parseConfigNumber("speedMediumMax", 15);
      setInputValue("speedLowMax", Math.min(valueFromPointer(event, track, max, step), medium));
      render();
    });
    bindDrag("speedRangeSlider", "speedMediumHandle", (event, track) => {
      const low = parseConfigNumber("speedLowMax", 10);
      setInputValue("speedMediumMax", Math.max(valueFromPointer(event, track, max, step), low));
      render();
    });
    render();
  }

  function initRpmTransitConfig() {
    const startInput = byId("rpmIntenseLow");
    const middleInput = byId("rpmLightMin");
    const endInput = byId("rpmLightMax");
    if (!startInput || !middleInput || !endInput || !byId("rpmTransitSlider")) return;

    const max = 3000;
    const step = 1;

    const render = () => {
      let start = clamp(parseConfigNumber("rpmIntenseLow", 900), 0, max);
      let middle = clamp(parseConfigNumber("rpmLightMin", 1100), 0, max);
      let end = clamp(parseConfigNumber("rpmLightMax", 1900), 0, max);
      middle = clamp(middle, start, end);
      start = Math.min(start, middle);
      end = Math.max(end, middle);

      setInputValue("rpmIntenseLow", Math.round(start));
      setInputValue("rpmLightMin", Math.round(middle));
      setInputValue("rpmLightMax", Math.round(end));
      setInputValue("rpmIntenseHigh", Math.round(end));
      setInputValue("rpmMediumMin", Math.round(start));
      setInputValue("rpmMediumMax", Math.max(Math.round(middle) - 1, Math.round(start)));

      positionHandle("rpmTransitHandleStart", start, max);
      positionHandle("rpmTransitHandleMiddle", middle, max);
      positionHandle("rpmTransitHandleEnd", end, max);
      updateRangePart("rpmTransitSegIntenseLow", 0, start, max);
      updateRangePart("rpmTransitSegMedium", start, middle, max);
      updateRangePart("rpmTransitSegLight", middle, end, max);
      updateRangePart("rpmTransitSegIntenseHigh", end, max, max);

      setText("rpmIntenseLowPreview", Math.round(start));
      setText("rpmIntenseHighPreview", Math.round(end));
      setText("rpmMediumMinPreview", Math.round(start));
      setText("rpmMediumMaxPreview", Math.max(Math.round(middle) - 1, Math.round(start)));
      setText("rpmLightMinPreview", Math.round(middle));
      setText("rpmLightMaxPreview", Math.round(end));
    };

    [startInput, middleInput, endInput].forEach((input) => input.addEventListener("input", render));
    bindDrag("rpmTransitSlider", "rpmTransitHandleStart", (event, track) => {
      const middle = parseConfigNumber("rpmLightMin", 1100);
      setInputValue("rpmIntenseLow", Math.min(valueFromPointer(event, track, max, step), middle));
      render();
    });
    bindDrag("rpmTransitSlider", "rpmTransitHandleMiddle", (event, track) => {
      const start = parseConfigNumber("rpmIntenseLow", 900);
      const end = parseConfigNumber("rpmLightMax", 1900);
      setInputValue("rpmLightMin", clamp(valueFromPointer(event, track, max, step), start, end));
      render();
    });
    bindDrag("rpmTransitSlider", "rpmTransitHandleEnd", (event, track) => {
      const middle = parseConfigNumber("rpmLightMin", 1100);
      setInputValue("rpmLightMax", Math.max(valueFromPointer(event, track, max, step), middle));
      render();
    });
    render();
  }

  function initBrakeConfig() {
    const mediumInput = byId("brakeMediumMin");
    const intenseInput = byId("brakeIntenseMin");
    if (!mediumInput || !intenseInput || !byId("brakeRangeSlider")) return;

    const max = 8;
    const step = 0.01;

    const render = () => {
      let medium = clamp(parseConfigNumber("brakeMediumMin", 2), 0, max);
      let intense = clamp(parseConfigNumber("brakeIntenseMin", 4), 0, max);
      if (medium > intense) medium = intense;
      const lightMax = Math.max(0, medium - step);
      const mediumMax = Math.max(medium, intense - step);

      setInputValue("brakeMediumMin", medium, 2);
      setInputValue("brakeIntenseMin", intense, 2);
      setInputValue("brakeLightMax", lightMax, 2);
      setInputValue("brakeMediumMax", mediumMax, 2);

      positionHandle("brakeMediumHandle", medium, max);
      positionHandle("brakeIntenseHandle", intense, max);
      updateRangePart("brakeSegLight", 0, medium, max);
      updateRangePart("brakeSegMedium", medium, intense, max);
      updateRangePart("brakeSegIntense", intense, max, max);

      setText("brakeLightMaxPreview", formatNumber(lightMax, 2));
      setText("brakeLightMaxValuePreview", formatNumber(lightMax, 2));
      setText("brakeMediumMinPreview", formatSmart(medium, 2));
      setText("brakeMediumMaxPreview", formatNumber(mediumMax, 2));
      setText("brakeMediumMaxValuePreview", formatNumber(mediumMax, 2));
      setText("brakeIntenseMinPreview", formatSmart(intense, 2));
    };

    mediumInput.addEventListener("input", render);
    intenseInput.addEventListener("input", render);
    bindDrag("brakeRangeSlider", "brakeMediumHandle", (event, track) => {
      const intense = parseConfigNumber("brakeIntenseMin", 4);
      setInputValue("brakeMediumMin", Math.min(valueFromPointer(event, track, max, step), intense), 2);
      render();
    });
    bindDrag("brakeRangeSlider", "brakeIntenseHandle", (event, track) => {
      const medium = parseConfigNumber("brakeMediumMin", 2);
      setInputValue("brakeIntenseMin", Math.max(valueFromPointer(event, track, max, step), medium), 2);
      render();
    });
    render();
  }

  function initAccelConfig() {
    const minInput = byId("accelMediumMin");
    const maxInput = byId("accelMediumMax");
    if (!minInput || !maxInput || !byId("accelRangeSlider")) return;

    const max = 100;
    const step = 0.01;

    const render = () => {
      let min = clamp(parseConfigNumber("accelMediumMin", 30), 0, max - step);
      let mediumMax = clamp(parseConfigNumber("accelMediumMax", 60), step, max - step);
      if (min > mediumMax) min = mediumMax;
      const intenseMin = Math.min(max, mediumMax + step);

      setInputValue("accelMediumMin", min, 2);
      setInputValue("accelMediumMax", mediumMax, 2);
      setInputValue("accelLightMax", min, 2);
      setInputValue("accelIntenseMin", intenseMin, 2);

      positionHandle("accelMediumMinHandle", min, max);
      positionHandle("accelMediumMaxHandle", mediumMax, max);
      updateRangePart("accelSegLight", 0, min, max);
      updateRangePart("accelSegMedium", min, mediumMax, max);
      updateRangePart("accelSegIntense", mediumMax, max, max);

      setText("accelLightMaxPreview", formatSmart(min, 2));
      setText("accelLightMaxValuePreview", formatSmart(min, 2));
      setText("accelMediumMinPreview", formatSmart(min, 2));
      setText("accelMediumMaxPreview", formatSmart(mediumMax, 2));
      setText("accelIntenseMinPreview", formatNumber(intenseMin, 2));
      setText("accelIntenseMinValuePreview", formatNumber(intenseMin, 2));
    };

    minInput.addEventListener("input", render);
    maxInput.addEventListener("input", render);
    bindDrag("accelRangeSlider", "accelMediumMinHandle", (event, track) => {
      const mediumMax = parseConfigNumber("accelMediumMax", 60);
      setInputValue("accelMediumMin", Math.min(valueFromPointer(event, track, max, step), mediumMax), 2);
      render();
    });
    bindDrag("accelRangeSlider", "accelMediumMaxHandle", (event, track) => {
      const min = parseConfigNumber("accelMediumMin", 30);
      setInputValue("accelMediumMax", Math.max(valueFromPointer(event, track, max, step), min), 2);
      render();
    });
    render();
  }

  function initBestRpmConfig() {
    const minRange = byId("bestRpmMinRange");
    const maxRange = byId("bestRpmMaxRange");
    const minInput = byId("bestRpmMin");
    const maxInput = byId("bestRpmMax");
    const fill = byId("bestRpmTrackFill");
    if (!minRange || !maxRange || !minInput || !maxInput || !fill) return;

    const max = 3000;

    const render = () => {
      let min = clamp(parseConfigNumber("bestRpmMin", 1100), 0, max);
      let rpmMax = clamp(parseConfigNumber("bestRpmMax", 1900), 0, max);
      if (min > rpmMax) min = rpmMax;

      setInputValue("bestRpmMin", Math.round(min));
      setInputValue("bestRpmMax", Math.round(rpmMax));
      minRange.value = String(Math.round(min));
      maxRange.value = String(Math.round(rpmMax));

      fill.style.left = pct(min, max);
      fill.style.width = pct(rpmMax - min, max);
    };

    minRange.addEventListener("input", () => {
      setInputValue("bestRpmMin", Math.min(Number(minRange.value), parseConfigNumber("bestRpmMax", 1900)));
      render();
    });
    maxRange.addEventListener("input", () => {
      setInputValue("bestRpmMax", Math.max(Number(maxRange.value), parseConfigNumber("bestRpmMin", 1100)));
      render();
    });
    minInput.addEventListener("input", render);
    maxInput.addEventListener("input", render);
    render();
  }

  function initTreatmentConfigControls() {
    initSpeedConfig();
    initRpmTransitConfig();
    initBrakeConfig();
    initAccelConfig();
    initBestRpmConfig();
  }

  function setButtonState() {
    els.btnSelect.disabled = state.isProcessing;
    els.btnAddAnalysis.disabled = state.isProcessing || !state.currentFile;
    els.btnAddAnalysis.textContent = state.analysisApplied
      ? "Reaplicar colunas de análise"
      : "Adicionar colunas de análise";
    els.btnExport.disabled = state.isProcessing || !state.jobId;
    els.btnClear.disabled = false;
  }

  function resetState() {
    state.originalFileName = "";
    state.exportFileName = "";
    state.sheetName = "";
    state.headers = [];
    state.rows = [];
    state.totalRows = 0;
    state.jobId = "";
    state.currentFile = null;
    state.analysisApplied = false;
    state.isProcessing = false;
    state.processingStartedAt = null;
    state.progressRowCount = 0;
    state.activePollToken += 1;

    els.fileName.textContent = "-";
    els.rowCount.textContent = "0";
    els.columnCount.textContent = "0";
    els.meta.textContent = "Nenhum arquivo processado.";

    updateStatus("Aguardando arquivo", "warn");
    clearTable();
    setButtonState();
  }

  function renderTable(headers, rows, totalRows = rows.length, label = "linhas tratadas") {
    if (!headers.length || !rows.length) {
      clearTable();
      return;
    }

    const previewLimit = 200;
    const previewRows = rows.slice(0, previewLimit);

    els.head.innerHTML = `
      <tr>
        ${headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("")}
      </tr>
    `;

    els.body.innerHTML = previewRows
      .map((row) => `
        <tr>
          ${headers.map((header) => `<td>${escapeHtml(row[header])}</td>`).join("")}
        </tr>
      `)
      .join("");

    els.previewInfo.textContent =
      totalRows > previewLimit
        ? `Mostrando ${Math.min(previewRows.length, previewLimit)} de ${totalRows} ${label}.`
        : `Mostrando ${totalRows} ${label}.`;

    requestAnimationFrame(updateTopScrollbarWidth);
  }

  async function loadTreatmentConfigs() {
    if (!els.configsRoot) return;

    try {
      const response = await fetch(frontendUrl("/components/treatment/configs"), {
        cache: "no-store"
      });

      if (!response.ok) {
        throw new Error("Não foi possível carregar os parâmetros do treatment.");
      }

      els.configsRoot.innerHTML = await response.text();
      initTreatmentConfigControls();
    } catch (error) {
      console.error("Erro ao carregar configs do treatment:", error);
    }
  }

  async function createTreatmentJob(path, file, extraHeaders = {}) {
    const response = await fetch(apiUrl(path), {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
        "X-Filename": encodeURIComponent(file.name),
        ...extraHeaders
      },
      body: file
    });

    const result = await parseApiResponse(response);

    if (!response.ok || !result.ok || !result.job_id) {
      throw new Error(result?.error || "Erro ao criar job de processamento.");
    }

    return result;
  }

  async function fetchTreatmentStatus(jobId) {
    const response = await fetch(apiUrl(`/treatment_status?job_id=${encodeURIComponent(jobId)}`), {
      cache: "no-store"
    });
    const result = await parseApiResponse(response);

    if (!response.ok || !result.ok) {
      throw new Error(result?.error || "Erro ao consultar status do processamento.");
    }

    return result;
  }

  function describeProgress(progress, fallbackMessage) {
    const message = progress?.message || fallbackMessage;
    const current = progress?.current;
    const total = progress?.total;
    const totalIsEstimate = Boolean(progress?.total_is_estimate);
    const parts = [message];

    if (typeof current === "number" && typeof total === "number" && total > 0) {
      const percent = Math.max(0, Math.min(100, Math.round((current / total) * 100)));
      parts.push(`${percent}%`);
      if (totalIsEstimate) {
        parts.push("(estimado)");
      }
    } else if (typeof current === "number") {
      parts.push(`(${current.toLocaleString("pt-BR")} linhas)`);
    }

    if (typeof state.processingStartedAt === "number") {
      const elapsedSeconds = Math.max(0, Math.floor((Date.now() - state.processingStartedAt) / 1000));
      const minutes = Math.floor(elapsedSeconds / 60);
      const seconds = elapsedSeconds % 60;
      parts.push(`Tempo decorrido: ${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`);
    }

    return parts.join(" ");
  }

  function applyProgressState(statusPayload) {
    const progress = statusPayload?.progress || {};
    const phase = progress.phase || "";
    const message = describeProgress(progress, "Processando arquivo...");
    const current = progress?.current;

    if (phase === "upload_received") {
      updateStatus("Arquivo enviado", "warn");
    } else if (phase === "parsing_container") {
      updateStatus("Lendo estrutura", "warn");
    } else if (phase === "reading_shared_strings") {
      updateStatus("Lendo textos", "warn");
    } else if (phase === "reading_trips") {
      updateStatus("Lendo viagens", "warn");
    } else if (phase === "writing_output" || phase === "finalizing") {
      updateStatus("Finalizando arquivo", "warn");
    } else if (phase === "done") {
      updateStatus("Pronto para download", "ok");
    } else {
      updateStatus("Tratando linhas", "warn");
    }

    if (typeof current === "number") {
      state.progressRowCount = Math.max(state.progressRowCount || 0, current);
      els.rowCount.textContent = String(state.progressRowCount);
    }
    if (state.isProcessing) {
      els.columnCount.textContent = "-";
    }

    els.meta.textContent = message;
  }

  function applyResult(result, file, options = {}) {
    const label = options.previewLabel || "linhas tratadas";
    const tripsCount = Number(result.trips_count || 0);

    state.originalFileName = result.original_file_name || file.name;
    state.exportFileName = result.export_file_name || "";
    state.sheetName = result.sheet_name || "";
    state.headers = Array.isArray(result.preview_headers) ? result.preview_headers : [];
    state.rows = Array.isArray(result.preview_rows) ? result.preview_rows : [];
    state.totalRows = Number(result.row_count || 0);
    state.jobId = result.job_id || state.jobId;
    state.currentFile = file;
    state.analysisApplied = Boolean(options.analysisApplied);
    state.isProcessing = false;
    state.processingStartedAt = null;
    state.progressRowCount = 0;

    els.fileName.textContent = state.originalFileName;
    els.rowCount.textContent = String(state.totalRows);
    els.columnCount.textContent = String(result.column_count || state.headers.length);
    els.meta.textContent = state.analysisApplied
      ? `Planilha lida: ${state.sheetName}. Viagens encontradas: ${tripsCount}. Arquivo final: ${state.exportFileName}`
      : `Planilha lida: ${state.sheetName}. Arquivo final: ${state.exportFileName}`;

    renderTable(state.headers, state.rows, state.totalRows, label);
    updateStatus("Concluído", "ok");
    setButtonState();
  }

  async function pollTreatmentStatus(jobId, file, options = {}) {
    const pollToken = ++state.activePollToken;
    const startedAt = Date.now();

    while (pollToken === state.activePollToken) {
      const statusPayload = await fetchTreatmentStatus(jobId);

      if (pollToken !== state.activePollToken) {
        return;
      }

      if (statusPayload.status === "done") {
        applyResult(statusPayload.result || {}, file, options);
        return;
      }

      if (statusPayload.status === "error") {
        state.isProcessing = false;
        state.processingStartedAt = null;
        setButtonState();
        updateStatus("Falha", "bad");
        els.meta.textContent = statusPayload.error || "Falha no processamento.";
        throw new Error(statusPayload.error || "Falha no processamento.");
      }

      state.isProcessing = true;
      setButtonState();
      applyProgressState(statusPayload);

      if (Date.now() - startedAt > POLL_TIMEOUT_MS) {
        state.isProcessing = false;
        state.processingStartedAt = null;
        setButtonState();
        updateStatus("Falha", "bad");
        throw new Error("Tempo limite excedido ao aguardar o processamento.");
      }

      await new Promise((resolve) => window.setTimeout(resolve, POLL_INTERVAL_MS));
    }
  }

  async function startTreatmentJob(path, file, options = {}) {
    state.currentFile = file;
    state.jobId = "";
    state.analysisApplied = Boolean(options.analysisAppliedBeforeStart);
    state.isProcessing = true;
    state.processingStartedAt = Date.now();
    state.progressRowCount = 0;
    setButtonState();

    updateStatus("Arquivo enviado", "warn");
    els.fileName.textContent = file.name;
    els.rowCount.textContent = "0";
    els.columnCount.textContent = "-";
    els.meta.textContent = "Upload concluído. Preparando processamento...";

    const extraHeaders = options.extraHeaders || {};
    const created = await createTreatmentJob(path, file, extraHeaders);
    state.jobId = created.job_id;
    await pollTreatmentStatus(created.job_id, file, options);
  }

  async function handleBaseTreatment(file) {
    if (!file) return;

    await startTreatmentJob("/process_treatment", file, {
      analysisApplied: false,
      analysisAppliedBeforeStart: false,
      previewLabel: "linhas tratadas"
    });
  }

  async function handleStep1Analysis() {
    if (!state.currentFile) {
      alert("Carregue um arquivo antes de adicionar as colunas de análise.");
      return;
    }

    await startTreatmentJob("/process_treatment_step1", state.currentFile, {
      analysisApplied: true,
      analysisAppliedBeforeStart: true,
      previewLabel: "linhas com colunas de análise",
      extraHeaders: {
        "X-Treatment-Config": JSON.stringify(getBackendTreatmentConfig())
      }
    });
  }

  async function exportFile() {
    if (!state.jobId || state.isProcessing) {
      alert("O arquivo ainda não está pronto para exportação.");
      return;
    }

    try {
      updateStatus("Baixando", "warn");
      els.meta.textContent = "Validando arquivo final para download...";

      const response = await fetch(
        apiUrl(`/download_treatment_result?job_id=${encodeURIComponent(state.jobId)}`),
        { cache: "no-store" }
      );

      if (!response.ok) {
        let message = "NÃ£o foi possÃ­vel baixar o arquivo processado.";

        try {
          const errorPayload = await parseApiResponse(response);
          message = errorPayload?.error || message;
        } catch (error) {
          console.warn("Treatment: resposta de erro do download nÃ£o era JSON.", error);
        }

        throw new Error(message);
      }

      const contentDisposition = response.headers.get("Content-Disposition") || "";
      const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
      const simpleMatch = contentDisposition.match(/filename=\"?([^\";]+)\"?/i);
      let downloadName = state.exportFileName || "arquivo_tratado.xlsx";

      if (utf8Match?.[1]) {
        try {
          downloadName = decodeURIComponent(utf8Match[1]);
        } catch (error) {
          console.warn("Treatment: falha ao decodificar nome UTF-8 do download.", error);
        }
      } else if (simpleMatch?.[1]) {
        downloadName = simpleMatch[1];
      }

      const fileBlob = await response.blob();
      const objectUrl = window.URL.createObjectURL(fileBlob);
      const downloadLink = document.createElement("a");
      downloadLink.href = objectUrl;
      downloadLink.download = downloadName;
      document.body.appendChild(downloadLink);
      downloadLink.click();
      downloadLink.remove();
      window.setTimeout(() => window.URL.revokeObjectURL(objectUrl), 1000);

      updateStatus("ConcluÃ­do", "ok");
      els.meta.textContent = `Download iniciado: ${downloadName}`;
    } catch (error) {
      console.error("Treatment export:", error);
      updateStatus("Falha", "bad");
      els.meta.textContent = error?.message || "Erro ao baixar o arquivo processado.";
      alert(error?.message || "Erro ao baixar o arquivo processado.");
    }
  }

  function syncTopScrollbar() {
    if (!els.topScroll || !els.topScrollInner || !els.tableWrap) return;

    let syncingFromTop = false;
    let syncingFromBottom = false;

    els.topScroll.addEventListener("scroll", () => {
      if (syncingFromBottom) return;
      syncingFromTop = true;
      els.tableWrap.scrollLeft = els.topScroll.scrollLeft;
      syncingFromTop = false;
    });

    els.tableWrap.addEventListener("scroll", () => {
      if (syncingFromTop) return;
      syncingFromBottom = true;
      els.topScroll.scrollLeft = els.tableWrap.scrollLeft;
      syncingFromBottom = false;
    });

    window.addEventListener("resize", updateTopScrollbarWidth);
    updateTopScrollbarWidth();
  }

  function bindEvents() {
    els.btnSelect.addEventListener("click", () => {
      if (state.isProcessing) return;
      els.input.value = "";
      els.input.click();
    });

    els.input.addEventListener("change", async (event) => {
      const file = event.target.files?.[0];
      if (!file) return;

      try {
        await handleBaseTreatment(file);
      } catch (error) {
        console.error("Treatment:", error);
        state.isProcessing = false;
        state.processingStartedAt = null;
        setButtonState();
        updateStatus("Falha", "bad");
        els.meta.textContent = error?.message || "Erro ao processar o arquivo.";
        alert(error?.message || "Erro ao processar o arquivo.");
      }
    });

    els.btnAddAnalysis.addEventListener("click", async () => {
      try {
        await handleStep1Analysis();
      } catch (error) {
        console.error("Treatment step 1:", error);
        state.isProcessing = false;
        state.processingStartedAt = null;
        setButtonState();
        updateStatus("Falha", "bad");
        els.meta.textContent = error?.message || "Erro ao processar a análise.";
        alert(error?.message || "Erro ao processar a análise.");
      }
    });

    els.btnExport.addEventListener("click", exportFile);

    els.btnClear.addEventListener("click", () => {
      els.input.value = "";
      resetState();
    });
  }

  async function init() {
    await loadTreatmentConfigs();
    bindEvents();
    resetState();
    syncTopScrollbar();
  }

  init();
})();
