(function () {
  const REQUIRED_SHEET_NAME = "Mensagens";
  const ANALYSIS_EXPORT_SUFFIX = "_tratado_analise.xlsx";

  const ANALYSIS_HEADERS = [
    "dia-mês",
    "Faixa Verde",
    "Trânsito (Velocidade)",
    "Trânsito (RPM)",
    "Aceleração",
    "Frenagens",
    "Nota Rota",
    "Soma Pontuação",
    "Nota Motorista"
  ];

  const NUMERIC_COLUMNS = new Set([
    "tensao",
    "bateria",
    "temperatura ambiente",
    "odometro",
    "pressao p1e1",
    "temperatura p1e1",
    "pressao p2e1",
    "temperatura p2e1",
    "pressao p1e2",
    "temperatura p1e2",
    "pressao p2e2",
    "temperatura p2e2",
    "pressao p3e2",
    "temperatura p3e2",
    "pressao p4e2",
    "temperatura p4e2",
    "pressao p1e3",
    "temperatura p1e3",
    "pressao p2e3",
    "temperatura p2e3",
    "rpm",
    "carga do motor",
    "temperatura do motor",
    "velocidade",
    "velocidade_2",
    "rpm max",
    "acelerador",
    "nivel de combustivel",
    "consumo instantaneo",
    "consumido",
    "arla",
    "volante",
    "inercia",
    "aceleracao",
    "desaceleracao",
    "peso dianteiro",
    "pesoraseiro",
    "pesootal",
    "altitude"
  ]);

  const ROUND_2_COLUMNS = new Set([
    "tensao",
    "bateria",
    "temperatura ambiente",
    "velocidade",
    "velocidade_2",
    "rpm max",
    "carga do motor",
    "acelerador",
    "nivel de combustivel",
    "inercia",
    "aceleracao"
  ]);

  const state = {
    originalFileName: "",
    exportFileName: "",
    sheetName: "",
    baseHeaders: [],
    baseRows: [],
    analysisHeaders: [],
    analysisRows: [],
    analysisApplied: false,
    parseToken: 0
  };

  const els = {
    input: document.getElementById("treatmentFileInput"),
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
    topScrollInner: document.getElementById("treatmentTopScrollInner")
  };

  if (
    !els.input ||
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
    return;
  }

  function normalizeSpaces(value) {
    return String(value ?? "")
      .replace(/\u00A0/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function normalizeHeader(value, fallbackIndex) {
    const text = normalizeSpaces(value).replace(/\*/g, "");
    return text || `Coluna_${fallbackIndex + 1}`;
  }

  function normalizeKey(value) {
    return normalizeSpaces(value)
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\*/g, "")
      .toLowerCase();
  }

  function makeUniqueHeaders(headers) {
    const seen = new Map();

    return headers.map((header) => {
      const count = seen.get(header) || 0;
      seen.set(header, count + 1);
      return count === 0 ? header : `${header}_${count + 1}`;
    });
  }

  function isRowEmpty(row) {
    return !Array.isArray(row) || row.every((cell) => normalizeSpaces(cell) === "");
  }

  function excelSerialToDate(value) {
    if (!Number.isFinite(value)) return null;

    const utcDays = Math.floor(value - 25569);
    const utcValue = utcDays * 86400;
    const fractionalDay = value - Math.floor(value);
    const totalSeconds = Math.round(fractionalDay * 86400);

    const date = new Date((utcValue + totalSeconds) * 1000);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function parseBrazilDateTime(value) {
    if (value == null || value === "") return null;

    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }

    if (typeof value === "number") {
      return excelSerialToDate(value);
    }

    let text = normalizeSpaces(value);
    if (!text) return null;

    text = text.replace(/^(dom|seg|ter|qua|qui|quin|sex|s[áa]b)\.?\s+/i, "").trim();

    let match = text.match(
      /^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
    );

    if (match) {
      let [, dd, mm, yyyy, hh = "0", mi = "0", ss = "0"] = match;
      let year = Number(yyyy);

      if (yyyy.length === 2) {
        year += year >= 70 ? 1900 : 2000;
      }

      const date = new Date(
        year,
        Number(mm) - 1,
        Number(dd),
        Number(hh),
        Number(mi),
        Number(ss),
        0
      );

      return Number.isNaN(date.getTime()) ? null : date;
    }

    match = text.match(
      /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
    );

    if (match) {
      const [, yyyy, mm, dd, hh = "0", mi = "0", ss = "0"] = match;

      const date = new Date(
        Number(yyyy),
        Number(mm) - 1,
        Number(dd),
        Number(hh),
        Number(mi),
        Number(ss),
        0
      );

      return Number.isNaN(date.getTime()) ? null : date;
    }

    return null;
  }

  function formatDateTimeBR(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";

    const dd = String(date.getDate()).padStart(2, "0");
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const yyyy = date.getFullYear();
    const hh = String(date.getHours()).padStart(2, "0");
    const mi = String(date.getMinutes()).padStart(2, "0");
    const ss = String(date.getSeconds()).padStart(2, "0");

    return `${dd}/${mm}/${yyyy} ${hh}:${mi}:${ss}`;
  }

  function tokenToNumber(token) {
    if (!token) return null;

    let value = token.replace(/\u2212/g, "-").trim();
    if (!value) return null;

    const hasDot = value.includes(".");
    const hasComma = value.includes(",");

    if (hasDot && hasComma) {
      if (value.lastIndexOf(".") > value.lastIndexOf(",")) {
        value = value.replace(/,/g, "");
      } else {
        value = value.replace(/\./g, "").replace(",", ".");
      }
    } else if (hasComma) {
      const commaCount = (value.match(/,/g) || []).length;

      if (commaCount > 1) {
        const lastComma = value.lastIndexOf(",");
        value =
          value.slice(0, lastComma).replace(/,/g, "") +
          "." +
          value.slice(lastComma + 1);
      } else {
        value = value.replace(",", ".");
      }
    } else if (hasDot) {
      const dotCount = (value.match(/\./g) || []).length;

      if (dotCount > 1) {
        const lastDot = value.lastIndexOf(".");
        value =
          value.slice(0, lastDot).replace(/\./g, "") +
          "." +
          value.slice(lastDot + 1);
      }
    }

    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function toNumberMaybe(value) {
    if (value == null || value === "") return "";

    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }

    const original = normalizeSpaces(value);
    if (!original) return "";

    const candidates = original.match(/-?\d[\d.,]*/g);
    if (!candidates || !candidates.length) return original;

    const token = candidates.sort((a, b) => {
      const digitsA = (a.match(/\d/g) || []).length;
      const digitsB = (b.match(/\d/g) || []).length;
      if (digitsB !== digitsA) return digitsB - digitsA;
      return b.length - a.length;
    })[0];

    const num = tokenToNumber(token);
    return Number.isFinite(num) ? num : original;
  }

  function roundIfNeeded(value, key) {
    if (!Number.isFinite(value)) return value;
    if (!ROUND_2_COLUMNS.has(key)) return value;
    return Number(value.toFixed(2));
  }

  function findHeaderIndex(sheetRows) {
    const indexByHora = sheetRows.findIndex((row) => {
      if (!Array.isArray(row)) return false;
      return row.some((cell) => normalizeKey(cell) === "hora");
    });

    if (indexByHora >= 0) return indexByHora;

    return sheetRows.findIndex((row) => Array.isArray(row) && !isRowEmpty(row));
  }

  function processWorkbookRows(sheetRows) {
    if (!Array.isArray(sheetRows) || !sheetRows.length) {
      throw new Error("A planilha não possui dados válidos.");
    }

    const headerIndex = findHeaderIndex(sheetRows);

    if (headerIndex < 0) {
      throw new Error("Não foi possível localizar o cabeçalho.");
    }

    const headerRow = sheetRows[headerIndex] || [];
    const rawHeaders = headerRow.map((value, idx) => normalizeHeader(value, idx));
    const headers = makeUniqueHeaders(rawHeaders);
    const keyMap = headers.map((header) => normalizeKey(header));

    const dataRows = sheetRows.slice(headerIndex + 1).filter((row) => !isRowEmpty(row));

    const treatedRows = dataRows
      .map((row, rowIndex) => {
        const obj = {};
        let hasAnyData = false;

        headers.forEach((header, colIndex) => {
          const rawValue = row[colIndex];
          const key = keyMap[colIndex];

          if (key === "agrupamento") {
            return;
          }

          if (key === "hora") {
            const parsedDate = parseBrazilDateTime(rawValue);
            obj[header] = parsedDate ? formatDateTimeBR(parsedDate) : normalizeSpaces(rawValue);
            obj["Hora Unix"] = parsedDate ? Math.floor(parsedDate.getTime() / 1000) : "";

            if (obj[header] !== "" || obj["Hora Unix"] !== "") {
              hasAnyData = true;
            }
            return;
          }

          if (NUMERIC_COLUMNS.has(key)) {
            const numericValue = toNumberMaybe(rawValue);
            obj[header] =
              typeof numericValue === "number"
                ? roundIfNeeded(numericValue, key)
                : numericValue;

            if (obj[header] !== "") {
              hasAnyData = true;
            }
            return;
          }

          if (rawValue instanceof Date) {
            obj[header] = formatDateTimeBR(rawValue);
          } else {
            obj[header] = normalizeSpaces(rawValue);
          }

          if (obj[header] !== "") {
            hasAnyData = true;
          }
        });

        if (!hasAnyData) {
          return null;
        }

        obj["Linha Origem"] = headerIndex + rowIndex + 2;
        return obj;
      })
      .filter(Boolean);

    if (!treatedRows.length) {
      throw new Error("Nenhuma linha válida foi encontrada para tratamento.");
    }

    return {
      headers: Object.keys(treatedRows[0] || {}),
      rows: treatedRows
    };
  }

  function findRequiredSheetName(workbook) {
    if (!workbook || !Array.isArray(workbook.SheetNames)) return null;

    if (workbook.SheetNames.includes(REQUIRED_SHEET_NAME)) {
      return REQUIRED_SHEET_NAME;
    }

    const normalizedTarget = normalizeKey(REQUIRED_SHEET_NAME);
    return (
      workbook.SheetNames.find((name) => normalizeKey(name) === normalizedTarget) || null
    );
  }

  function getInputNumber(id, fallback) {
    const el = document.getElementById(id);
    if (!el) return fallback;

    const raw = String(el.value ?? "").replace(",", ".");
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
  }

  function getTreatmentConfigValues() {
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

  function findHeaderByAliases(headers, aliases) {
    const normalizedAliases = aliases.map((alias) => normalizeKey(alias));
    return headers.find((header) => normalizedAliases.includes(normalizeKey(header))) || null;
  }

  function extractNumericFromRow(row, headerNames) {
    for (const headerName of headerNames) {
      if (!headerName) continue;
      const parsed = toNumberMaybe(row[headerName]);
      if (typeof parsed === "number" && Number.isFinite(parsed)) {
        return parsed;
      }
    }
    return null;
  }

  function getRowDate(row, hourHeader, hourUnixHeader) {
    if (hourUnixHeader) {
      const unix = extractNumericFromRow(row, [hourUnixHeader]);
      if (typeof unix === "number" && Number.isFinite(unix)) {
        const date = new Date(unix * 1000);
        if (!Number.isNaN(date.getTime())) return date;
      }
    }

    if (hourHeader) {
      const parsed = parseBrazilDateTime(row[hourHeader]);
      if (parsed) return parsed;
    }

    return null;
  }

  function formatDayMonth(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
    const dd = String(date.getDate()).padStart(2, "0");
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    return `${dd}/${mm}`;
  }

  function classifyGreenBand(rpmValue, config) {
    if (!Number.isFinite(rpmValue)) return "";
    return rpmValue >= config.bestRpmMin && rpmValue <= config.bestRpmMax
      ? "Faixa Verde"
      : "Fora da Faixa";
  }

  function classifySpeedTransit(speedValue, config) {
    if (!Number.isFinite(speedValue)) return "";
    if (speedValue <= config.speedLowMax) return "Intenso";
    if (speedValue <= config.speedMediumMax) return "Médio";
    return "Leve";
  }

  function classifyRpmTransit(rpmValue, config) {
    if (!Number.isFinite(rpmValue)) return "";
    if (rpmValue < config.rpmUsefulStart || rpmValue > config.rpmUsefulEnd) return "Intenso";
    if (rpmValue < config.rpmLightStart) return "Médio";
    return "Leve";
  }

  function classifyAcceleration(accelValue, config) {
    if (!Number.isFinite(accelValue)) return "";
    if (accelValue <= config.accelLightMax) return "Leve";
    if (accelValue <= config.accelMediumMax) return "Média";
    return "Intensa";
  }

  function classifyBrake(brakeCount, config) {
    if (!Number.isFinite(brakeCount)) return "";
    if (brakeCount < config.brakeMediumMin) return "Leve";
    if (brakeCount < config.brakeIntenseMin) return "Média";
    return "Intensa";
  }

  function getRouteScore(speedTransitLabel) {
    if (speedTransitLabel === "Intenso") return 10;
    if (speedTransitLabel === "Médio" || speedTransitLabel === "Média") return 6;
    if (speedTransitLabel === "Leve") return 0;
    return "";
  }

  function isBrakeActivated(value) {
    if (value == null || value === "") return false;

    if (typeof value === "number") {
      return Number.isFinite(value) && value > 0;
    }

    const normalized = normalizeKey(value);
    if (!normalized) return false;

    if (["sim", "true", "on", "ativo", "acionado", "pressed"].includes(normalized)) {
      return true;
    }

    if (["nao", "não", "false", "off", "inativo", "desacionado"].includes(normalized)) {
      return false;
    }

    const numeric = tokenToNumber(normalized);
    return Number.isFinite(numeric) ? numeric > 0 : false;
  }

  function calculateDriverScore(sumScore, routeScore) {
    if (!Number.isFinite(sumScore)) return "";
    if (!Number.isFinite(routeScore) || routeScore === 0) return "100%";

    const percent = (sumScore / routeScore) * 100;
    return `${Math.round(percent)}%`;
  }

  function mergeHeadersWithAnalysis(headers) {
    const baseHeaders = headers.filter((header) => !ANALYSIS_HEADERS.includes(header));
    return [...baseHeaders, ...ANALYSIS_HEADERS];
  }

  function buildAnalysisRows(rows, headers) {
    const config = getTreatmentConfigValues();
    const hourHeader = findHeaderByAliases(headers, ["Hora"]);
    const hourUnixHeader = findHeaderByAliases(headers, ["Hora Unix", "HoraUnix"]);
    const rpmHeader = findHeaderByAliases(headers, ["RPM"]);
    const speedHeader = findHeaderByAliases(headers, ["Velocidade_2", "Velocidade"]);
    const accelHeader = findHeaderByAliases(headers, ["Acelerador"]);
    const brakeHeader = findHeaderByAliases(headers, ["Freio"]);

    const recentBrakeEvents = [];
    let previousBrakeActive = false;

    return rows.map((row) => {
      const currentDate = getRowDate(row, hourHeader, hourUnixHeader);
      const currentTimestamp = currentDate ? currentDate.getTime() : null;
      const rpmValue = extractNumericFromRow(row, [rpmHeader]);
      const speedValue = extractNumericFromRow(row, [speedHeader]);
      const accelValue = extractNumericFromRow(row, [accelHeader]);
      const brakeActive = brakeHeader ? isBrakeActivated(row[brakeHeader]) : false;

      if (currentTimestamp != null) {
        while (recentBrakeEvents.length && recentBrakeEvents[0] < currentTimestamp - 60000) {
          recentBrakeEvents.shift();
        }

        if (brakeActive && !previousBrakeActive) {
          recentBrakeEvents.push(currentTimestamp);
        }
      }

      const speedTransit = classifySpeedTransit(speedValue, config);
      const noteRoute = getRouteScore(speedTransit);

      const brakeCount =
        currentTimestamp != null && brakeHeader
          ? recentBrakeEvents.length
          : null;

      const brakeLabel = classifyBrake(brakeCount, config);
      const greenBand = classifyGreenBand(rpmValue, config);
      const rpmTransit = classifyRpmTransit(rpmValue, config);
      const accelerationLabel = classifyAcceleration(accelValue, config);
      const sumScore = calculateSumScore({
        greenBand,
        speedTransit,
        rpmTransit,
        acceleration: accelerationLabel,
        brakes: brakeLabel
      });
      const driverScore = calculateDriverScore(sumScore, noteRoute);
      
      previousBrakeActive = brakeActive;

      return {
        ...row,
        "dia-mês": formatDayMonth(currentDate),
        "Faixa Verde": greenBand,
        "Trânsito (Velocidade)": speedTransit,
        "Trânsito (RPM)": rpmTransit,
        "Aceleração": accelerationLabel,
        "Frenagens": brakeLabel,
        "Nota Rota": noteRoute,
        "Soma Pontuação": sumScore,
        "Nota Motorista": driverScore
      };
    });
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function updateTopScrollbarWidth() {
    if (!els.topScrollInner || !els.previewTable) return;
    els.topScrollInner.style.width = `${els.previewTable.scrollWidth}px`;
  }

  function renderTable(headers, rows) {
    if (!headers.length || !rows.length) return;

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
      rows.length > previewLimit
        ? `Mostrando ${previewLimit} de ${rows.length} linhas com colunas de análise.`
        : `Mostrando ${rows.length} linhas com colunas de análise.`;

    requestAnimationFrame(updateTopScrollbarWidth);
  }

  function buildExportRows(headers, rawRows) {
    return rawRows.map((row) => {
      const out = {};
      for (const header of headers) {
        out[header] = row[header] ?? "";
      }
      return out;
    });
  }

  function autosizeColumns(rows, headers) {
    return headers.map((header) => {
      let maxLen = String(header).length;

      for (const row of rows.slice(0, 1000)) {
        const cell = row[header] == null ? "" : String(row[header]);
        if (cell.length > maxLen) maxLen = cell.length;
      }

      return { wch: Math.min(Math.max(maxLen + 2, 10), 40) };
    });
  }

  function formatAnalysisExportName(fileName) {
    if (!fileName) return `arquivo${ANALYSIS_EXPORT_SUFFIX}`;
    if (fileName.endsWith(ANALYSIS_EXPORT_SUFFIX)) return fileName;

    if (/_tratado\.xlsx$/i.test(fileName)) {
      return fileName.replace(/_tratado\.xlsx$/i, ANALYSIS_EXPORT_SUFFIX);
    }

    const dotIndex = fileName.lastIndexOf(".");
    if (dotIndex < 0) return `${fileName}${ANALYSIS_EXPORT_SUFFIX}`;
    return `${fileName.slice(0, dotIndex)}${ANALYSIS_EXPORT_SUFFIX}`;
  }

  function updateBadge(text, type) {
    els.badge.className = "badge";

    if (type === "ok") {
      els.badge.classList.add("badge-ok");
    } else if (type === "warn") {
      els.badge.classList.add("badge-warn");
    } else {
      els.badge.classList.add("badge-bad");
    }

    els.badge.innerHTML = `<span class="badge-dot"></span>${text}`;
  }

  function setButtonState() {
    els.btnAddAnalysis.disabled = !state.baseRows.length;
    els.btnAddAnalysis.textContent = state.analysisApplied
      ? "Reaplicar colunas de análise"
      : "Adicionar colunas de análise";
  }

  function resetAnalysisState() {
    state.originalFileName = "";
    state.exportFileName = "";
    state.sheetName = "";
    state.baseHeaders = [];
    state.baseRows = [];
    state.analysisHeaders = [];
    state.analysisRows = [];
    state.analysisApplied = false;
    setButtonState();
  }

  async function parseFileForStep1(file, token) {
    if (!file || typeof XLSX === "undefined") {
      return;
    }

    const buffer = await file.arrayBuffer();
    const workbook = XLSX.read(buffer, {
      type: "array",
      cellDates: true
    });

    const targetSheetName = findRequiredSheetName(workbook);
    if (!targetSheetName) {
      throw new Error('A planilha obrigatória "Mensagens" não foi encontrada no arquivo.');
    }

    const sheet = workbook.Sheets[targetSheetName];
    const rows = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      raw: true
    });

    const treated = processWorkbookRows(rows);

    if (token !== state.parseToken) {
      return;
    }

    state.originalFileName = file.name;
    state.exportFileName = formatAnalysisExportName(file.name);
    state.sheetName = targetSheetName;
    state.baseHeaders = treated.headers;
    state.baseRows = treated.rows;
    state.analysisHeaders = [];
    state.analysisRows = [];
    state.analysisApplied = false;
    setButtonState();
  }

  function applyAnalysisColumns() {
    if (!state.baseRows.length || !state.baseHeaders.length) {
      alert("Carregue um arquivo antes de adicionar as colunas de análise.");
      return;
    }

    state.analysisRows = buildAnalysisRows(state.baseRows, state.baseHeaders);
    state.analysisHeaders = mergeHeadersWithAnalysis(state.baseHeaders);
    state.analysisApplied = true;
    state.exportFileName = formatAnalysisExportName(state.originalFileName || state.exportFileName);

    renderTable(state.analysisHeaders, state.analysisRows);

    els.fileName.textContent = state.originalFileName || "-";
    els.rowCount.textContent = String(state.analysisRows.length);
    els.columnCount.textContent = String(state.analysisHeaders.length);
    els.meta.textContent = `Planilha lida: ${state.sheetName}. Arquivo final: ${state.exportFileName}`;

    updateBadge("Análise step 1 aplicada", "ok");
    setButtonState();
  }

  function exportAnalysisFile(event) {
    if (!state.analysisApplied || !state.analysisRows.length || !state.analysisHeaders.length) {
      return;
    }

    event.preventDefault();
    event.stopImmediatePropagation();

    const exportRows = buildExportRows(state.analysisHeaders, state.analysisRows);
    const worksheet = XLSX.utils.json_to_sheet(exportRows, {
      header: state.analysisHeaders
    });

    worksheet["!cols"] = autosizeColumns(exportRows, state.analysisHeaders);

    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Tratado Analise");
    XLSX.writeFile(workbook, state.exportFileName);
  }

  function bindEvents() {
    setButtonState();

    els.input.addEventListener("change", async (event) => {
      const file = event.target.files && event.target.files[0];
      state.parseToken += 1;
      const currentToken = state.parseToken;

      resetAnalysisState();

      if (!file) {
        return;
      }

      try {
        await parseFileForStep1(file, currentToken);
      } catch (error) {
        console.error("Step 1: erro ao preparar análise", error);
        resetAnalysisState();
      }
    });

    els.btnAddAnalysis.addEventListener("click", applyAnalysisColumns);

    els.btnExport.addEventListener("click", exportAnalysisFile, true);

    els.btnClear.addEventListener("click", () => {
      resetAnalysisState();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindEvents, { once: true });
  } else {
    bindEvents();
  }

  function scoreIntensityLabel(value) {
    if (value === "Leve") return 2;
    if (value === "Médio" || value === "Média") return 1;
    if (value === "Intenso" || value === "Intensa") return 0;
    return 0;
  }

  function scoreSpeedTransitLabel(value) {
    return scoreIntensityLabel(value);
  }

  function scoreGreenBand(value) {
    return value === "Faixa Verde" ? 2 : 0;
  }

  function calculateSumScore({
    greenBand,
    speedTransit,
    rpmTransit,
    acceleration,
    brakes
  }) {
    return (
      scoreIntensityLabel(rpmTransit) +
      scoreIntensityLabel(acceleration) +
      scoreIntensityLabel(brakes) +
      scoreGreenBand(greenBand) +
      scoreSpeedTransitLabel(speedTransit)
    );
  }

})();