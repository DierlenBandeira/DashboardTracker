(function () {
  const state = {
    originalFileName: "",
    exportFileName: "",
    sheetName: "",
    headers: [],
    rows: []
  };

  const REQUIRED_SHEET_NAME = "Mensagens";

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

  const els = {
    input: document.getElementById("treatmentFileInput"),
    btnSelect: document.getElementById("btnSelectFile"),
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

    text = text
      .replace(/^(dom|seg|ter|qua|qui|sex|s[áa]b)\.?\s+/i, "")
      .trim();

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

  function formatExportName(fileName) {
    const dotIndex = fileName.lastIndexOf(".");
    if (dotIndex < 0) return `${fileName}_tratado.xlsx`;

    const base = fileName.slice(0, dotIndex);
    return `${base}_tratado.xlsx`;
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

    if (type === "ok") els.badge.classList.add("badge-ok");
    else if (type === "warn") els.badge.classList.add("badge-warn");
    else els.badge.classList.add("badge-bad");

    els.badge.innerHTML = `<span class="badge-dot"></span>${text}`;
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
    updateTopScrollbarWidth();
  }

  function resetState() {
    state.originalFileName = "";
    state.exportFileName = "";
    state.sheetName = "";
    state.headers = [];
    state.rows = [];

    els.fileName.textContent = "-";
    els.rowCount.textContent = "0";
    els.columnCount.textContent = "0";
    els.meta.textContent = "Nenhum arquivo processado.";
    els.btnExport.disabled = true;
    els.btnClear.disabled = true;

    updateStatus("Aguardando arquivo", "warn");
    clearTable();
  }

  function updateTopScrollbarWidth() {
    if (!els.topScrollInner || !els.previewTable) return;
    els.topScrollInner.style.width = `${els.previewTable.scrollWidth}px`;
  }

  function renderTable(headers, rows) {
    if (!headers.length || !rows.length) {
      clearTable();
      return;
    }

    const previewLimit = 200;
    const previewRows = rows.slice(0, previewLimit);

    els.head.innerHTML = `
      <tr>
        ${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}
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
        ? `Mostrando ${previewLimit} de ${rows.length} linhas tratadas.`
        : `Mostrando ${rows.length} linhas tratadas.`;

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

    const exportHeaders = Object.keys(treatedRows[0] || {});

    return {
      headers: exportHeaders,
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
      workbook.SheetNames.find((name) => normalizeKey(name) === normalizedTarget) ||
      null
    );
  }

  async function handleFile(file) {
    if (!file) return;

    if (typeof XLSX === "undefined") {
      alert("A biblioteca XLSX não está disponível.");
      return;
    }

    updateStatus("Processando arquivo...", "warn");

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

    state.originalFileName = file.name;
    state.exportFileName = formatExportName(file.name);
    state.sheetName = targetSheetName;
    state.headers = treated.headers;
    state.rows = treated.rows;

    els.fileName.textContent = state.originalFileName;
    els.rowCount.textContent = String(state.rows.length);
    els.columnCount.textContent = String(state.headers.length);
    els.meta.textContent = `Planilha lida: ${state.sheetName}. Arquivo final: ${state.exportFileName}`;

    els.btnExport.disabled = !state.rows.length;
    els.btnClear.disabled = false;

    renderTable(state.headers, state.rows);
    updateStatus("Arquivo tratado com sucesso", "ok");
  }

  function exportFile() {
    if (!state.rows.length || !state.headers.length) {
      alert("Não há dados para exportar.");
      return;
    }

    const exportRows = buildExportRows(state.headers, state.rows);
    const worksheet = XLSX.utils.json_to_sheet(exportRows, {
      header: state.headers
    });

    worksheet["!cols"] = autosizeColumns(exportRows, state.headers);

    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Tratado");
    XLSX.writeFile(workbook, state.exportFileName);
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
      els.input.value = "";
      els.input.click();
    });

    els.input.addEventListener("change", async (event) => {
      const file = event.target.files?.[0];
      if (!file) return;

      try {
        await handleFile(file);
      } catch (error) {
        console.error(error);
        updateStatus("Falha no tratamento", "bad");
        alert(error?.message || "Erro ao processar o arquivo.");
      }
    });

    els.btnExport.addEventListener("click", exportFile);

    els.btnClear.addEventListener("click", () => {
      els.input.value = "";
      resetState();
    });
  }

  if (
    !els.input ||
    !els.btnSelect ||
    !els.btnExport ||
    !els.btnClear ||
    !els.fileName ||
    !els.rowCount ||
    !els.columnCount ||
    !els.meta ||
    !els.badge ||
    !els.previewInfo ||
    !els.head ||
    !els.body
  ) {
    console.error("Treatment: elementos da página não encontrados.");
    return;
  }

  bindEvents();
  resetState();
  syncTopScrollbar();
})();