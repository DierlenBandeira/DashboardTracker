(function () {
  const POLL_INTERVAL_MS = 1500;
  const POLL_TIMEOUT_MS = 30 * 60 * 1000;

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
    activePollToken: 0
  };

  const API_BASE = (window.__API_BASE__ || window.location.origin).replace(/\/$/, "");

  function apiUrl(path) {
    return `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
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
      const response = await fetch(apiUrl("/components/treatment/configs"), {
        cache: "no-store"
      });

      if (!response.ok) {
        throw new Error("Não foi possível carregar os parâmetros do treatment.");
      }

      els.configsRoot.innerHTML = await response.text();
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
    const parts = [message];

    if (typeof current === "number" && typeof total === "number" && total > 0) {
      const percent = Math.max(0, Math.min(100, Math.round((current / total) * 100)));
      parts.push(`${percent}%`);
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
    } else if (phase === "writing_output" || phase === "finalizing") {
      updateStatus("Finalizando arquivo", "warn");
    } else {
      updateStatus("Processando", "warn");
    }

    if (typeof current === "number") {
      els.rowCount.textContent = String(current);
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
