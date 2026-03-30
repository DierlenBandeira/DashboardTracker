// replay.js

(function () {
  let replayCardSeq = 0;

  function getReplayCardsContainer() {
    let el = document.getElementById("replayCards");
    if (el) return el;

    const section =
      document.getElementById("replayCompareSection") ||
      document.getElementById("replaySection") ||
      document.body;

    el = document.createElement("div");
    el.id = "replayCards";
    el.className = "replay-cards";
    section.appendChild(el);
    return el;
  }

  function updateReplayCardsCount() {
    const countEl = document.getElementById("replayCardsCount");
    if (countEl) {
      countEl.textContent = String(getReplayChartInstances().length);
    }

    const wrap = getReplayCardsContainer();
    const hasCards = !!wrap.querySelector(".replay-compare-card");

    let empty = wrap.querySelector(".replay-empty-state");

    if (!hasCards) {
      if (!empty) {
        empty = document.createElement("div");
        empty.className = "replay-empty-state";
        empty.innerHTML =
          'Nenhum arquivo carregado ainda. Use <b>Upload Dados</b> para criar o primeiro comparativo.';
        wrap.appendChild(empty);
      }
    } else if (empty) {
      empty.remove();
    }
  }

  function formatReplayMetric(value, suffix = "", decimals = 2) {
    if (value == null || !Number.isFinite(value)) return "-";
    return (
      Number(value).toLocaleString("pt-BR", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      }) + suffix
    );
  }

  function formatReplayMetricInt(value) {
    if (value == null || !Number.isFinite(value)) return "-";
    return Math.round(value).toLocaleString("pt-BR");
  }

  function getRangeDelta(values, { minValue = null } = {}) {
    const nums = values.filter(
      (v) => Number.isFinite(v) && (minValue == null || v > minValue)
    );
    if (nums.length < 2) return null;
    const min = Math.min(...nums);
    const max = Math.max(...nums);
    return max >= min ? max - min : null;
  }

  function computeReplayKpis(rows) {
    const validRows = Array.isArray(rows) ? rows : [];
    const movingRows = validRows.filter(
      (row) => Number.isFinite(row?.velocidade) && row.velocidade > 0
    );

    const avg = (arr, pick) => {
      const nums = arr.map(pick).filter((v) => v != null && Number.isFinite(v));
      if (!nums.length) return null;
      return nums.reduce((acc, v) => acc + v, 0) / nums.length;
    };

    const kmRodado = getRangeDelta(validRows.map((row) => row?.odometro), {
      minValue: 0,
    });

    const litrosConsumidos = getRangeDelta(
      validRows.map((row) => row?.consumidoRaw),
      { minValue: 1 }
    );

    const consumoMedio =
      Number.isFinite(kmRodado) &&
      Number.isFinite(litrosConsumidos) &&
      kmRodado > 0 &&
      litrosConsumidos > 0
        ? kmRodado / litrosConsumidos
        : null;

    const mediaVelocidade = avg(movingRows, (row) => row?.velocidade);
    const rpmMedio = avg(movingRows, (row) => row?.rpm);
    const aceleradorMedio = avg(movingRows, (row) => row?.pct_acelerado);

    const qtdFrenagens = movingRows.filter((row) => {
      const freio = row?.freio;
      return freio === 1 || freio === true || freio === "1";
    }).length;

    let qtdParadas = 0;
    let insideStop = false;

    for (const row of validRows) {
      const vel = row?.velocidade;
      if (Number.isFinite(vel) && vel === 0) {
        if (!insideStop) {
          qtdParadas += 1;
          insideStop = true;
        }
      } else {
        insideStop = false;
      }
    }

    return {
      consumoMedio,
      mediaVelocidade,
      rpmMedio,
      aceleradorMedio,
      qtdFrenagens,
      qtdParadas,
    };
  }

  function parseExcelSerialDate(value) {
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
      return parseExcelSerialDate(value);
    }

    let text = String(value).trim();
    if (!text) return null;

    text = text
      .replace(/\u00A0/g, " ")
      .replace(/^(dom|seg|ter|qua|qui|quin|quinta|sex|s[áa]b)\.?\s+/i, "")
      .replace(/\s+/g, " ")
      .trim();

    let match = text.match(
      /^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
    );

    if (!match) {
      match = text.match(
        /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
      );
      if (!match) return null;

      const [, yyyy, mm, dd, hh = "0", mi = "0", ss = "0"] = match;
      const year = Number(yyyy);

      const date = new Date(
        year,
        Number(mm) - 1,
        Number(dd),
        Number(hh),
        Number(mi),
        Number(ss),
        0
      );

      if (
        date.getFullYear() !== year ||
        date.getMonth() !== Number(mm) - 1 ||
        date.getDate() !== Number(dd)
      ) {
        return null;
      }

      return Number.isNaN(date.getTime()) ? null : date;
    }

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

    if (
      date.getFullYear() !== year ||
      date.getMonth() !== Number(mm) - 1 ||
      date.getDate() !== Number(dd)
    ) {
      return null;
    }

    return Number.isNaN(date.getTime()) ? null : date;
  }

  function normalizeReplayRowFromArray(row, cols) {
    const id = getCell(row, cols.id);

    let tmText = getCell(row, cols.tm);

    if (tmText == null || String(tmText).trim() === "") {
      tmText = getCell(row, cols.agrupamento);
    }
    if (tmText == null || String(tmText).trim() === "") {
      tmText = row[2];
    }
    if (tmText == null || String(tmText).trim() === "") {
      tmText = row[1];
    }

    const dt = parseBrazilDateTime(tmText);
    const msgTm = dt ? Math.floor(dt.getTime() / 1000) : null;

    return {
      id: id ?? null,
      msgTm,
      velocidade: toNum(getCell(row, cols.velocidade)),
      altitude: toNum(getCell(row, cols.altitude)),
      pct_acelerado: toNum(getCell(row, cols.pct_acelerado)),
      rpm: toNum(getCell(row, cols.rpm)),
      odometro: toNum(getCell(row, cols.odometro)),
      motor: normalizeOnOff(getCell(row, cols.motor)),
      temperatura_motor: toNum(getCell(row, cols.temperatura_motor)),
      ar_cond: normalizeOnOff(getCell(row, cols.ar_cond)),
      freio: normalizeBrake(getCell(row, cols.freio)),
      peso_total: toNum(getCell(row, cols.peso_total)),
      consumido: 0,
      consumidoRaw: toNum(getCell(row, cols.consumido_raw)),
      arla: 0,
      arlaRaw: toNum(getCell(row, cols.arla_raw)),
      consumido_delta: 0,
      consumidoDeltaRaw: toNum(getCell(row, cols.consumido_delta_raw)),
      comment: String(getCell(row, cols.comment) ?? "").trim(),
    };
  }

  async function parseReplayFile(file) {
    const buffer = await file.arrayBuffer();
    const workbook = XLSX.read(buffer, { type: "array" });

    const sheetName = workbook.SheetNames.includes("Mensagens")
      ? "Mensagens"
      : workbook.SheetNames[0];

    if (!sheetName) throw new Error("Arquivo sem planilhas.");

    const sheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      raw: true,
    });

    if (!rows.length) throw new Error("Arquivo sem linhas de dados.");

    const nonEmptyRows = rows.filter(
      (row) =>
        Array.isArray(row) &&
        row.some((cell) => String(cell ?? "").trim() !== "")
    );

    if (!nonEmptyRows.length) throw new Error("Arquivo sem linhas válidas.");

    const headerRowIndex = findReplayHeaderRow(nonEmptyRows);
    if (headerRowIndex < 0) {
      throw new Error("Não foi possível localizar o cabeçalho pela coluna 'Hora'.");
    }

    const headerRow = nonEmptyRows[headerRowIndex];
    const headerIndexMap = buildHeaderIndexMap(headerRow);
    const cols = resolveImportCols(headerIndexMap) || {};

    const normalizedHeaders = headerRow.map((h) =>
      String(h ?? "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/\*/g, "")
        .trim()
        .toLowerCase()
    );

    if (cols.id == null) {
      const idx = normalizedHeaders.findIndex((h) => h === "nº" || h === "no" || h === "n" || h === "numero");
      if (idx >= 0) cols.id = idx;
      else cols.id = 0;
    }

    if (cols.tm == null) {
      let idx = normalizedHeaders.findIndex((h) => h === "hora");
      if (idx < 0) idx = normalizedHeaders.findIndex((h) => h.includes("hora"));
      if (idx < 0) idx = 2;
      cols.tm = idx;
    }

    if (cols.agrupamento == null) {
      let idx = normalizedHeaders.findIndex((h) => h === "agrupamento");
      if (idx < 0) idx = 1;
      cols.agrupamento = idx;
    }

    const dataRows = nonEmptyRows.slice(headerRowIndex + 1);
    const parsed = [];

    for (const row of dataRows) {
      const parsedRow = normalizeReplayRowFromArray(row, cols);
      if (parsedRow.msgTm == null) continue;
      parsed.push(parsedRow);
    }

    if (!parsed.length) {
      throw new Error("Nenhuma linha válida encontrada no arquivo.");
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
      if (row.consumidoDeltaRaw != null) {
        previousConsumidoDeltaRaw = row.consumidoDeltaRaw;
      }
    }

    return parsed;
  }

  function pushReplayRowToState(state, row) {
    state.samples.push({
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

    if (row.msgTm != null) state.lastMsgTm = row.msgTm;
    if (row.consumidoRaw != null) state.lastConsumidoRaw = row.consumidoRaw;
    if (row.comment && row.msgTm != null) {
      state.commentsByMsgTm[row.msgTm] = row.comment;
    }
  }

  function buildReplayCardElement(state, kpis) {
    const card = document.createElement("article");
    card.className = "replay-compare-card";
    card.dataset.replayInstanceId = state.id;

    card.innerHTML = `
      <div class="replay-compare-top">
        <div>
          <div class="replay-compare-title">${escapeHtml(state.fileName || state.title || "Arquivo replay")}</div>
          <div class="replay-compare-subtitle">Comparativo independente com comentários, tooltip e replay próprio.</div>
        </div>
        <div class="replay-compare-actions">
          <button type="button" data-action="play">Play</button>
          <button type="button" data-action="pause">Pause</button>
          <button type="button" data-action="show-all">Mostrar tudo</button>
          <button type="button" data-action="remove">Remover</button>
          <button type="button" data-action="download" onclick="window.downloadData()">Download</button>
        </div>
      </div>

      <div class="replay-compare-grid">
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">Consumo Médio</div>
          <div class="replay-compare-metric-value">${formatReplayMetric(kpis.consumoMedio, " km/L", 2)}</div>
        </div>
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">Velocidade Média</div>
          <div class="replay-compare-metric-value">${formatReplayMetric(kpis.mediaVelocidade, "", 2)}</div>
        </div>
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">RPM Médio</div>
          <div class="replay-compare-metric-value">${formatReplayMetric(kpis.rpmMedio, "", 0)}</div>
        </div>
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">Acelerador Médio</div>
          <div class="replay-compare-metric-value">${formatReplayMetric(kpis.aceleradorMedio, "%", 2)}</div>
        </div>
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">Frenagens</div>
          <div class="replay-compare-metric-value">${formatReplayMetricInt(kpis.qtdFrenagens)}</div>
        </div>
        <div class="replay-compare-metric">
          <div class="replay-compare-metric-label">Paradas</div>
          <div class="replay-compare-metric-value">${formatReplayMetricInt(kpis.qtdParadas)}</div>
        </div>
      </div>

      <div class="replay-card-statusline">
        <div><b>Status:</b> <span data-role="status">Arquivo carregado. Pronto para reprodução.</span></div>
        <div><b>Atual:</b> <span data-role="current">0</span></div>
      </div>

      <div class="replay-card-chart-wrap">
        <canvas class="replay-card-canvas"></canvas>
        <div class="replay-card-tip" style="display:none;position:absolute;"></div>

        <div class="comment-popover hidden replay-comment-popover" role="dialog" aria-label="Comentário do ponto">
          <div class="comment-popover-title">Comentário do ponto</div>
          <div class="comment-popover-time">-</div>
          <textarea placeholder="Digite um comentário para este instante..."></textarea>
          <div class="comment-actions">
            <button type="button" data-action="save-comment">Salvar</button>
            <button type="button" data-action="remove-comment">Remover</button>
            <button type="button" data-action="close-comment">Fechar</button>
          </div>
        </div>
      </div>

      <div class="small replay-card-meta"></div>
    `;

    const actions = card.querySelector(".replay-compare-actions");
    actions?.addEventListener("click", (ev) => {
      const button = ev.target.closest("button");
      if (!button) return;

      const action = button.dataset.action;
      const instance = getChartInstanceById(state.id);
      if (!instance) return;

      setActiveChartInstance(instance.id);

      if (action === "play") startReplayForInstance(instance);
      if (action === "pause") pauseReplayForInstance(instance);
      if (action === "show-all") showAllReplayForInstance(instance);
      if (action === "remove") destroyReplayCardInstance(instance);
    });

    const pop = card.querySelector(".replay-comment-popover");
    pop?.addEventListener("click", (ev) => {
      const button = ev.target.closest("button");
      if (!button) return;

      const instance = getChartInstanceById(state.id);
      if (!instance) return;

      const action = button.dataset.action;
      if (action === "save-comment") saveCommentInstance(instance);
      if (action === "remove-comment") removeCommentInstance(instance);
      if (action === "close-comment") closeCommentPopoverInstance(instance);
    });

    return card;
  }

  function createReplayInstanceFromRows(fileName, rows) {
    const state = createChartState({
      id: `replay-card-${++replayCardSeq}`,
      mode: "replay",
      title: fileName,
      fileName,
      replayRows: rows,
      replayIndex: 0,
      samples: [],
      commentsByMsgTm: {},
      autoScrollToEnd: true,
    });

    const kpis = computeReplayKpis(rows);
    const cardEl = buildReplayCardElement(state, kpis);

    const wrap = cardEl.querySelector(".replay-card-chart-wrap");
    const canvas = cardEl.querySelector(".replay-card-canvas");
    const tip = cardEl.querySelector(".replay-card-tip");
    const meta = cardEl.querySelector(".replay-card-meta");
    const popover = cardEl.querySelector(".replay-comment-popover");
    const commentInput = popover?.querySelector("textarea");
    const commentTimeLabel = popover?.querySelector(".comment-popover-time");
    const status = cardEl.querySelector("[data-role='status']");
    const current = cardEl.querySelector("[data-role='current']");

    bindDomToState(state, {
      wrap,
      canvas,
      tip,
      meta,
      popover,
      commentInput,
      commentTimeLabel,
      status,
      current,
    });

    const instance = {
      id: state.id,
      state,
      cardEl,
      destroy() {
        clearInstanceReplayTimer(state);
        closeCommentPopoverInstance(instance, true);
        cardEl.remove();
        unregisterChartInstance(instance.id);
        updateReplayCardsCount();
      },
    };

    registerChartInstance(instance);
    attachChartEvents(instance);

    return instance;
  }

  function startReplayForInstance(instanceLike) {
    const instance = getResolvedInstance(instanceLike);
    const state = getResolvedState(instance);
    if (!instance || !state || !state.replayRows.length) return;

    setActiveChartInstance(instance.id);

    if (state.replayIndex >= state.replayRows.length) {
      if (state.dom?.status) state.dom.status.textContent = "Replay concluído.";
      drawChartInstance(instance);
      return;
    }

    if (state.replayTimer) return;

    if (state.dom?.status) state.dom.status.textContent = "Reproduzindo...";

    state.replayTimer = setInterval(() => {
      if (state.replayIndex >= state.replayRows.length) {
        clearInstanceReplayTimer(state);
        if (state.dom?.status) state.dom.status.textContent = "Replay concluído.";
        drawChartInstance(instance);
        return;
      }

      const row = state.replayRows[state.replayIndex];
      pushReplayRowToState(state, row);
      state.replayIndex += 1;

      if (state.dom?.current) state.dom.current.textContent = String(state.replayIndex);
      drawChartInstance(instance);
      maybeScrollChartToEndForInstance(instance, true);
    }, REPLAY_STEP_MS || 2000);
  }

  function pauseReplayForInstance(instanceLike) {
    const instance = getResolvedInstance(instanceLike);
    const state = getResolvedState(instance);
    if (!instance || !state) return;

    clearInstanceReplayTimer(state);
    if (state.dom?.status) state.dom.status.textContent = "Replay pausado.";
    drawChartInstance(instance);
  }

  function showAllReplayForInstance(instanceLike) {
    const instance = getResolvedInstance(instanceLike);
    const state = getResolvedState(instance);
    if (!instance || !state) return;

    clearInstanceReplayTimer(state);

    state.samples = [];
    state.commentsByMsgTm = {};
    state.replayIndex = 0;
    state.lastConsumidoRaw = null;
    state.lastMsgTm = null;

    const total = state.replayRows.length;
    const step = total <= 5000 ? 1 : Math.ceil(total / 5000);

    for (let i = 0; i < total; i += step) {
      pushReplayRowToState(state, state.replayRows[i]);
    }

    state.replayIndex = state.replayRows.length;

    if (state.dom?.status) state.dom.status.textContent = "Visualização completa carregada.";
    if (state.dom?.current) state.dom.current.textContent = String(state.replayIndex);

    drawChartInstance(instance);
    maybeScrollChartToEndForInstance(instance, true);
  }

  function destroyReplayCardInstance(instanceLike) {
    const instance = getResolvedInstance(instanceLike);
    if (!instance) return;
    if (instance.destroy) {
      instance.destroy();
    } else {
      destroyChartInstance(instance);
      updateReplayCardsCount();
    }
  }

  async function loadReplayFile(file) {
    if (!file) return;
    if (typeof XLSX === "undefined") {
      alert("Biblioteca XLSX não carregada.");
      return;
    }

    try {
      const rows = await parseReplayFile(file);
      const instance = createReplayInstanceFromRows(file.name, rows);

      const wrap = getReplayCardsContainer();
      wrap.appendChild(instance.cardEl);

      updateReplayCardsCount();
      setActiveChartInstance(instance.id);
      drawChartInstance(instance);
      instance.cardEl.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (err) {
      console.error(err);
      alert(err?.message || "Falha ao processar o arquivo de replay.");
    }
  }

  function initReplayUploadListener() {
    const input = document.getElementById("replayFileInput");
    if (!input || input.dataset.bound === "1") {
      updateReplayCardsCount();
      return;
    }

    input.addEventListener("change", async (event) => {
      const file = event.target.files?.[0];
      if (!file) return;
      await loadReplayFile(file);
      input.value = "";
    });

    input.dataset.bound = "1";
    updateReplayCardsCount();
  }

  window.triggerReplayUpload = function () {
    const input = document.getElementById("replayFileInput");
    if (!input) return;
    input.value = "";
    input.click();
  };

  window.loadReplayFile = loadReplayFile;

  window.startReplay = function () {
    const instance = getActiveChartInstance();
    if (!instance || instance.state?.mode !== "replay") return;
    startReplayForInstance(instance);
  };

  window.pauseReplay = function () {
    const instance = getActiveChartInstance();
    if (!instance || instance.state?.mode !== "replay") return;
    pauseReplayForInstance(instance);
  };

  window.showAllReplayData = function () {
    const instance = getActiveChartInstance();
    if (!instance || instance.state?.mode !== "replay") return;
    showAllReplayForInstance(instance);
  };

  window.stopReplayMode = function () {
    const instance = getActiveChartInstance();
    if (!instance || instance.state?.mode !== "replay") return;
    destroyReplayCardInstance(instance);
  };

  window.downloadData = function () {
    const instance = getActiveChartInstance();
    const state = instance?.state;

    if (!instance || !state || !state.samples.length) {
      alert("Não há dados carregados para exportar.");
      return;
    }

    const rows = state.samples
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
        Comentarios: state.commentsByMsgTm[s.msgTm] ?? "",
      }));

    if (!rows.length) {
      alert("Não há leituras válidas para exportar.");
      return;
    }

    const ws = XLSX.utils.json_to_sheet(rows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Dados");

    const baseName =
      state.fileName || state.title || instance.id || "dados_replay";
    const fileName = `export_${baseName.replace(/\.[^.]+$/, "")}.xlsx`;

    XLSX.writeFile(wb, fileName);
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initReplayUploadListener);
  } else {
    initReplayUploadListener();
  }
})();
