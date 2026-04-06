// ui.js

function getUiResolvedInstance(instanceLike = null) {
  return getResolvedInstance(instanceLike);
}

function getUiResolvedState(instanceLike = null) {
  return getResolvedState(instanceLike);
}

function getUiDom(instanceLike = null) {
  const state = getUiResolvedState(instanceLike);
  return getInstanceDom(state);
}

function toggleConfigPanel() {
  const wrap = document.getElementById("configPanelWrap");
  const btn = document.getElementById("toggleConfigBtn");
  const text = document.getElementById("toggleConfigText");
  if (!wrap || !btn || !text) return;

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

function renderLegend() {
  const legend = document.getElementById("chartLegend");
  if (!legend) return;

  legend.innerHTML = "";

  getAllSeriesWithColors("replay")
    .filter((serie, index, arr) => arr.findIndex((s) => s.key === serie.key) === index)
    .forEach((serie) => {
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
        drawAllCharts();
      });

      legend.appendChild(item);
    });
}

function openCommentPopoverInstance(instanceLike, idx, px, py) {
  const instance = getUiResolvedInstance(instanceLike);
  const state = getUiResolvedState(instance);
  const dom = getUiDom(instance);

  if (!instance || !state || !dom.popover || !dom.commentInput || !dom.commentTimeLabel || !dom.wrap) {
    return;
  }

  const sample = getSampleAtVisualIndex(instance, idx);
  if (!sample || sample.msgTm == null) return;

  setActiveChartInstance(instance.id);

  state.selectedIdx = idx;
  state.selectedMsgTm = sample.msgTm;

  dom.commentTimeLabel.textContent = `Instante: ${formatTimestampFull(sample.msgTm)}`;
  dom.commentInput.value = state.commentsByMsgTm[state.selectedMsgTm] || "";

  const coordinatesEl = dom.popover.querySelector("[data-role='comment-coordinates']");
  const mapsLinkEl = dom.popover.querySelector("[data-role='comment-maps-link']");
  const coordinatesLabel =
    sample.coordinatesText || formatCoordinatesLabel(sample.coordinates);
  const mapsUrl = buildGoogleMapsUrl(sample.coordinates || sample.coordinatesText);

  if (coordinatesEl) {
    coordinatesEl.textContent = coordinatesLabel ? `Coordenadas: ${coordinatesLabel}` : "";
    coordinatesEl.hidden = !coordinatesLabel;
  }

  if (mapsLinkEl) {
    if (mapsUrl) {
      mapsLinkEl.href = mapsUrl;
      mapsLinkEl.hidden = false;
    } else {
      mapsLinkEl.removeAttribute("href");
      mapsLinkEl.hidden = true;
    }
  }

  dom.popover.classList.remove("hidden");

  const pw = Math.max(320, dom.popover.offsetWidth || 320);
  const ph = Math.max(210, dom.popover.offsetHeight || 210);

  const scrollLeft = dom.wrap.scrollLeft;
  const visibleLeft = scrollLeft;
  const visibleRight = scrollLeft + dom.wrap.clientWidth;

  let left = px + 14;
  let top = py + 14;

  if (left + pw > visibleRight - 8) left = px - pw - 14;
  if (top + ph > dom.wrap.clientHeight - 8) top = py - ph - 14;

  left = Math.max(visibleLeft + 8, left);
  top = Math.max(8, top);

  dom.popover.style.left = left + "px";
  dom.popover.style.top = top + "px";

  drawChartInstance(instance);
  setTimeout(() => dom.commentInput.focus(), 0);
}

function closeCommentPopoverInstance(instanceLike, force = false) {
  const instance = getUiResolvedInstance(instanceLike);
  const state = getUiResolvedState(instance);
  const dom = getUiDom(instance);

  if (!instance || !state || !dom.popover) return;

  state.selectedIdx = null;
  state.selectedMsgTm = null;

  const coordinatesEl = dom.popover.querySelector("[data-role='comment-coordinates']");
  const mapsLinkEl = dom.popover.querySelector("[data-role='comment-maps-link']");
  if (coordinatesEl) {
    coordinatesEl.textContent = "";
    coordinatesEl.hidden = true;
  }
  if (mapsLinkEl) {
    mapsLinkEl.removeAttribute("href");
    mapsLinkEl.hidden = true;
  }

  dom.popover.classList.add("hidden");

  if (!force) {
    drawChartInstance(instance);
    hideTipForInstance(instance, true);
  }
}

function saveCommentInstance(instanceLike) {
  const instance = getUiResolvedInstance(instanceLike);
  const state = getUiResolvedState(instance);
  const dom = getUiDom(instance);

  if (!instance || !state || state.selectedMsgTm == null || !dom.commentInput) return;

  const text = dom.commentInput.value.trim();

  if (text) state.commentsByMsgTm[state.selectedMsgTm] = text;
  else delete state.commentsByMsgTm[state.selectedMsgTm];

  if (state.mode === "replay") {
    const row = state.replayRows.find((r) => r.msgTm === state.selectedMsgTm);
    if (row) row.comment = text || "";
  }

  drawChartInstance(instance);
  closeCommentPopoverInstance(instance);
}

function removeCommentInstance(instanceLike) {
  const instance = getUiResolvedInstance(instanceLike);
  const state = getUiResolvedState(instance);

  if (!instance || !state || state.selectedMsgTm == null) return;

  delete state.commentsByMsgTm[state.selectedMsgTm];

  if (state.mode === "replay") {
    const row = state.replayRows.find((r) => r.msgTm === state.selectedMsgTm);
    if (row) row.comment = "";
  }

  drawChartInstance(instance);
  closeCommentPopoverInstance(instance);
}

function openCommentPopover(idx, px, py) {
  const instance = getActiveChartInstance() || getLiveChartInstance() || mainChartInstance;
  openCommentPopoverInstance(instance, idx, px, py);
}

function closeCommentPopover(force = false) {
  const instance = getActiveChartInstance() || getLiveChartInstance() || mainChartInstance;
  closeCommentPopoverInstance(instance, force);
}

function saveComment() {
  const instance = getActiveChartInstance() || getLiveChartInstance() || mainChartInstance;
  saveCommentInstance(instance);
}

function removeComment() {
  const instance = getActiveChartInstance() || getLiveChartInstance() || mainChartInstance;
  removeCommentInstance(instance);
}

function renderYConfig() {
  const wrap = document.getElementById("yConfig");
  if (!wrap) return;

  wrap.innerHTML = getAllSeriesWithColors("replay")
    .filter((serie, index, arr) => arr.findIndex((s) => s.key === serie.key) === index)
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

function ensureTransitManageConfigContainer() {
  return document.getElementById("transitManageConfig");
}

function renderTransitManageConfig() {
  const container = ensureTransitManageConfigContainer();
  if (!container) return;

  const velCfg = transitManageCfg?.velocidade || {};
  const rpmCfg = transitManageCfg?.rpm || {};
  const accCfg = transitManageCfg?.pct_acelerado || {};

  container.innerHTML = `
    <div class="card">
      <div class="k">Velocidade</div>
      <div class="cfgrow" style="margin-bottom:8px;">
        <label style="display:flex;align-items:center;gap:8px;">
          <input type="checkbox" id="tm_enabled_velocidade" ${velCfg.enabled ? "checked" : ""}>
          Ativo
        </label>
      </div>
      <div class="cfgrow">
        <input id="tm_velocidade_value" placeholder="Intenso abaixo de" value="${velCfg.value ?? 25}">
      </div>
      <div class="small">Linha tracejada quando velocidade for menor que este valor.</div>
    </div>

    <div class="card">
      <div class="k">RPM</div>
      <div class="cfgrow" style="margin-bottom:8px;">
        <label style="display:flex;align-items:center;gap:8px;">
          <input type="checkbox" id="tm_enabled_rpm" ${rpmCfg.enabled ? "checked" : ""}>
          Ativo
        </label>
      </div>
      <div class="cfgrow">
        <input id="tm_rpm_min" placeholder="RPM mínimo normal" value="${rpmCfg.min ?? 800}">
        <input id="tm_rpm_max" placeholder="RPM máximo normal" value="${rpmCfg.max ?? 1800}">
      </div>
      <div class="small">Linha tracejada fora da faixa normal.</div>
    </div>

    <div class="card">
      <div class="k">% Acelerado</div>
      <div class="cfgrow" style="margin-bottom:8px;">
        <label style="display:flex;align-items:center;gap:8px;">
          <input type="checkbox" id="tm_enabled_pct_acelerado" ${accCfg.enabled ? "checked" : ""}>
          Ativo
        </label>
      </div>
      <div class="cfgrow">
        <input id="tm_pct_acelerado_value" placeholder="Intenso a partir de" value="${accCfg.value ?? 50.01}">
      </div>
      <div class="small">Linha tracejada quando % acelerado for maior ou igual a este valor.</div>
    </div>
  `;
}

window.applyTransitManageConfig = function () {
  transitManageCfg.velocidade = {
    enabled: !!document.getElementById("tm_enabled_velocidade")?.checked,
    rule: "below",
    value: Number(document.getElementById("tm_velocidade_value")?.value ?? 25),
  };

  transitManageCfg.rpm = {
    enabled: !!document.getElementById("tm_enabled_rpm")?.checked,
    rule: "outside_range",
    min: Number(document.getElementById("tm_rpm_min")?.value ?? 800),
    max: Number(document.getElementById("tm_rpm_max")?.value ?? 1800),
  };

  transitManageCfg.pct_acelerado = {
    enabled: !!document.getElementById("tm_enabled_pct_acelerado")?.checked,
    rule: "gte",
    value: Number(document.getElementById("tm_pct_acelerado_value")?.value ?? 50.01),
  };

  drawAllCharts();
};

window.resetTransitManageConfig = function () {
  transitManageCfg = JSON.parse(JSON.stringify(DEFAULT_TRANSIT_MANAGE_CFG));
  renderTransitManageConfig();
  drawAllCharts();
};

function ensureColorConfigContainer() {
  return document.getElementById("colorConfig");
}

function renderColorConfig() {
  const container = ensureColorConfigContainer();
  if (!container) return;

  container.innerHTML = getAllSeriesWithColors("replay")
    .filter((serie, index, arr) => arr.findIndex((s) => s.key === serie.key) === index)
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

  getAllSeriesWithColors("replay")
    .filter((serie, index, arr) => arr.findIndex((s) => s.key === serie.key) === index)
    .forEach((s) => {
      const colorInput = document.getElementById(`color_${s.key}`);
      const hexInput = document.getElementById(`colorhex_${s.key}`);
      if (!colorInput || !hexInput) return;

      colorInput.addEventListener("input", () => {
        hexInput.value = colorInput.value;
        PALETTE[s.key] = colorInput.value;
        renderLegend();
        drawAllCharts();
      });

      hexInput.addEventListener("change", () => {
        const v = String(hexInput.value || "").trim();
        if (/^#[0-9a-fA-F]{6}$/.test(v)) {
          colorInput.value = v;
          PALETTE[s.key] = v;
          renderLegend();
          drawAllCharts();
        } else {
          hexInput.value = PALETTE[s.key];
        }
      });
    });
}

window.applyYConfig = function () {
  getAllSeriesWithColors("replay")
    .filter((serie, index, arr) => arr.findIndex((s) => s.key === serie.key) === index)
    .forEach((s) => {
      const minv = document.getElementById("ymin_" + s.key)?.value?.trim();
      const maxv = document.getElementById("ymax_" + s.key)?.value?.trim();
      yCfg[s.key] = {
        min: minv === "" ? null : Number(minv),
        max: maxv === "" ? null : Number(maxv),
      };
    });

  drawAllCharts();
};

window.resetYConfig = function () {
  yCfg = JSON.parse(JSON.stringify(DEFAULT_Y_CFG));
  renderYConfig();
  drawAllCharts();
};

window.resetSeriesColors = function () {
  PALETTE = { ...DEFAULT_PALETTE };
  renderLegend();
  renderColorConfig();
  drawAllCharts();
};

function toggleInnerConfig(wrapId, btn) {
  const wrap = document.getElementById(wrapId);
  if (!wrap) return;

  const isOpen = wrap.classList.contains("open");

  if (isOpen) {
    wrap.classList.remove("open");
    if (btn) btn.classList.remove("open");
  } else {
    wrap.classList.add("open");
    if (btn) btn.classList.add("open");
  }
}
