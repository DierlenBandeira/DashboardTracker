// main.js

function apiUrl(path) {
  return `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

function pushGapToLiveInstance() {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!state) return;

  state.samples.push({
    t: Date.now(),
    msgTm: null,
    values: {},
    isGap: true,
  });
}

function pushSampleToLiveInstance(fields, msgTm) {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!state) return;

  const consumidoRaw = toNum(fields?.consumido);
  let consumido = null;

  if (consumidoRaw != null) {
    if (state.lastConsumidoRaw == null) {
      consumido = 0;
    } else {
      const delta = consumidoRaw - state.lastConsumidoRaw;
      consumido = Number.isFinite(delta) && delta > 0 ? delta : 0;
    }
    state.lastConsumidoRaw = consumidoRaw;
  }

  state.samples.push({
    t: Date.now(),
    msgTm,
    values: {
      velocidade: toNum(fields?.velocidade),
      altitude: toNum(fields?.altitude),
      pct_acelerado: toNum(fields?.pct_acelerado),
      consumido,
      rpm: toNum(fields?.rpm),

      motor: normalizeOnOff(fields?.motor),
      temperatura_motor: toNum(fields?.temperatura_motor),
      ar_cond: normalizeOnOff(fields?.ar_cond),
      freio: normalizeBrake(fields?.freio),
      arla: toNum(fields?.arla),
      consumido_delta: toNum(fields?.consumido_delta),
      peso_total: toNum(fields?.peso_total),
    },
    isGap: false,
  });
}

async function tick() {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;

  if (!liveInstance || !state) return;

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

    const unitNow = document.getElementById("unitNow");
    if (unitNow) unitNow.innerText = itemId ?? "-";

    let addedNewPoint = false;

    if (ok) {
      if (stale) {
        pushGapToLiveInstance();
        drawChartInstance(liveInstance);
        addedNewPoint = true;
      } else if (msgTm != null && msgTm !== state.lastMsgTm) {
        if (state.lastMsgTm != null) {
          const gapSec = Math.max(0, msgTm - state.lastMsgTm);
          const missingSteps = Math.floor(gapSec / STEP_SEC) - 1;

          for (let i = 0; i < missingSteps; i++) {
            pushGapToLiveInstance();
          }
        }

        state.lastMsgTm = msgTm;
        pushSampleToLiveInstance(f, msgTm);

        drawChartInstance(liveInstance);
        addedNewPoint = true;
      }
    }

    if (addedNewPoint) {
      maybeScrollChartToEndForInstance(liveInstance);
    }

    const ts = j.last_update_epoch
      ? new Date(j.last_update_epoch * 1000).toLocaleString()
      : "-";

    if (ok) {
      const statusConsulta = document.getElementById("statusConsulta");
      const statusTelemetria = document.getElementById("statusTelemetria");
      const statusTelemetriaSub = document.getElementById("statusTelemetriaSub");
      const statusSnapshot = document.getElementById("statusSnapshot");

      if (statusConsulta) statusConsulta.innerText = ts;
      if (statusTelemetria) statusTelemetria.innerText = formatAge(age);
      if (statusTelemetriaSub) {
        statusTelemetriaSub.innerText =
          age != null ? `Último dado recebido há ${formatAge(age)}` : "Sem informação";
      }
      if (statusSnapshot) statusSnapshot.innerText = refreshed ? "Novo" : "Mantido";

      const replayCount = getReplayChartInstances().length;

      const badges = [];
      badges.push(badgeHtml("Backend OK", "badge-ok"));
      if (replayCount > 0) {
        badges.push(badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn"));
      }
      badges.push(stale ? badgeHtml("Unidade sem sinal", "badge-bad") : badgeHtml("Unidade online", "badge-ok"));
      badges.push(refreshed ? badgeHtml("Snapshot novo", "badge-ok") : badgeHtml("Snapshot mantido", "badge-warn"));

      if (statusBadges) statusBadges.innerHTML = badges.filter(Boolean).join("");
      if (meta) meta.innerHTML = `Consulta do servidor em ${ts}`;
    } else {
      const statusConsulta = document.getElementById("statusConsulta");
      const statusTelemetria = document.getElementById("statusTelemetria");
      const statusTelemetriaSub = document.getElementById("statusTelemetriaSub");
      const statusSnapshot = document.getElementById("statusSnapshot");

      if (statusConsulta) statusConsulta.innerText = ts;
      if (statusTelemetria) statusTelemetria.innerText = "-";
      if (statusTelemetriaSub) statusTelemetriaSub.innerText = "Falha ao obter status";
      if (statusSnapshot) statusSnapshot.innerText = "-";

      if (statusBadges) {
        const replayCount = getReplayChartInstances().length;
        statusBadges.innerHTML = [
          badgeHtml("Erro no backend", "badge-bad"),
          replayCount > 0 ? badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn") : "",
        ]
          .filter(Boolean)
          .join("");
      }

      if (meta) {
        meta.innerHTML = `<span class="bad">ERRO</span> • ${err ?? "sem detalhe"} • ${ts}`;
      }
    }
  } catch (e) {
    const meta = document.getElementById("meta");
    if (meta) {
      meta.innerHTML = `<span class="bad">ERRO</span> • ${e}`;
    }

    const statusBadges = document.getElementById("statusBadges");
    if (statusBadges) {
      const replayCount = getReplayChartInstances().length;
      statusBadges.innerHTML = [
        badgeHtml("Erro de comunicação", "badge-bad"),
        replayCount > 0 ? badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn") : "",
      ]
        .filter(Boolean)
        .join("");
    }
  }
}

function bindMainEvents() {
  document.addEventListener("change", async (ev) => {
    if (ev.target && ev.target.id === "unitSelect") {
      const itemId = Number(ev.target.value);
      if (!Number.isFinite(itemId)) return;

      try {
        await postJson(apiUrl("/set_unit"), { itemId });
        window.__currentItemId = itemId;

        const liveInstance = getLiveChartInstance() || mainChartInstance;
        if (liveInstance?.state) {
          resetFullInstanceState(liveInstance.state);
          drawChartInstance(liveInstance);
          maybeScrollChartToEndForInstance(liveInstance, true);
        }
      } catch (e) {
        alert("Erro ao trocar unidade: " + e);
      }
    }
  });

  document.addEventListener("click", (ev) => {
    const activeInstance = getActiveChartInstance();
    const state = activeInstance?.state;
    const dom = activeInstance ? getInstanceDom(state) : null;

    if (!activeInstance || !state || state.selectedIdx === null || !dom?.popover) return;

    const clickedInsidePopover = dom.popover.contains(ev.target);
    const clickedCanvas = dom.canvas && ev.target === dom.canvas;

    if (!clickedInsidePopover && !clickedCanvas) {
      closeCommentPopoverInstance(activeInstance);
    }
  });

  window.addEventListener("mouseup", () => {
    chartInstances.forEach((instance) => {
      if (!instance?.state) return;
      setTimeout(() => {
        instance.state.pointerDownInfo = null;
      }, 0);
    });
  });

  window.addEventListener("resize", () => {
    chartInstances.forEach((instance) => {
      const state = instance?.state;
      const dom = instance ? getInstanceDom(state) : null;
      const wrap = dom?.wrap;

      const previousLeft = wrap ? wrap.scrollLeft : 0;
      const keepManual = state ? !state.autoScrollToEnd : false;

      drawChartInstance(instance);

      if (wrap) {
        if (keepManual) {
          wrap.scrollLeft = previousLeft;
        } else {
          maybeScrollChartToEndForInstance(instance);
        }
      }
    });
  });
}

function bootstrapApp() {
  renderYConfig();
  renderTransitManageConfig();
  renderLegend();
  renderColorConfig();

  bindMainChartDom();
  drawChartInstance(getLiveChartInstance() || mainChartInstance);

  refreshUnits();
  maybeScrollChartToEndForInstance(getLiveChartInstance() || mainChartInstance, true);

  tick();
  startPollProgress();

  setInterval(async () => {
    await tick();
    restartPollProgress();
  }, POLL_MS);
}

bindMainEvents();
bootstrapApp();