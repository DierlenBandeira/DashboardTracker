// main.js

function apiUrl(path) {
  return `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

const LIVE_SERIES_LIMIT = 180;

function getFieldRaw(field) {
  if (field == null) return null;

  if (typeof field === "object" && field.raw != null) {
    const n = Number(field.raw);
    return Number.isFinite(n) ? n : null;
  }

  const n = Number(field);
  return Number.isFinite(n) ? n : null;
}

function pushGapToLiveInstanceFromSample(sample) {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!state) return;

  const sampleMs =
    sample?.sample_tm != null ? Number(sample.sample_tm) * 1000 : Date.now();

  state.samples.push({
    t: sampleMs,
    msgTm: sample?.msg_tm ?? null,
    values: {},
    isGap: true,
    isRepeated: false,
    sampleState: "stale",
  });
}

function pushSampleToLiveInstanceFromSample(sample) {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!state) return;

  const values = sample?.values || {};
  const sampleMs =
    sample?.sample_tm != null ? Number(sample.sample_tm) * 1000 : Date.now();

  const consumidoRaw = getFieldRaw(values?.consumido);
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
    t: sampleMs,
    msgTm: sample?.msg_tm ?? null,
    values: {
      velocidade: getFieldRaw(values?.velocidade),
      altitude: getFieldRaw(values?.altitude),
      pct_acelerado: getFieldRaw(values?.pct_acelerado),
      consumido,
      rpm: getFieldRaw(values?.rpm),

      motor: normalizeOnOff(getFieldRaw(values?.motor)),
      temperatura_motor: getFieldRaw(values?.temperatura_motor),
      ar_cond: normalizeOnOff(getFieldRaw(values?.ar_cond)),
      freio: normalizeBrake(getFieldRaw(values?.freio)),
      arla: getFieldRaw(values?.arla),
      consumido_delta: getFieldRaw(values?.consumido_delta),
      peso_total: getFieldRaw(values?.peso_total),
    },
    isGap: false,
    isRepeated: !!sample?.is_repeated,
    sampleState: sample?.state || "repeated",
  });
}

function rebuildMainSeriesFromBuffer(samples) {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!state) return;

  resetFullInstanceState(state);

  const ordered = Array.isArray(samples)
    ? [...samples].sort((a, b) => {
        const at = Number(a?.sample_tm || 0);
        const bt = Number(b?.sample_tm || 0);
        return at - bt;
      })
    : [];

  for (const sample of ordered) {
    if (sample?.is_gap) {
      pushGapToLiveInstanceFromSample(sample);
    } else {
      pushSampleToLiveInstanceFromSample(sample);
      if (sample?.msg_tm != null) {
        state.lastMsgTm = sample.msg_tm;
      }
    }
  }
}

async function refreshLiveSeries() {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  const state = liveInstance?.state;
  if (!liveInstance || !state) return;

  const r = await fetch(apiUrl(`/live_series?limit=${LIVE_SERIES_LIMIT}`), {
    cache: "no-store",
  });
  const j = await r.json();

  if (!r.ok || !j.ok) {
    throw new Error(j.error || `HTTP ${r.status}`);
  }

  const itemId = j?.item_id ?? null;
  const samples = Array.isArray(j?.samples) ? j.samples : [];

  if (itemId != null) {
    window.__currentItemId = itemId;
    const unitNow = document.getElementById("unitNow");
    if (unitNow) unitNow.innerText = itemId;
  }

  rebuildMainSeriesFromBuffer(samples);
  drawChartInstance(liveInstance);
  maybeScrollChartToEndForInstance(liveInstance);
}

async function refreshStatusCards() {
  const r = await fetch(endpoint, { cache: "no-store" });
  const j = await r.json();

  const meta = document.getElementById("meta");
  const statusBadges = document.getElementById("statusBadges");

  const ok = !!j.ok;
  const err = j.error;

  const data = j?.data || {};
  const itemId = data?.item_id;
  const stale = !!data?.stale;
  const age = data?.age_sec;
  const hasNewSample = !!data?.has_new_sample;
  const sampleState = data?.sample_state || "repeated";

  if (itemId != null) {
    window.__currentItemId = itemId;
    const unitNow = document.getElementById("unitNow");
    if (unitNow) unitNow.innerText = itemId;
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

    if (statusSnapshot) {
      if (stale) {
        statusSnapshot.innerText = "Sem sinal";
      } else if (hasNewSample) {
        statusSnapshot.innerText = "Novo";
      } else {
        statusSnapshot.innerText = "Mantido";
      }
    }

    const replayCount = getReplayChartInstances().length;
    const badges = [];

    badges.push(badgeHtml("Backend OK", "badge-ok"));

    if (replayCount > 0) {
      badges.push(
        badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn")
      );
    }

    if (stale) {
      badges.push(badgeHtml("Unidade sem sinal", "badge-bad"));
      badges.push(badgeHtml("Sem nova amostra", "badge-bad"));
    } else {
      badges.push(badgeHtml("Unidade online", "badge-ok"));

      if (sampleState === "new") {
        badges.push(badgeHtml("Nova amostra", "badge-ok"));
      } else {
        badges.push(badgeHtml("Amostra mantida", "badge-warn"));
      }
    }

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
        replayCount > 0
          ? badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn")
          : "",
      ]
        .filter(Boolean)
        .join("");
    }

    if (meta) {
      meta.innerHTML = `<span class="bad">ERRO</span> • ${err ?? "sem detalhe"} • ${ts}`;
    }
  }
}

async function tick() {
  try {
    await refreshStatusCards();
    await refreshLiveSeries();
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
        replayCount > 0
          ? badgeHtml(`${replayCount} replay${replayCount > 1 ? "s" : ""}`, "badge-warn")
          : "",
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

        await tick();
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