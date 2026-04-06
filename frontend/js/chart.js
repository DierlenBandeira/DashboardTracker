// chart.js

function getResolvedInstance(instanceLike = null) {
  if (instanceLike && instanceLike.state) return instanceLike;
  if (instanceLike && instanceLike.id) return getChartInstanceById(instanceLike.id) || instanceLike;
  return getActiveChartInstance() || getLiveChartInstance() || mainChartInstance || null;
}

function getResolvedState(instanceLike = null) {
  const instance = getResolvedInstance(instanceLike);
  return instance?.state || null;
}

function getMainChartDomFallbacks() {
  return {
    wrap: document.getElementById("chartWrap"),
    canvas: document.getElementById("chart"),
    tip: document.getElementById("tip"),
    meta: document.getElementById("chartMeta"),
    popover: document.getElementById("commentPopover"),
    commentInput: document.getElementById("commentInput"),
    commentTimeLabel: document.getElementById("commentTimeLabel"),
  };
}

function getInstanceDom(state) {
  if (!state) return getMainChartDomFallbacks();

  const fallbacks = state.mode === "live" ? getMainChartDomFallbacks() : {};

  return {
    wrap: state.dom?.wrap || fallbacks.wrap || null,
    canvas: state.dom?.canvas || fallbacks.canvas || null,
    tip: state.dom?.tip || fallbacks.tip || null,
    meta: state.dom?.meta || fallbacks.meta || null,
    popover: state.dom?.popover || fallbacks.popover || null,
    commentInput: state.dom?.commentInput || fallbacks.commentInput || null,
    commentTimeLabel: state.dom?.commentTimeLabel || fallbacks.commentTimeLabel || null,
    status: state.dom?.status || null,
    current: state.dom?.current || null,
    zoomLabel: state.dom?.zoomLabel || null,
  };
}

function getStateBlockBandLayout(hit) {
  const visibleBlockSeries = hit.series.filter((s) => STATE_BLOCK_SERIES.has(s.key));

  if (!visibleBlockSeries.length) {
    return {
      visibleBlockSeries,
      singleMode: false,
      getBandTop: () => hit.M.t,
      getBandHeight: () => 12,
      getBandCenterY: () => hit.M.t + 6,
    };
  }

  if (visibleBlockSeries.length === 1) {
    return {
      visibleBlockSeries,
      singleMode: true,
      getBandTop: () => hit.M.t,
      getBandHeight: () => hit.plotH,
      getBandCenterY: () => hit.M.t + hit.plotH / 2,
    };
  }

  const topOffset = 26;
  const rowGap = 18;
  const bandHeight = 12;

  return {
    visibleBlockSeries,
    singleMode: false,
    getBandTop: (seriesKey) => {
      const idx = visibleBlockSeries.findIndex((s) => s.key === seriesKey);
      return hit.M.t + topOffset + idx * rowGap;
    },
    getBandHeight: () => bandHeight,
    getBandCenterY: (seriesKey) => {
      const idx = visibleBlockSeries.findIndex((s) => s.key === seriesKey);
      return hit.M.t + topOffset + idx * rowGap + bandHeight / 2;
    },
  };
}

function buildRenderableData(instanceLike = null) {
  const state = getResolvedState(instanceLike);
  if (!state) return { seriesData: {}, gapFlags: [] };

  const lastSample = state.samples[state.samples.length - 1] || null;
  const cacheKey = [
    state.mode,
    state.samples.length,
    lastSample?.msgTm ?? "",
    lastSample?.t ?? "",
    lastSample?.isGap ? 1 : 0,
  ].join("|");

  if (state.renderDataCacheKey === cacheKey && state.renderDataCache) {
    return state.renderDataCache;
  }

  const allSeries = getAllSeries(state.mode);
  const seriesData = {};
  for (const s of allSeries) seriesData[s.key] = new Array(state.samples.length);

  const gapFlags = new Array(state.samples.length);

  for (let i = 0; i < state.samples.length; i++) {
    const smp = state.samples[i];
    gapFlags[i] = !!smp?.isGap;

    for (const s of allSeries) {
      seriesData[s.key][i] = smp?.values?.[s.key] ?? null;
    }
  }

  const renderableData = { seriesData, gapFlags };
  state.renderDataCacheKey = cacheKey;
  state.renderDataCache = renderableData;
  return renderableData;
}

function getChartXZoom(state) {
  return clamp(Number(state?.xZoom) || DEFAULT_X_ZOOM, MIN_X_ZOOM, MAX_X_ZOOM);
}

function formatChartZoom(state) {
  const rawPct = getChartXZoom(state) * 100;
  const pct = state?.mode === "replay" ? Math.round(rawPct / 5) * 5 : Math.round(rawPct);
  return `${pct}%`;
}

function getCanvasWidthCapForState(state) {
  if (state?.mode !== "replay") return MAX_CANVAS_CSS_WIDTH;

  const zoomFactor = getChartXZoom(state);
  const replayCap = Math.round(MAX_CANVAS_CSS_WIDTH * zoomFactor);
  return clamp(replayCap, MAX_CANVAS_CSS_WIDTH, MAX_REPLAY_CANVAS_CSS_WIDTH);
}

function updateChartZoomUi(instanceLike = null) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  if (dom.zoomLabel) {
    dom.zoomLabel.textContent = formatChartZoom(state);
  }
}

function getReplayAxisTickCount(state, totalSlots) {
  if (!totalSlots) return 0;
  if (state?.mode !== "replay") return Math.min(6, totalSlots);

  const zoomFactor = getChartXZoom(state);
  const tickCount = Math.round(6 + (zoomFactor - 1) * 6);
  return Math.min(totalSlots, Math.max(6, tickCount));
}

function getEffectivePxPerPoint(totalPoints, zoomFactor = DEFAULT_X_ZOOM, widthCap = MAX_CANVAS_CSS_WIDTH) {
  if (totalPoints <= 0) return BASE_PX_PER_POINT * zoomFactor;

  const naturalWidth = totalPoints * BASE_PX_PER_POINT * zoomFactor;
  if (naturalWidth <= widthCap) return BASE_PX_PER_POINT * zoomFactor;

  const compressed = widthCap / totalPoints;
  return Math.max(MIN_PX_PER_POINT, compressed);
}

function getSafeDevicePixelRatio(state = null, cssWidth = 0) {
  const dpr = window.devicePixelRatio || 1;
  if (state?.mode === "replay") {
    let safeDpr = Math.min(dpr, MAX_REPLAY_DEVICE_PIXEL_RATIO);

    if (cssWidth >= LARGE_REPLAY_CANVAS_CSS_WIDTH) {
      safeDpr = Math.min(safeDpr, 0.8);
    }

    if (cssWidth > 0 && MAX_CANVAS_PIXEL_WIDTH > 0) {
      const pixelBoundDpr = MAX_CANVAS_PIXEL_WIDTH / cssWidth;
      if (Number.isFinite(pixelBoundDpr) && pixelBoundDpr > 0) {
        safeDpr = Math.min(safeDpr, pixelBoundDpr);
      }
    }

    return Math.max(MIN_REPLAY_DEVICE_PIXEL_RATIO, safeDpr);
  }

  return Math.min(dpr, MAX_DEVICE_PIXEL_RATIO);
}

function setupCanvasForInstance(instanceLike = null) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const canvas = dom.canvas;
  const wrap = dom.wrap;

  if (!canvas) return null;

  const visibleWidth = wrap ? Math.max(1, Math.floor(wrap.clientWidth)) : 1200;
  const totalPoints = Math.max(state.samples.length, MIN_VISIBLE_POINTS);
  const zoomFactor = getChartXZoom(state);
  const widthCap = getCanvasWidthCapForState(state);

  const preferredPxPerPoint = getEffectivePxPerPoint(totalPoints, zoomFactor, widthCap);
  const desiredCssWidth = Math.max(visibleWidth, Math.ceil(totalPoints * preferredPxPerPoint));
  const cssW = Math.min(desiredCssWidth, widthCap);
  const cssH = state.mode === "live" ? 560 : 560;
  const pxPerPoint = totalPoints > 0 ? cssW / totalPoints : preferredPxPerPoint;
  const dpr = getSafeDevicePixelRatio(state, cssW);

  if (canvas.style.width !== `${cssW}px`) canvas.style.width = cssW + "px";
  if (canvas.style.height !== `${cssH}px`) canvas.style.height = cssH + "px";

  const pxW = Math.floor(cssW * dpr);
  const pxH = Math.floor(cssH * dpr);

  if (canvas.width !== pxW || canvas.height !== pxH) {
    canvas.width = pxW;
    canvas.height = pxH;
  }

  const ctx = canvas.getContext("2d", { alpha: false });
  if (!ctx) return null;

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  return {
    ctx,
    W: cssW,
    H: cssH,
    dpr,
    pxPerPoint,
    zoomFactor,
    totalPoints,
  };
}

function setChartXZoomInstance(instanceLike, nextZoom, options = {}) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  const dom = getInstanceDom(state);
  const wrap = dom.wrap;

  if (!instance || !state || !wrap || state.mode !== "replay") return;

  if (state.pendingZoomFrame) {
    cancelAnimationFrame(state.pendingZoomFrame);
    state.pendingZoomFrame = 0;
    state.pendingZoomTarget = null;
    state.pendingZoomAnchorClientX = null;
  }

  const targetZoom = clamp(Number(nextZoom) || DEFAULT_X_ZOOM, MIN_X_ZOOM, MAX_X_ZOOM);
  const currentZoom = getChartXZoom(state);
  if (Math.abs(targetZoom - currentZoom) < 0.001) {
    updateChartZoomUi(instance);
    return;
  }

  const keepEnd = state.autoScrollToEnd;
  const wrapRect = wrap.getBoundingClientRect();
  const anchorClientX =
    typeof options.anchorClientX === "number"
      ? options.anchorClientX
      : wrapRect.left + wrap.clientWidth / 2;
  const anchorOffset = clamp(anchorClientX - wrapRect.left, 0, wrap.clientWidth);
  const oldScrollWidth = Math.max(1, wrap.scrollWidth);
  const oldAnchorRatio = (wrap.scrollLeft + anchorOffset) / oldScrollWidth;

  state.xZoom = targetZoom;
  updateChartZoomUi(instance);
  drawChartInstance(instance);

  requestAnimationFrame(() => {
    const maxScrollLeft = Math.max(0, wrap.scrollWidth - wrap.clientWidth);
    let nextScrollLeft = maxScrollLeft;

    if (!keepEnd) {
      nextScrollLeft = oldAnchorRatio * wrap.scrollWidth - anchorOffset;
    }

    wrap.scrollLeft = clamp(nextScrollLeft, 0, maxScrollLeft);
    state.autoScrollToEnd =
      wrap.scrollLeft + wrap.clientWidth >= wrap.scrollWidth - 40;
  });
}

function zoomChartXInstance(instanceLike, direction, options = {}) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state || state.mode !== "replay") return;

  const factor = direction > 0 ? X_ZOOM_STEP : 1 / X_ZOOM_STEP;
  const baseZoom =
    state.pendingZoomTarget !== null && state.pendingZoomTarget !== undefined
      ? state.pendingZoomTarget
      : getChartXZoom(state);

  state.pendingZoomTarget = clamp(baseZoom * factor, MIN_X_ZOOM, MAX_X_ZOOM);
  state.pendingZoomAnchorClientX =
    typeof options.anchorClientX === "number" ? options.anchorClientX : null;

  if (state.pendingZoomFrame) return;

  state.pendingZoomFrame = requestAnimationFrame(() => {
    const targetZoom = state.pendingZoomTarget;
    const anchorClientX = state.pendingZoomAnchorClientX;

    state.pendingZoomFrame = 0;
    state.pendingZoomTarget = null;
    state.pendingZoomAnchorClientX = null;

    setChartXZoomInstance(instance, targetZoom, { anchorClientX });
  });
}

function getYRangeFor(key, dataArr) {
  const cfg = yCfg[key] || {};
  const hasMin = cfg.min !== null && cfg.min !== undefined && cfg.min !== "";
  const hasMax = cfg.max !== null && cfg.max !== undefined && cfg.max !== "";

  if (hasMin && hasMax) {
    const min = Number(cfg.min);
    const max = Number(cfg.max);
    if (Number.isFinite(min) && Number.isFinite(max) && min !== max) {
      return { min, max, auto: false };
    }
  }

  let min = Infinity;
  let max = -Infinity;
  for (const v of dataArr) {
    if (v === null) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (min === Infinity) return { min: 0, max: 1, auto: true };
  if (min === max) return { min: min - 1, max: max + 1, auto: true };

  const pad = (max - min) * 0.12;
  return { min: min - pad, max: max + pad, auto: true };
}

function isTransitManagedSeries(seriesKey) {
  return ["velocidade", "rpm", "pct_acelerado"].includes(seriesKey);
}

function isTransitIntense(seriesKey, value) {
  const cfg = transitManageCfg?.[seriesKey];
  if (!cfg?.enabled) return false;
  if (value === null || value === undefined) return false;

  const num = Number(value);
  if (!Number.isFinite(num)) return false;

  switch (cfg.rule) {
    case "below":
      return num < Number(cfg.value);
    case "lte":
      return num <= Number(cfg.value);
    case "gte":
      return num >= Number(cfg.value);
    case "above":
      return num > Number(cfg.value);
    case "outside_range": {
      const min = Number(cfg.min);
      const max = Number(cfg.max);
      if (!Number.isFinite(min) || !Number.isFinite(max)) return false;
      return num < min || num > max;
    }
    case "inside_range": {
      const min = Number(cfg.min);
      const max = Number(cfg.max);
      if (!Number.isFinite(min) || !Number.isFinite(max)) return false;
      return num >= min && num <= max;
    }
    default:
      return false;
  }
}

function getTransitState(seriesKey, value) {
  if (!isTransitManagedSeries(seriesKey)) return "normal";
  return isTransitIntense(seriesKey, value) ? "intenso" : "normal";
}

function getTransitDashFromState(state) {
  return state === "intenso" ? [3, 2] : [];
}

function getTransitCutLevels(seriesKey) {
  const cfg = transitManageCfg?.[seriesKey];
  if (!cfg?.enabled) return [];

  switch (cfg.rule) {
    case "below":
    case "lte":
    case "gte":
    case "above":
      return [Number(cfg.value)].filter(Number.isFinite);
    case "outside_range":
    case "inside_range":
      return [Number(cfg.min), Number(cfg.max)].filter(Number.isFinite).sort((a, b) => a - b);
    default:
      return [];
  }
}

function interpolateTransitPoint(p1, p2, level) {
  const y1 = Number(p1.v);
  const y2 = Number(p2.v);

  if (!Number.isFinite(y1) || !Number.isFinite(y2)) return null;
  if (y1 === y2) return null;

  const t = (level - y1) / (y2 - y1);
  if (!(t > 0 && t < 1)) return null;

  return {
    x: p1.x + (p2.x - p1.x) * t,
    y: p1.y + (p2.y - p1.y) * t,
    v: level,
    t,
  };
}

function buildTransitSubsegments(seriesKey, prevPoint, point) {
  const cutLevels = getTransitCutLevels(seriesKey);
  const cuts = [];

  for (const level of cutLevels) {
    const crossing =
      (prevPoint.v < level && point.v > level) ||
      (prevPoint.v > level && point.v < level);

    if (!crossing) continue;

    const cutPoint = interpolateTransitPoint(prevPoint, point, level);
    if (cutPoint) cuts.push(cutPoint);
  }

  cuts.sort((a, b) => a.t - b.t);

  const nodes = [prevPoint, ...cuts, point];
  const segments = [];

  for (let i = 0; i < nodes.length - 1; i++) {
    const a = nodes[i];
    const b = nodes[i + 1];
    const midValue = (Number(a.v) + Number(b.v)) / 2;
    const state = getTransitState(seriesKey, midValue);

    segments.push({
      from: a,
      to: b,
      state,
      dash: getTransitDashFromState(state),
    });
  }

  return segments;
}

function showTipForInstance(instanceLike, x, y, html) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const tip = dom.tip;
  const chartWrap = dom.wrap;

  if (!tip || !chartWrap) return;

  tip.innerHTML = html;
  tip.style.display = "block";

  const tw = tip.offsetWidth;
  const th = tip.offsetHeight;

  const scrollLeft = chartWrap.scrollLeft;
  const visibleLeft = scrollLeft;
  const visibleRight = scrollLeft + chartWrap.clientWidth;

  let left = x + 12;
  let top = y + 12;

  const minL = visibleLeft + 6;
  const maxL = visibleRight - tw - 6;
  const maxT = chartWrap.clientHeight - th - 6;

  left = clamp(left, minL, Math.max(minL, maxL));
  top = clamp(top, 6, Math.max(6, maxT));

  tip.style.left = left + "px";
  tip.style.top = top + "px";
}

function hideTipForInstance(instanceLike, force = false) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const tip = dom.tip;

  if (!tip) return;
  if (!force && state?.selectedIdx !== null) return;

  tip.style.display = "none";
}

function maybeScrollChartToEndForInstance(instanceLike, force = false) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const wrap = dom.wrap;

  if (!state || !wrap) return;

  if (force || state.autoScrollToEnd) {
    requestAnimationFrame(() => {
      wrap.scrollLeft = Math.max(0, wrap.scrollWidth - wrap.clientWidth);
    });
  }
}

function preserveScrollDuringDraw(instanceLike = null) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const wrap = dom.wrap;

  if (!state || !wrap) return () => {};

  const previousLeft = wrap.scrollLeft;
  const shouldKeepManualPosition = !state.autoScrollToEnd;

  return () => {
    if (shouldKeepManualPosition) {
      wrap.scrollLeft = previousLeft;
    }
  };
}

function getSampleAtVisualIndex(instanceLike, idx) {
  const state = getResolvedState(instanceLike);
  if (!state) return null;

  const totalSlots = Math.max(state.samples.length, MIN_VISIBLE_POINTS);
  const slotOffset = totalSlots - state.samples.length;
  const sampleIdx = idx - slotOffset;
  if (sampleIdx < 0 || sampleIdx >= state.samples.length) return null;
  return state.samples[sampleIdx];
}

function drawChartInstance(instanceLike = null) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state || state.isDrawing) return;

  state.isDrawing = true;

  try {
    const dom = getInstanceDom(state);
    const canvas = dom.canvas;
    if (!canvas) return;

    const restoreScroll = preserveScrollDuringDraw(instance);
    const setup = setupCanvasForInstance(instance);
    if (!setup) return;

    const { ctx, W, H, pxPerPoint } = setup;
    restoreScroll();
    updateChartZoomUi(instance);

    ctx.clearRect(0, 0, W, H);

    const { seriesData, gapFlags } = buildRenderableData(instance);
    const visibleSeries = getVisibleSeries(state.mode);

    const totalSlots = Math.max(state.samples.length, MIN_VISIBLE_POINTS);
    const slotOffset = totalSlots - state.samples.length;

    const M = { l: 20, r: 20, t: 12, b: 42 };
    const plotW = W - M.l - M.r;
    const plotH = H - M.t - M.b;

    const xAt = (i) => {
      if (totalSlots <= 1) return M.l;
      return M.l + (plotW * (i / (totalSlots - 1)));
    };

    const yAtNorm = (n) => M.t + plotH * (1 - n);

    state.HIT = {
      mode: "single_norm",
      W,
      H,
      M,
      plotW,
      plotH,
      xAt,
      series: [],
      count: state.samples.length,
      totalSlots,
      slotOffset,
    };

    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, W, H);

    ctx.strokeStyle = "#f0f0f0";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const yy = M.t + (plotH * i) / 4;
      ctx.beginPath();
      ctx.moveTo(M.l, yy);
      ctx.lineTo(W - M.r, yy);
      ctx.stroke();
    }

    let inGap = false;
    let gapStart = null;

    for (let i = 0; i < state.samples.length; i++) {
      const isGap = gapFlags[i];

      if (isGap && !inGap) {
        inGap = true;
        gapStart = i;
      }

      const isLast = i === state.samples.length - 1;
      if (inGap && (!isGap || isLast)) {
        const gapEnd = isGap && isLast ? i : i - 1;
        const x1 = xAt(slotOffset + gapStart);
        const x2 = xAt(slotOffset + gapEnd);
        const gapCount = gapEnd - gapStart + 1;
        const gapSeconds = gapCount * STEP_SEC;

        if (gapSeconds >= 15) {
          const width = Math.max(6, x2 - x1);
          ctx.fillStyle = "rgba(220, 38, 38, 0.10)";
          ctx.fillRect(x1, M.t, width, plotH);

          ctx.strokeStyle = "rgba(220, 38, 38, 0.25)";
          ctx.lineWidth = 1;
          ctx.strokeRect(x1, M.t, width, plotH);

          if (width > 70) {
            ctx.fillStyle = "rgba(153, 27, 27, 0.9)";
            ctx.font = "12px Arial";
            ctx.textAlign = "left";
            ctx.textBaseline = "alphabetic";
            ctx.fillText("Falha de sinal", x1 + 8, M.t + 18);
          }
        }

        inGap = false;
        gapStart = null;
      }
    }

    if (!visibleSeries.length) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "600 16px Arial";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("Nenhuma série selecionada", W / 2, H / 2);

      if (dom.meta) {
        dom.meta.innerText =
          `Leituras: ${state.samples.length} • base visual mínima: ${MIN_VISIBLE_POINTS} pontos • comentários: ${Object.keys(state.commentsByMsgTm).length}`;
      }
      return;
    }

    for (const s of visibleSeries) {
      const arr = seriesData[s.key];
      const { min, max } = getYRangeFor(s.key, arr);
      state.HIT.series.push({ key: s.key, label: s.label, color: s.color, arr, min, max });
    }

    const stateBandLayout = getStateBlockBandLayout(state.HIT);

    for (const s of visibleSeries) {
      const hitSeries = state.HIT.series.find((it) => it.key === s.key);
      if (!hitSeries) continue;

      const arr = hitSeries.arr;
      const min = hitSeries.min;
      const max = hitSeries.max;
      const denom = (max - min) || 1;

      if (STATE_BLOCK_SERIES.has(s.key)) {
        let blockStart = null;

        for (let i = 0; i < arr.length; i++) {
          const v = arr[i];
          const isOn = Number(v) === 1;
          const isLast = i === arr.length - 1;

          if (isOn && blockStart === null) {
            blockStart = i;
          }

          if (blockStart !== null && (!isOn || isLast)) {
            const blockEnd = isOn && isLast ? i : i - 1;
            const x1 = xAt(slotOffset + blockStart);
            const x2 = xAt(slotOffset + blockEnd);
            const width = Math.max(6, x2 - x1 + 2);

            const bandTop = stateBandLayout.getBandTop(s.key);
            const bandHeight = stateBandLayout.getBandHeight(s.key);

            ctx.fillStyle = s.color + "33";
            ctx.fillRect(x1, bandTop, width, bandHeight);

            ctx.strokeStyle = s.color;
            ctx.lineWidth = 1;
            ctx.strokeRect(x1, bandTop, width, bandHeight);

            if (!stateBandLayout.singleMode && width > 48) {
              ctx.fillStyle = s.color;
              ctx.font = "11px Arial";
              ctx.textAlign = "left";
              ctx.textBaseline = "middle";
              ctx.fillText(s.label, x1 + 6, bandTop + bandHeight / 2);
            }

            blockStart = null;
          }
        }

        continue;
      }

      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;

      let prevPoint = null;

      for (let i = 0; i < arr.length; i++) {
        const v = arr[i];
        if (v === null || v === undefined) {
          prevPoint = null;
          continue;
        }

        let n = (v - min) / denom;
        if (!Number.isFinite(n)) {
          prevPoint = null;
          continue;
        }

        n = Math.max(0, Math.min(1, n));

        const point = {
          i,
          v: Number(v),
          x: xAt(slotOffset + i),
          y: yAtNorm(n),
        };

        if (prevPoint) {
          const subsegments = buildTransitSubsegments(s.key, prevPoint, point);

          for (const segment of subsegments) {
            ctx.beginPath();
            ctx.setLineDash(segment.dash);
            ctx.moveTo(segment.from.x, segment.from.y);
            ctx.lineTo(segment.to.x, segment.to.y);
            ctx.stroke();
          }
        }

        prevPoint = point;
      }

      ctx.setLineDash([]);

      const pointStep =
        state.samples.length > 12000 ? 24 :
        state.samples.length > 8000 ? 16 :
        state.samples.length > 5000 ? 10 :
        state.samples.length > 3000 ? 6 :
        state.samples.length > 1500 ? 3 : 1;

      const pointRadius =
        pxPerPoint <= 3 ? 0 :
        pxPerPoint <= 5 ? 1 : 2;

      if (state.mode !== "replay" && pointRadius > 0) {
        ctx.fillStyle = s.color;
        for (let i = 0; i < arr.length; i += pointStep) {
          const v = arr[i];
          if (v === null || v === undefined) continue;

          let n = (v - min) / denom;
          if (!Number.isFinite(n)) continue;
          n = Math.max(0, Math.min(1, n));

          ctx.beginPath();
          ctx.arc(xAt(slotOffset + i), yAtNorm(n), pointRadius, 0, Math.PI * 2);
          ctx.fill();
        }
      }
    }

    for (let i = 0; i < state.samples.length; i++) {
      const sample = state.samples[i];
      if (!sample || sample.isGap || sample.msgTm == null) continue;
      if (!state.commentsByMsgTm[sample.msgTm]) continue;

      const x = xAt(slotOffset + i);
      const y = M.t + 14;

      ctx.beginPath();
      ctx.fillStyle = state.selectedMsgTm === sample.msgTm ? "#2563eb" : "#111827";
      ctx.arc(x, y, 7, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = "#ffffff";
      ctx.font = "bold 10px Arial";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("i", x, y + 0.5);
    }

    if (state.HOVER && state.HOVER.idx !== null && state.HOVER.x !== null) {
      ctx.strokeStyle = "rgba(0,0,0,0.18)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(state.HOVER.x, M.t);
      ctx.lineTo(state.HOVER.x, M.t + plotH);
      ctx.stroke();

      if (state.HOVER.y !== null) {
        ctx.strokeStyle = "rgba(0,0,0,0.12)";
        ctx.beginPath();
        ctx.moveTo(M.l, state.HOVER.y);
        ctx.lineTo(W - M.r, state.HOVER.y);
        ctx.stroke();
      }
    }

    const axisTickCount = getReplayAxisTickCount(state, totalSlots);
    ctx.fillStyle = "#6b7280";
    ctx.font = "11px Arial";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";

    for (let k = 0; k < axisTickCount; k++) {
      const visualIdx =
        axisTickCount === 1
          ? totalSlots - 1
          : Math.round((k / (axisTickCount - 1)) * (totalSlots - 1));

      const sample = getSampleAtVisualIndex(instance, visualIdx);
      const x = xAt(visualIdx);
      const y = M.t + plotH + 8;

      ctx.strokeStyle = "#d1d5db";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, M.t + plotH);
      ctx.lineTo(x, M.t + plotH + 4);
      ctx.stroke();

      const label = sample?.msgTm ? formatTimestampLabel(sample.msgTm) : "";
      if (label) ctx.fillText(label, x, y);
    }

    if (dom.meta) {
      const zoomText =
        state.mode === "replay"
          ? ` • zoom X: ${formatChartZoom(state)} • Ctrl + roda para zoom`
          : "";
      dom.meta.innerText =
        `Leituras: ${state.samples.length} • base visual mínima: ${MIN_VISIBLE_POINTS} pontos • densidade adaptativa ativa${zoomText} • comentários: ${Object.keys(state.commentsByMsgTm).length}`;
    }

    if (dom.status) dom.status.textContent = state.mode === "replay" ? "Pronto" : "Online";
    if (dom.current) dom.current.textContent = String(state.replayIndex ?? 0);
  } finally {
    state.isDrawing = false;
  }
}

function drawAllCharts() {
  chartInstances.forEach((instance) => drawChartInstance(instance));
}

function getMousePositionOnChart(instanceLike, ev) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  const chartWrap = dom.wrap;
  if (!chartWrap) return null;

  const wrapRect = chartWrap.getBoundingClientRect();

  return {
    mx: ev.clientX - wrapRect.left + chartWrap.scrollLeft,
    my: ev.clientY - wrapRect.top,
    wrapRect,
    chartWrap,
  };
}

function findNearestRenderablePointInstance(instanceLike, ev) {
  const state = getResolvedState(instanceLike);
  const pos = getMousePositionOnChart(instanceLike, ev);

  if (!state || !pos || !state.HIT || state.HIT.mode !== "single_norm" || state.HIT.count <= 0 || !state.HIT.series.length) {
    return null;
  }

  const { mx, my } = pos;
  const { M, plotW, xAt, totalSlots, slotOffset } = state.HIT;

  const t = (mx - M.l) / plotW;
  const visualIdx = Math.round(clamp(t, 0, 1) * (totalSlots - 1));
  const idx = visualIdx - slotOffset;

  if (idx < 0 || idx >= state.samples.length) return null;

  let best = null;
  const stateBandLayout = getStateBlockBandLayout(state.HIT);

  for (const s of state.HIT.series) {
    const v = s.arr[idx];
    if (v === null || v === undefined) continue;

    const px = xAt(visualIdx);
    let py;

    if (STATE_BLOCK_SERIES.has(s.key)) {
      py = stateBandLayout.getBandCenterY(s.key);
    } else {
      const denom = (s.max - s.min) || 1;
      let n = (v - s.min) / denom;
      if (!Number.isFinite(n)) continue;
      n = Math.max(0, Math.min(1, n));
      py = M.t + state.HIT.plotH * (1 - n);
    }

    const dx = mx - px;
    const dy = my - py;
    const dist2 = dx * dx + dy * dy;

    if (!best || dist2 < best.dist2) {
      best = { series: s, v, px, py, dist2, visualIdx, idx, mx, my, wrapRect: pos.wrapRect, chartWrap: pos.chartWrap };
    }
  }

  return best;
}

function onMoveInstance(instanceLike, ev) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state) return;

  setActiveChartInstance(instance.id);

  const best = findNearestRenderablePointInstance(instance, ev);

  if (!best) {
    if (state.HOVER.idx !== null || state.HOVER.seriesKey !== null) {
      state.HOVER = { idx: null, x: null, y: null, seriesKey: null };
      drawChartInstance(instance);
    }
    hideTipForInstance(instance);
    return;
  }

  const sameHover =
    state.HOVER.idx === best.visualIdx &&
    state.HOVER.seriesKey === best.series.key &&
    state.HOVER.x === best.px &&
    state.HOVER.y === best.py;

  if (!sameHover) {
    state.HOVER = {
      idx: best.visualIdx,
      x: best.px,
      y: best.py,
      seriesKey: best.series.key,
    };
    drawChartInstance(instance);
  }

  const hoveredSample = getSampleAtVisualIndex(instance, best.visualIdx);
  const comment = hoveredSample?.msgTm != null ? state.commentsByMsgTm[hoveredSample.msgTm] : null;
  const timeLabel = hoveredSample?.msgTm != null ? formatTimestampFull(hoveredSample.msgTm) : "Sem timestamp";

  const html = `
    <div><b>${best.series.label}</b></div>
    <div style="margin-top:4px;">Valor: <b>${escapeHtml(formatValueForTip(best.series.key, best.v))}</b></div>
    <div style="margin-top:4px;opacity:.9;">Timestamp: ${timeLabel}</div>
    ${
      comment
        ? `<div style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.15);"><b>Comentário:</b><br>${escapeHtml(comment)}</div>`
        : `<div style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.15);opacity:.85;">Clique no ponto para adicionar comentário</div>`
    }
  `;

  showTipForInstance(instance, best.px, best.py, html);
}

function onLeaveInstance(instanceLike) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state) return;

  state.HOVER = { idx: null, x: null, y: null, seriesKey: null };
  drawChartInstance(instance);
  hideTipForInstance(instance);
}

function onChartClickInstance(instanceLike, ev) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state) return;

  if (Date.now() - state.lastScrollEventAt < 180) return;
  if (state.pointerDownInfo?.moved) return;

  const best = findNearestRenderablePointInstance(instance, ev);
  if (!best) return;

  const sample = getSampleAtVisualIndex(instance, best.visualIdx);
  if (!sample || sample.isGap || sample.msgTm == null) return;

  const CLICK_RADIUS = 12;
  const MAX_X_DISTANCE = 10;

  const dx = Math.abs(best.mx - best.px);
  if (dx > MAX_X_DISTANCE) return;
  if (best.dist2 > CLICK_RADIUS * CLICK_RADIUS) return;

  ev.preventDefault();

  setActiveChartInstance(instance.id);

  if (typeof openCommentPopoverInstance === "function") {
    openCommentPopoverInstance(
      instance,
      best.visualIdx,
      ev.clientX - best.wrapRect.left + best.chartWrap.scrollLeft,
      ev.clientY - best.wrapRect.top
    );
    return;
  }

  if (typeof openCommentPopover === "function") {
    openCommentPopover(
      best.visualIdx,
      ev.clientX - best.wrapRect.left + best.chartWrap.scrollLeft,
      ev.clientY - best.wrapRect.top
    );
  }
}

function attachChartEvents(instanceLike) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  const dom = getInstanceDom(state);

  if (!instance || !state || !dom.wrap || !dom.canvas) return;

  if (dom.wrap.dataset.chartBound === "1") return;

  dom.wrap.addEventListener("scroll", () => {
    state.lastScrollEventAt = Date.now();
    const nearEnd = dom.wrap.scrollLeft + dom.wrap.clientWidth >= dom.wrap.scrollWidth - 40;
    state.autoScrollToEnd = nearEnd;
  });

  dom.wrap.addEventListener("mousedown", (ev) => {
    setActiveChartInstance(instance.id);
    state.pointerDownInfo = {
      x: ev.clientX,
      y: ev.clientY,
      moved: false,
    };
  });

  dom.wrap.addEventListener("mousemove", (ev) => {
    if (state.pointerDownInfo) {
      const dx = Math.abs(ev.clientX - state.pointerDownInfo.x);
      const dy = Math.abs(ev.clientY - state.pointerDownInfo.y);
      if (dx > 5 || dy > 5) {
        state.pointerDownInfo.moved = true;
      }
    }
  });

  dom.wrap.addEventListener("mousemove", (ev) => onMoveInstance(instance, ev));
  dom.wrap.addEventListener("mouseleave", () => onLeaveInstance(instance));
  dom.wrap.addEventListener("wheel", (ev) => {
    if (state.mode !== "replay") return;
    if (!(ev.ctrlKey || ev.metaKey)) return;

    ev.preventDefault();
    setActiveChartInstance(instance.id);
    zoomChartXInstance(instance, ev.deltaY < 0 ? 1 : -1, {
      anchorClientX: ev.clientX,
    });
  }, { passive: false });
  dom.canvas.addEventListener("click", (ev) => onChartClickInstance(instance, ev));

  dom.wrap.dataset.chartBound = "1";
}

function detachChartEvents(instanceLike) {
  const state = getResolvedState(instanceLike);
  const dom = getInstanceDom(state);
  if (dom.wrap) delete dom.wrap.dataset.chartBound;
}

function bindMainChartDom() {
  bindDomToState(MAIN_CHART_STATE, getMainChartDomFallbacks());
  attachChartEvents(mainChartInstance);
}

function destroyChartInstance(instanceLike) {
  const instance = getResolvedInstance(instanceLike);
  const state = getResolvedState(instance);
  if (!instance || !state) return;

  clearInstanceReplayTimer(state);
  if (state.pendingZoomFrame) {
    cancelAnimationFrame(state.pendingZoomFrame);
    state.pendingZoomFrame = 0;
    state.pendingZoomTarget = null;
    state.pendingZoomAnchorClientX = null;
  }
  detachChartEvents(instance);
  unregisterChartInstance(instance.id);
}

function drawChart(target = null) {
  if (target) {
    drawChartInstance(target);
    return;
  }
  drawAllCharts();
}

function findNearestRenderablePoint(ev) {
  return findNearestRenderablePointInstance(getLiveChartInstance() || mainChartInstance, ev);
}

function onMove(ev) {
  onMoveInstance(getLiveChartInstance() || mainChartInstance, ev);
}

function onLeave() {
  onLeaveInstance(getLiveChartInstance() || mainChartInstance);
}

function onChartClick(ev) {
  onChartClickInstance(getLiveChartInstance() || mainChartInstance, ev);
}

function showTip(x, y, html) {
  showTipForInstance(getActiveChartInstance() || getLiveChartInstance() || mainChartInstance, x, y, html);
}

function hideTip(force = false) {
  hideTipForInstance(getActiveChartInstance() || getLiveChartInstance() || mainChartInstance, force);
}

function maybeScrollChartToEnd(force = false) {
  maybeScrollChartToEndForInstance(getLiveChartInstance() || mainChartInstance, force);
}

window.clearChart = function () {
  const liveInstance = getLiveChartInstance() || mainChartInstance;
  if (!liveInstance) return;

  resetChartStateForInstance(liveInstance.state);

  if (typeof closeCommentPopoverInstance === "function") {
    closeCommentPopoverInstance(liveInstance, true);
  } else if (typeof closeCommentPopover === "function") {
    closeCommentPopover();
  }

  drawChartInstance(liveInstance);
  maybeScrollChartToEndForInstance(liveInstance, true);
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bindMainChartDom);
} else {
  bindMainChartDom();
}
