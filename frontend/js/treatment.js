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

  function initSpeedThresholdUX() {
    const lowInput = document.getElementById("speedLowMax");
    const mediumInput = document.getElementById("speedMediumMax");
    const highHidden = document.getElementById("speedHighMin");

    const lowPreview = document.getElementById("speedLowPreview");
    const mediumFromPreview = document.getElementById("speedMediumFromPreview");
    const mediumToPreview = document.getElementById("speedMediumToPreview");
    const highPreview = document.getElementById("speedHighPreview");
    const highLegendPreview = document.getElementById("speedHighLegendPreview");

    const segLow = document.getElementById("speedSegLow");
    const segMedium = document.getElementById("speedSegMedium");
    const segHigh = document.getElementById("speedSegHigh");

    const slider = document.getElementById("speedRangeSlider");
    const lowHandle = document.getElementById("speedLowHandle");
    const mediumHandle = document.getElementById("speedMediumHandle");

    if (
      !lowInput || !mediumInput || !highHidden ||
      !lowPreview || !mediumFromPreview || !mediumToPreview ||
      !highPreview || !highLegendPreview ||
      !segLow || !segMedium || !segHigh ||
      !slider || !lowHandle || !mediumHandle
    ) return;

    const SCALE_MIN = 0;
    const SCALE_MAX = 30;
    const STEP = 0.1;
    const MIN_GAP = STEP;
    let activeHandle = null;

    function clamp(value, min, max) {
      return Math.min(Math.max(value, min), max);
    }

    function snap(value) {
      return Math.round(value / STEP) * STEP;
    }

    function formatValue(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "0";
      return Number.isInteger(num) ? String(num) : num.toFixed(1);
    }

    function valueToPercent(value) {
      return ((value - SCALE_MIN) / (SCALE_MAX - SCALE_MIN)) * 100;
    }

    function clientXToValue(clientX) {
      const rect = slider.getBoundingClientRect();
      const ratio = clamp((clientX - rect.left) / rect.width, 0, 1);
      return snap(SCALE_MIN + ratio * (SCALE_MAX - SCALE_MIN));
    }

    function applyValues(low, medium, source) {
      low = snap(clamp(low, SCALE_MIN, SCALE_MAX - MIN_GAP));
      medium = snap(clamp(medium, SCALE_MIN + MIN_GAP, SCALE_MAX));

      if (medium - low < MIN_GAP) {
        if (source === "low") {
          low = medium - MIN_GAP;
        } else {
          medium = low + MIN_GAP;
        }
      }

      low = clamp(low, SCALE_MIN, SCALE_MAX - MIN_GAP);
      medium = clamp(medium, SCALE_MIN + MIN_GAP, SCALE_MAX);

      lowInput.value = formatValue(low);
      mediumInput.value = formatValue(medium);
      highHidden.value = formatValue(medium);

      lowPreview.textContent = formatValue(low);
      mediumFromPreview.textContent = formatValue(low);
      mediumToPreview.textContent = formatValue(medium);
      highPreview.textContent = formatValue(medium);
      highLegendPreview.textContent = formatValue(medium);

      const lowPct = valueToPercent(low);
      const mediumPct = valueToPercent(medium);

      segLow.style.left = "0%";
      segLow.style.width = `${lowPct}%`;

      segMedium.style.left = `${lowPct}%`;
      segMedium.style.width = `${Math.max(mediumPct - lowPct, 0)}%`;

      segHigh.style.left = `${mediumPct}%`;
      segHigh.style.width = `${Math.max(100 - mediumPct, 0)}%`;

      lowHandle.style.left = `${lowPct}%`;
      mediumHandle.style.left = `${mediumPct}%`;
    }

    function syncFromInputs(source) {
      const low = Number(lowInput.value || SCALE_MIN);
      const medium = Number(mediumInput.value || SCALE_MAX);
      applyValues(low, medium, source);
    }

    function onDrag(clientX) {
      if (!activeHandle) return;

      const draggedValue = clientXToValue(clientX);
      const currentLow = Number(lowInput.value || SCALE_MIN);
      const currentMedium = Number(mediumInput.value || SCALE_MAX);

      if (activeHandle === "low") {
        applyValues(draggedValue, currentMedium, "low");
      } else {
        applyValues(currentLow, draggedValue, "medium");
      }
    }

    function onPointerMove(event) {
      onDrag(event.clientX);
    }

    function onPointerUp() {
      activeHandle = null;
      lowHandle.classList.remove("is-active");
      mediumHandle.classList.remove("is-active");
      document.removeEventListener("pointermove", onPointerMove);
      document.removeEventListener("pointerup", onPointerUp);
    }

    function startDrag(which, clientX) {
      activeHandle = which;
      lowHandle.classList.toggle("is-active", which === "low");
      mediumHandle.classList.toggle("is-active", which === "medium");
      onDrag(clientX);
      document.addEventListener("pointermove", onPointerMove);
      document.addEventListener("pointerup", onPointerUp);
    }

    lowHandle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("low", event.clientX);
    });

    mediumHandle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("medium", event.clientX);
    });

    slider.addEventListener("pointerdown", (event) => {
      if (event.target === lowHandle || event.target === mediumHandle) return;

      const clickedValue = clientXToValue(event.clientX);
      const currentLow = Number(lowInput.value || SCALE_MIN);
      const currentMedium = Number(mediumInput.value || SCALE_MAX);

      const closest =
        Math.abs(clickedValue - currentLow) <= Math.abs(clickedValue - currentMedium)
          ? "low"
          : "medium";

      startDrag(closest, event.clientX);
    });

    lowInput.addEventListener("input", () => syncFromInputs("low"));
    mediumInput.addEventListener("input", () => syncFromInputs("medium"));
    lowInput.addEventListener("blur", () => syncFromInputs("low"));
    mediumInput.addEventListener("blur", () => syncFromInputs("medium"));

    lowHandle.addEventListener("keydown", (event) => {
      const currentLow = Number(lowInput.value || SCALE_MIN);
      const currentMedium = Number(mediumInput.value || SCALE_MAX);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        applyValues(currentLow - STEP, currentMedium, "low");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        applyValues(currentLow + STEP, currentMedium, "low");
      }
    });

    mediumHandle.addEventListener("keydown", (event) => {
      const currentLow = Number(lowInput.value || SCALE_MIN);
      const currentMedium = Number(mediumInput.value || SCALE_MAX);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        applyValues(currentLow, currentMedium - STEP, "medium");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        applyValues(currentLow, currentMedium + STEP, "medium");
      }
    });

    applyValues(
      Number(lowInput.value || 10),
      Number(mediumInput.value || 15),
      "medium"
    );
  }

  function initTransitRpmUX() {
    const usefulStartInput = document.getElementById("rpmIntenseLow");
    const lightStartInput = document.getElementById("rpmLightMin");
    const usefulEndInput = document.getElementById("rpmLightMax");

    const intenseHighHidden = document.getElementById("rpmIntenseHigh");
    const mediumMinHidden = document.getElementById("rpmMediumMin");
    const mediumMaxHidden = document.getElementById("rpmMediumMax");

    const intenseLowPreview = document.getElementById("rpmIntenseLowPreview");
    const intenseHighPreview = document.getElementById("rpmIntenseHighPreview");
    const mediumMinPreview = document.getElementById("rpmMediumMinPreview");
    const mediumMaxPreview = document.getElementById("rpmMediumMaxPreview");
    const lightMinPreview = document.getElementById("rpmLightMinPreview");
    const lightMaxPreview = document.getElementById("rpmLightMaxPreview");

    const slider = document.getElementById("rpmTransitSlider");
    const handleStart = document.getElementById("rpmTransitHandleStart");
    const handleMiddle = document.getElementById("rpmTransitHandleMiddle");
    const handleEnd = document.getElementById("rpmTransitHandleEnd");

    const segIntenseLow = document.getElementById("rpmTransitSegIntenseLow");
    const segMedium = document.getElementById("rpmTransitSegMedium");
    const segLight = document.getElementById("rpmTransitSegLight");
    const segIntenseHigh = document.getElementById("rpmTransitSegIntenseHigh");

    if (
      !usefulStartInput || !lightStartInput || !usefulEndInput ||
      !intenseHighHidden || !mediumMinHidden || !mediumMaxHidden ||
      !intenseLowPreview || !intenseHighPreview ||
      !mediumMinPreview || !mediumMaxPreview ||
      !lightMinPreview || !lightMaxPreview ||
      !slider || !handleStart || !handleMiddle || !handleEnd ||
      !segIntenseLow || !segMedium || !segLight || !segIntenseHigh
    ) return;

    const SCALE_MIN = 0;
    const SCALE_MAX = 3000;
    const STEP = 1;
    const MIN_GAP = 1;

    let activeHandle = null;

    function clamp(value, min, max) {
      return Math.min(Math.max(value, min), max);
    }

    function snap(value) {
      return Math.round(value / STEP) * STEP;
    }

    function formatValue(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "0";
      return String(Math.round(num));
    }

    function valueToPercent(value) {
      return ((value - SCALE_MIN) / (SCALE_MAX - SCALE_MIN)) * 100;
    }

    function clientXToValue(clientX) {
      const rect = slider.getBoundingClientRect();
      const ratio = clamp((clientX - rect.left) / rect.width, 0, 1);
      return snap(SCALE_MIN + ratio * (SCALE_MAX - SCALE_MIN));
    }

    function applyValues(usefulStart, lightStart, usefulEnd, source) {
      usefulStart = snap(clamp(usefulStart, SCALE_MIN, SCALE_MAX - (MIN_GAP * 2)));
      lightStart = snap(clamp(lightStart, SCALE_MIN + MIN_GAP, SCALE_MAX - MIN_GAP));
      usefulEnd = snap(clamp(usefulEnd, SCALE_MIN + (MIN_GAP * 2), SCALE_MAX));

      if (lightStart - usefulStart < MIN_GAP) {
        if (source === "start") {
          usefulStart = lightStart - MIN_GAP;
        } else {
          lightStart = usefulStart + MIN_GAP;
        }
      }

      if (usefulEnd - lightStart < MIN_GAP) {
        if (source === "end") {
          usefulEnd = lightStart + MIN_GAP;
        } else {
          lightStart = usefulEnd - MIN_GAP;
        }
      }

      usefulStart = snap(clamp(usefulStart, SCALE_MIN, SCALE_MAX - (MIN_GAP * 2)));
      lightStart = snap(clamp(lightStart, usefulStart + MIN_GAP, SCALE_MAX - MIN_GAP));
      usefulEnd = snap(clamp(usefulEnd, lightStart + MIN_GAP, SCALE_MAX));

      const mediumMin = usefulStart;
      const mediumMax = lightStart - 1;
      const lightMin = lightStart;
      const lightMax = usefulEnd;
      const intenseLow = usefulStart;
      const intenseHigh = usefulEnd;

      usefulStartInput.value = formatValue(usefulStart);
      lightStartInput.value = formatValue(lightStart);
      usefulEndInput.value = formatValue(usefulEnd);

      mediumMinHidden.value = formatValue(mediumMin);
      mediumMaxHidden.value = formatValue(mediumMax);
      intenseHighHidden.value = formatValue(intenseHigh);

      intenseLowPreview.textContent = formatValue(intenseLow);
      intenseHighPreview.textContent = formatValue(intenseHigh);
      mediumMinPreview.textContent = formatValue(mediumMin);
      mediumMaxPreview.textContent = formatValue(mediumMax);
      lightMinPreview.textContent = formatValue(lightMin);
      lightMaxPreview.textContent = formatValue(lightMax);

      const startPct = valueToPercent(usefulStart);
      const middlePct = valueToPercent(lightStart);
      const endPct = valueToPercent(usefulEnd);

      segIntenseLow.style.left = "0%";
      segIntenseLow.style.width = `${startPct}%`;

      segMedium.style.left = `${startPct}%`;
      segMedium.style.width = `${Math.max(middlePct - startPct, 0)}%`;

      segLight.style.left = `${middlePct}%`;
      segLight.style.width = `${Math.max(endPct - middlePct, 0)}%`;

      segIntenseHigh.style.left = `${endPct}%`;
      segIntenseHigh.style.width = `${Math.max(100 - endPct, 0)}%`;

      handleStart.style.left = `${startPct}%`;
      handleMiddle.style.left = `${middlePct}%`;
      handleEnd.style.left = `${endPct}%`;
    }

    function syncFromInputs(source) {
      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);
      applyValues(usefulStart, lightStart, usefulEnd, source);
    }

    function onDrag(clientX) {
      if (!activeHandle) return;

      const draggedValue = clientXToValue(clientX);

      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);

      if (activeHandle === "start") {
        applyValues(draggedValue, lightStart, usefulEnd, "start");
      } else if (activeHandle === "middle") {
        applyValues(usefulStart, draggedValue, usefulEnd, "middle");
      } else {
        applyValues(usefulStart, lightStart, draggedValue, "end");
      }
    }

    function onPointerMove(event) {
      onDrag(event.clientX);
    }

    function onPointerUp() {
      activeHandle = null;
      handleStart.classList.remove("is-active");
      handleMiddle.classList.remove("is-active");
      handleEnd.classList.remove("is-active");

      document.removeEventListener("pointermove", onPointerMove);
      document.removeEventListener("pointerup", onPointerUp);
    }

    function startDrag(which, clientX) {
      activeHandle = which;
      handleStart.classList.toggle("is-active", which === "start");
      handleMiddle.classList.toggle("is-active", which === "middle");
      handleEnd.classList.toggle("is-active", which === "end");

      onDrag(clientX);

      document.addEventListener("pointermove", onPointerMove);
      document.addEventListener("pointerup", onPointerUp);
    }

    handleStart.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("start", event.clientX);
    });

    handleMiddle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("middle", event.clientX);
    });

    handleEnd.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("end", event.clientX);
    });

    slider.addEventListener("pointerdown", (event) => {
      if (
        event.target === handleStart ||
        event.target === handleMiddle ||
        event.target === handleEnd
      ) return;

      const clickedValue = clientXToValue(event.clientX);
      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);

      const distances = [
        { key: "start", distance: Math.abs(clickedValue - usefulStart) },
        { key: "middle", distance: Math.abs(clickedValue - lightStart) },
        { key: "end", distance: Math.abs(clickedValue - usefulEnd) }
      ].sort((a, b) => a.distance - b.distance);

      startDrag(distances[0].key, event.clientX);
    });

    usefulStartInput.addEventListener("input", () => syncFromInputs("start"));
    lightStartInput.addEventListener("input", () => syncFromInputs("middle"));
    usefulEndInput.addEventListener("input", () => syncFromInputs("end"));

    usefulStartInput.addEventListener("blur", () => syncFromInputs("start"));
    lightStartInput.addEventListener("blur", () => syncFromInputs("middle"));
    usefulEndInput.addEventListener("blur", () => syncFromInputs("end"));

    handleStart.addEventListener("keydown", (event) => {
      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        applyValues(usefulStart - STEP, lightStart, usefulEnd, "start");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        applyValues(usefulStart + STEP, lightStart, usefulEnd, "start");
      }
    });

    handleMiddle.addEventListener("keydown", (event) => {
      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        applyValues(usefulStart, lightStart - STEP, usefulEnd, "middle");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        applyValues(usefulStart, lightStart + STEP, usefulEnd, "middle");
      }
    });

    handleEnd.addEventListener("keydown", (event) => {
      const usefulStart = Number(usefulStartInput.value || 900);
      const lightStart = Number(lightStartInput.value || 1100);
      const usefulEnd = Number(usefulEndInput.value || 1900);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        applyValues(usefulStart, lightStart, usefulEnd - STEP, "end");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        applyValues(usefulStart, lightStart, usefulEnd + STEP, "end");
      }
    });

    applyValues(
      Number(usefulStartInput.value || 900),
      Number(lightStartInput.value || 1100),
      Number(usefulEndInput.value || 1900),
      "middle"
    );
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

  function initTreatmentConfigUI() {
    initSpeedThresholdUX();
    initTransitRpmUX();
    initBestRpmRange();
  }

  function initBestRpmRange() {
    const minInput = document.getElementById("bestRpmMin");
    const maxInput = document.getElementById("bestRpmMax");
    const minRange = document.getElementById("bestRpmMinRange");
    const maxRange = document.getElementById("bestRpmMaxRange");
    const fill = document.getElementById("bestRpmTrackFill");
    const sliderWrap = document.querySelector(".rpm-range-slider-wrap");

    if (!minInput || !maxInput || !minRange || !maxRange || !fill || !sliderWrap) return;

    const RANGE_MIN = Number(minRange.min || 0);
    const RANGE_MAX = Number(maxRange.max || 3000);
    const STEP = Number(minRange.step || 50);
    const MIN_GAP = STEP;

    let activeThumb = null;

    let minHandle = sliderWrap.querySelector(".rpm-range-handle--min");
    let maxHandle = sliderWrap.querySelector(".rpm-range-handle--max");

    if (!minHandle) {
      minHandle = document.createElement("button");
      minHandle.type = "button";
      minHandle.className = "rpm-range-handle rpm-range-handle--min";
      minHandle.setAttribute("aria-label", "Ajustar RPM mínimo");
      sliderWrap.appendChild(minHandle);
    }

    if (!maxHandle) {
      maxHandle = document.createElement("button");
      maxHandle.type = "button";
      maxHandle.className = "rpm-range-handle rpm-range-handle--max";
      maxHandle.setAttribute("aria-label", "Ajustar RPM máximo");
      sliderWrap.appendChild(maxHandle);
    }

    function clamp(value, min, max) {
      return Math.min(Math.max(value, min), max);
    }

    function snap(value) {
      return Math.round(value / STEP) * STEP;
    }

    function percentFromValue(value) {
      return ((value - RANGE_MIN) / (RANGE_MAX - RANGE_MIN)) * 100;
    }

    function valueFromClientX(clientX) {
      const rect = sliderWrap.getBoundingClientRect();
      const ratio = clamp((clientX - rect.left) / rect.width, 0, 1);
      return snap(RANGE_MIN + ratio * (RANGE_MAX - RANGE_MIN));
    }

    function updateVisuals(minVal, maxVal) {
      const minPct = percentFromValue(minVal);
      const maxPct = percentFromValue(maxVal);

      fill.style.left = `${minPct}%`;
      fill.style.width = `${Math.max(maxPct - minPct, 0)}%`;

      minHandle.style.left = `${minPct}%`;
      maxHandle.style.left = `${maxPct}%`;

      minHandle.classList.toggle("is-active", activeThumb === "min");
      maxHandle.classList.toggle("is-active", activeThumb === "max");
    }

    function setValues(minVal, maxVal, source) {
      minVal = snap(clamp(minVal, RANGE_MIN, RANGE_MAX - MIN_GAP));
      maxVal = snap(clamp(maxVal, RANGE_MIN + MIN_GAP, RANGE_MAX));

      if (maxVal - minVal < MIN_GAP) {
        if (source === "min") {
          minVal = maxVal - MIN_GAP;
        } else {
          maxVal = minVal + MIN_GAP;
        }
      }

      minVal = clamp(minVal, RANGE_MIN, RANGE_MAX - MIN_GAP);
      maxVal = clamp(maxVal, RANGE_MIN + MIN_GAP, RANGE_MAX);

      minInput.value = String(minVal);
      maxInput.value = String(maxVal);
      minRange.value = String(minVal);
      maxRange.value = String(maxVal);

      updateVisuals(minVal, maxVal);
    }

    function syncFromInputs(source) {
      const nextMin = Number(minInput.value || RANGE_MIN);
      const nextMax = Number(maxInput.value || RANGE_MAX);
      setValues(nextMin, nextMax, source);
    }

    function syncFromRanges(source) {
      const nextMin = Number(minRange.value || RANGE_MIN);
      const nextMax = Number(maxRange.value || RANGE_MAX);
      setValues(nextMin, nextMax, source);
    }

    function onDrag(clientX) {
      if (!activeThumb) return;

      const draggedValue = valueFromClientX(clientX);
      const currentMin = Number(minInput.value || RANGE_MIN);
      const currentMax = Number(maxInput.value || RANGE_MAX);

      if (activeThumb === "min") {
        setValues(draggedValue, currentMax, "min");
      } else {
        setValues(currentMin, draggedValue, "max");
      }
    }

    function onPointerMove(event) {
      onDrag(event.clientX);
    }

    function onPointerUp() {
      activeThumb = null;
      minHandle.classList.remove("is-active");
      maxHandle.classList.remove("is-active");
      document.removeEventListener("pointermove", onPointerMove);
      document.removeEventListener("pointerup", onPointerUp);
    }

    function startDrag(which, clientX) {
      activeThumb = which;
      updateVisuals(
        Number(minInput.value || RANGE_MIN),
        Number(maxInput.value || RANGE_MAX)
      );
      onDrag(clientX);
      document.addEventListener("pointermove", onPointerMove);
      document.addEventListener("pointerup", onPointerUp);
    }

    minHandle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("min", event.clientX);
    });

    maxHandle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      startDrag("max", event.clientX);
    });

    sliderWrap.addEventListener("pointerdown", (event) => {
      if (event.target === minHandle || event.target === maxHandle) return;

      const clickedValue = valueFromClientX(event.clientX);
      const currentMin = Number(minInput.value || RANGE_MIN);
      const currentMax = Number(maxInput.value || RANGE_MAX);
      const nextThumb =
        Math.abs(clickedValue - currentMin) <= Math.abs(clickedValue - currentMax)
          ? "min"
          : "max";

      startDrag(nextThumb, event.clientX);
    });

    minRange.addEventListener("input", () => syncFromRanges("min"));
    maxRange.addEventListener("input", () => syncFromRanges("max"));

    minInput.addEventListener("input", () => syncFromInputs("min"));
    maxInput.addEventListener("input", () => syncFromInputs("max"));
    minInput.addEventListener("blur", () => syncFromInputs("min"));
    maxInput.addEventListener("blur", () => syncFromInputs("max"));

    minHandle.addEventListener("keydown", (event) => {
      const currentMin = Number(minInput.value || RANGE_MIN);
      const currentMax = Number(maxInput.value || RANGE_MAX);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        setValues(currentMin - STEP, currentMax, "min");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        setValues(currentMin + STEP, currentMax, "min");
      }
    });

    maxHandle.addEventListener("keydown", (event) => {
      const currentMin = Number(minInput.value || RANGE_MIN);
      const currentMax = Number(maxInput.value || RANGE_MAX);

      if (event.key === "ArrowLeft") {
        event.preventDefault();
        setValues(currentMin, currentMax - STEP, "max");
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        setValues(currentMin, currentMax + STEP, "max");
      }
    });

    setValues(
      Number(minInput.value || minRange.value || RANGE_MIN),
      Number(maxInput.value || maxRange.value || RANGE_MAX),
      "max"
    );
  }

  async function loadTreatmentConfigs() {
    const root = document.getElementById("treatment-configs-root");
    if (!root) return;

    try {
      const response = await fetch("/components/treatment/configs", { cache: "no-store" });
      if (!response.ok) {
        throw new Error("Não foi possível carregar os parâmetros do treatment");
      }

      root.innerHTML = await response.text();
      initTreatmentConfigUI();
    } catch (error) {
      console.error("Erro ao carregar configs do treatment:", error);
    }
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

  async function init() {
    await loadTreatmentConfigs();
    bindEvents();
    resetState();
    syncTopScrollbar();
  }

  init();
})();