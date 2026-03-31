const CONFIG = {
  DEFAULT_CENTER: [-15.7801, -47.9292],
  DEFAULT_ZOOM: 4,
  MAP_ZOOM_ON_REFERENCE: 10,
  DATA_SOURCES: [
    "./data/municipios.csv",
  ],
  NOMINATIM_SEARCH_URL: "https://nominatim.openstreetmap.org/search",
  NOMINATIM_REVERSE_URL: "https://nominatim.openstreetmap.org/reverse",
  OSRM_ROUTE_URL: "https://router.project-osrm.org/route/v1/driving",
};

const state = {
  map: null,
  marker: null,
  destinationMarker: null,
  routeLayer: null,
  cityBase: [],
  distanceRows: [],
  visibleRows: [],
  reference: null,
  dataSourceUsed: null,
  activeRoute: null,
  routeRequestId: 0,
};

const els = {
  referenceStatusBadge: document.getElementById("referenceStatusBadge"),
  referenceStatusText: document.getElementById("referenceStatusText"),
  selectedPlaceName: document.getElementById("selectedPlaceName"),
  selectedPlaceSub: document.getElementById("selectedPlaceSub"),
  selectedCoords: document.getElementById("selectedCoords"),
  loadedCitiesCount: document.getElementById("loadedCitiesCount"),
  mapMeta: document.getElementById("mapMeta"),
  resultsMeta: document.getElementById("resultsMeta"),
  searchLocationInput: document.getElementById("searchLocationInput"),
  manualLatitudeInput: document.getElementById("manualLatitudeInput"),
  manualLongitudeInput: document.getElementById("manualLongitudeInput"),
  referenceLabelInput: document.getElementById("referenceLabelInput"),
  ufFilterSelect: document.getElementById("ufFilterSelect"),
  citySearchInput: document.getElementById("citySearchInput"),
  btnUseCurrentLocation: document.getElementById("btnUseCurrentLocation"),
  btnClearReference: document.getElementById("btnClearReference"),
  btnRecalculateDistances: document.getElementById("btnRecalculateDistances"),
  btnSearchLocation: document.getElementById("btnSearchLocation"),
  btnApplyManualCoords: document.getElementById("btnApplyManualCoords"),
  btnSortByDistance: document.getElementById("btnSortByDistance"),
  btnExportVisibleRows: document.getElementById("btnExportVisibleRows"),
  distanceTableBody: document.getElementById("distanceTableBody"),
  kpiReferenceName: document.getElementById("kpiReferenceName"),
  kpiNearestCity: document.getElementById("kpiNearestCity"),
  kpiNearestDistance: document.getElementById("kpiNearestDistance"),
  kpiVisibleCount: document.getElementById("kpiVisibleCount"),
};

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function parseDecimal(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const raw = String(value).trim();
  if (!raw) return null;

  const normalized = raw.replace(",", ".");
  const parsed = Number.parseFloat(normalized);

  return Number.isFinite(parsed) ? parsed : null;
}

function isValidCoordinate(lat, lng) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lng) &&
    lat >= -90 &&
    lat <= 90 &&
    lng >= -180 &&
    lng <= 180
  );
}

function formatCoord(value, digits = 6) {
  return Number(value).toLocaleString("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatDistanceKm(value) {
  if (!Number.isFinite(value)) return "-";
  return `${value.toLocaleString("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })} km`;
}

function formatRouteDistanceMeters(value) {
  if (!Number.isFinite(value)) return "-";
  return `${(value / 1000).toLocaleString("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })} km`;
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds)) return "-";

  const totalMinutes = Math.round(seconds / 60);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;

  if (hours <= 0) {
    return `${minutes} min`;
  }

  if (minutes === 0) {
    return `${hours}h`;
  }

  return `${hours}h ${minutes}min`;
}

function toSlugFileName(value) {
  return normalizeText(value)
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function getReferenceDisplayName() {
  if (!state.reference) return "-";
  const custom = String(state.reference.customLabel ?? "").trim();
  const auto = String(state.reference.autoLabel ?? "").trim();
  return custom || auto || "Ponto de referência";
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371.0088;
  const toRad = (deg) => (deg * Math.PI) / 180;

  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function setStatusBadge(type, text) {
  els.referenceStatusBadge.classList.remove("badge-ok", "badge-warn", "badge-bad");

  if (type === "ok") {
    els.referenceStatusBadge.classList.add("badge-ok");
  } else if (type === "bad") {
    els.referenceStatusBadge.classList.add("badge-bad");
  } else {
    els.referenceStatusBadge.classList.add("badge-warn");
  }

  els.referenceStatusText.textContent = text;
}

function setMapMeta(text) {
  els.mapMeta.textContent = text;
}

function setResultsMeta(text) {
  els.resultsMeta.textContent = text;
}

function getDestinationDisplayName(route) {
  if (!route) return "-";
  return [route.destCity, route.destUf].filter(Boolean).join(" / ") || "Destino";
}

function updateMapMetaFromRoute() {
  if (!state.activeRoute) {
    updateReferenceUi();
    return;
  }

  const destination = getDestinationDisplayName(state.activeRoute);
  const distance = formatRouteDistanceMeters(state.activeRoute.distanceMeters);
  const duration = formatDuration(state.activeRoute.durationSeconds);

  setMapMeta(`Rota para ${destination}: ${distance} • tempo estimado ${duration}.`);
}

function updateSummaryCards() {
  if (!state.reference) {
    els.selectedPlaceName.textContent = "-";
    els.selectedPlaceSub.textContent = "Nenhum ponto definido até o momento.";
    els.selectedCoords.textContent = "-";
  } else {
    els.selectedPlaceName.textContent = getReferenceDisplayName();
    els.selectedPlaceSub.textContent =
      state.reference.source === "search"
        ? "Origem definida por pesquisa."
        : state.reference.source === "geolocation"
        ? "Origem definida pela localização atual."
        : state.reference.source === "manual"
        ? "Origem definida por coordenadas manuais."
        : "Origem definida no mapa.";

    els.selectedCoords.textContent = `${formatCoord(state.reference.lat)} / ${formatCoord(state.reference.lng)}`;
  }

  els.loadedCitiesCount.textContent = String(state.cityBase.length);
}

function updateKpis(rows = []) {
  els.kpiReferenceName.textContent = getReferenceDisplayName();
  els.kpiVisibleCount.textContent = String(rows.length);

  if (!state.reference || rows.length === 0) {
    els.kpiNearestCity.textContent = "-";
    els.kpiNearestDistance.textContent = "-";
    return;
  }

  const nearest = rows[0];
  els.kpiNearestCity.textContent = `${nearest.city} / ${nearest.uf}`;
  els.kpiNearestDistance.textContent = formatDistanceKm(nearest.distanceKm);
}

function renderTableMessage(message) {
  els.distanceTableBody.innerHTML = `
    <tr>
      <td colspan="6" class="table-empty">${escapeHtml(message)}</td>
    </tr>
  `;
}

function renderTable(rows) {
  if (!state.reference) {
    renderTableMessage("Selecione um ponto de referência para calcular as distâncias.");
    return;
  }

  if (state.cityBase.length === 0) {
    renderTableMessage("A base de municípios não foi carregada.");
    return;
  }

  if (!rows.length) {
    renderTableMessage("Nenhum município encontrado com os filtros atuais.");
    return;
  }

  const html = rows
    .map((row) => {
      return `
        <tr>
          <td>${escapeHtml(row.id)}</td>
          <td>${escapeHtml(row.uf)}</td>
          <td>${escapeHtml(row.city)}</td>
          <td>${escapeHtml(row.munUf)}</td>
          <td>${escapeHtml(formatDistanceKm(row.distanceKm))}</td>
          <td>
            <button
              type="button"
              class="route-btn"
              data-lat="${row.lat}"
              data-lng="${row.lng}"
              data-city="${escapeHtml(row.city)}"
              data-uf="${escapeHtml(row.uf)}"
              title="Calcular rota no mapa"
            >
              Ver rota
            </button>
          </td>
        </tr>
      `;
    })
    .join("");

  els.distanceTableBody.innerHTML = html;
}

function normalizeCollection(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.cities)) return payload.cities;
  if (Array.isArray(payload?.municipios)) return payload.municipios;
  return [];
}

function detectSeparator(headerLine) {
  const semicolons = (headerLine.match(/;/g) || []).length;
  const commas = (headerLine.match(/,/g) || []).length;
  return semicolons >= commas ? ";" : ",";
}

function splitCsvLine(line, separator) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === separator && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  result.push(current);
  return result.map((item) => item.trim());
}

function parseCsv(text) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) return [];

  const separator = detectSeparator(lines[0]);
  const headers = splitCsvLine(lines[0], separator);

  return lines.slice(1).map((line) => {
    const values = splitCsvLine(line, separator);
    const row = {};

    headers.forEach((header, index) => {
      row[header] = values[index] ?? "";
    });

    return row;
  });
}

function pickFirst(obj, keys) {
  for (const key of keys) {
    if (obj && obj[key] !== undefined && obj[key] !== null && String(obj[key]).trim() !== "") {
      return obj[key];
    }
  }
  return null;
}

function normalizeCityRecord(raw, index) {
  const latFromCoordinates =
    Array.isArray(raw?.coordinates) && raw.coordinates.length >= 2
      ? parseDecimal(raw.coordinates[1])
      : null;

  const lngFromCoordinates =
    Array.isArray(raw?.coordinates) && raw.coordinates.length >= 2
      ? parseDecimal(raw.coordinates[0])
      : null;

  const lat =
    parseDecimal(
      pickFirst(raw, [
        "LATITUDE",
        "latitude",
        "lat",
        "Latitude",
        "LAT",
        "y",
      ])
    ) ?? latFromCoordinates;

  const lng =
    parseDecimal(
      pickFirst(raw, [
        "LONGITUDE",
        "longitude",
        "lng",
        "lon",
        "Longitude",
        "LON",
        "x",
      ])
    ) ?? lngFromCoordinates;

  const city = String(
    pickFirst(raw, [
      "MUNICIPIO",
      "municipio",
      "cidade",
      "Cidade",
      "city",
      "nome",
      "NOME",
      "municipality",
    ]) ?? ""
  ).trim();

  const uf = String(
    pickFirst(raw, [
      "UF",
      "uf",
      "estado",
      "Estado",
      "sigla_uf",
      "SIGLA_UF",
      "state",
    ]) ?? ""
  )
    .trim()
    .toUpperCase();

  if (!city || !uf || !isValidCoordinate(lat, lng)) {
    return null;
  }

  const id = String(
    pickFirst(raw, [
      "ID",
      "id",
      "codigo_ibge",
      "CODIGO_IBGE",
      "ibge",
      "code",
      "codigo",
    ]) ?? index + 1
  ).trim();

  const munUf =
    String(
      pickFirst(raw, ["MUN_UF", "mun_uf", "munuf", "MunUF", "municipio_uf"]) ?? ""
    ).trim() || `${city}/${uf}`;

  return {
    id,
    uf,
    city,
    munUf,
    lat,
    lng,
  };
}

async function fetchCityBase() {
  if (Array.isArray(window.CALC_DIST_CITIES) && window.CALC_DIST_CITIES.length) {
    return {
      rows: normalizeCollection(window.CALC_DIST_CITIES),
      source: "window.CALC_DIST_CITIES",
    };
  }

  for (const source of CONFIG.DATA_SOURCES) {
    try {
      const response = await fetch(source, { cache: "no-store" });
      if (!response.ok) continue;

      const contentType = response.headers.get("content-type") || "";
      let rawRows = [];

      if (contentType.includes("application/json") || source.endsWith(".json")) {
        const payload = await response.json();
        rawRows = normalizeCollection(payload);
      } else {
        const text = await response.text();
        rawRows = parseCsv(text);
      }

      if (Array.isArray(rawRows) && rawRows.length) {
        return { rows: rawRows, source };
      }
    } catch {
    }
  }

  return { rows: [], source: null };
}

function populateUfFilter(rows) {
  const currentValue = els.ufFilterSelect.value;
  const ufs = [...new Set(rows.map((row) => row.uf))].sort((a, b) => a.localeCompare(b));

  els.ufFilterSelect.innerHTML = `<option value="">Todas as UFs</option>`;

  for (const uf of ufs) {
    const option = document.createElement("option");
    option.value = uf;
    option.textContent = uf;
    els.ufFilterSelect.appendChild(option);
  }

  if (ufs.includes(currentValue)) {
    els.ufFilterSelect.value = currentValue;
  }
}

function updateOriginMarkerPopup() {
  if (!state.marker || !state.reference) return;

  state.marker.bindPopup(`
    <strong>${escapeHtml(getReferenceDisplayName())}</strong><br>
    Origem<br>
    ${escapeHtml(formatCoord(state.reference.lat))}, ${escapeHtml(formatCoord(state.reference.lng))}
  `);
}

function updateReferenceUi() {
  updateSummaryCards();

  if (!state.reference) {
    setStatusBadge("warn", "Aguardando seleção");
    setMapMeta("Clique em qualquer ponto do mapa para definir o ponto de referência.");
    return;
  }

  updateOriginMarkerPopup();
  setStatusBadge("ok", "Origem definida");

  if (state.activeRoute) {
    updateMapMetaFromRoute();
    return;
  }

  setMapMeta(
    `Ponto atual: ${getReferenceDisplayName()} (${formatCoord(state.reference.lat)}, ${formatCoord(
      state.reference.lng
    )}).`
  );
}

function ensureMarker(lat, lng) {
  if (!state.marker) {
    state.marker = L.marker([lat, lng], { draggable: true }).addTo(state.map);

    state.marker.on("dragend", async () => {
      const position = state.marker.getLatLng();
      setReference(position.lat, position.lng, {
        autoLabel: "Ponto selecionado no mapa",
        source: "map",
        centerMap: false,
        autoCalculate: true,
      });

      const label = await reverseGeocodeLabel(position.lat, position.lng);
      if (label) {
        applyAutoLabel(label);
      }
    });

    return;
  }

  state.marker.setLatLng([lat, lng]);
}

function clearRoute() {
  state.routeRequestId += 1;
  state.activeRoute = null;

  if (state.routeLayer) {
    state.map.removeLayer(state.routeLayer);
    state.routeLayer = null;
  }

  if (state.destinationMarker) {
    state.map.removeLayer(state.destinationMarker);
    state.destinationMarker = null;
  }
}

function applyAutoLabel(label) {
  if (!state.reference) return;

  state.reference.autoLabel = label;
  if (!String(state.reference.customLabel ?? "").trim()) {
    els.referenceLabelInput.value = "";
  }

  updateReferenceUi();
  updateKpis(state.visibleRows);
  updateResultsMetaFromState();
}

function setReference(lat, lng, options = {}) {
  if (!isValidCoordinate(lat, lng)) {
    setStatusBadge("bad", "Coordenadas inválidas");
    return;
  }

  clearRoute();

  const previousCustomLabel =
    state.reference?.customLabel ?? els.referenceLabelInput.value.trim();

  state.reference = {
    lat,
    lng,
    autoLabel: options.autoLabel || state.reference?.autoLabel || "Ponto de referência",
    customLabel: previousCustomLabel,
    source: options.source || state.reference?.source || "map",
  };

  ensureMarker(lat, lng);

  if (options.centerMap !== false) {
    state.map.setView([lat, lng], options.zoom || CONFIG.MAP_ZOOM_ON_REFERENCE);
  }

  els.manualLatitudeInput.value = String(lat).replace(".", ",");
  els.manualLongitudeInput.value = String(lng).replace(".", ",");

  updateReferenceUi();

  if (options.autoCalculate !== false) {
    recalculateDistances();
  }
}

function clearReference() {
  clearRoute();

  state.reference = null;
  state.distanceRows = [];
  state.visibleRows = [];

  if (state.marker) {
    state.map.removeLayer(state.marker);
    state.marker = null;
  }

  els.manualLatitudeInput.value = "";
  els.manualLongitudeInput.value = "";
  els.referenceLabelInput.value = "";
  els.citySearchInput.value = "";
  els.searchLocationInput.value = "";

  updateReferenceUi();
  updateKpis([]);
  renderTableMessage("Selecione um ponto de referência para calcular as distâncias.");
  setResultsMeta("Nenhum cálculo executado ainda.");
}

async function reverseGeocodeLabel(lat, lng) {
  try {
    const url = new URL(CONFIG.NOMINATIM_REVERSE_URL);
    url.searchParams.set("format", "jsonv2");
    url.searchParams.set("lat", String(lat));
    url.searchParams.set("lon", String(lng));
    url.searchParams.set("zoom", "10");
    url.searchParams.set("addressdetails", "1");
    url.searchParams.set("accept-language", "pt-BR");

    const response = await fetch(url.toString(), {
      headers: {
        "Accept-Language": "pt-BR",
      },
    });

    if (!response.ok) return null;

    const data = await response.json();
    const address = data?.address ?? {};
    const city =
      address.city ||
      address.town ||
      address.village ||
      address.municipality ||
      address.county ||
      "";
    const stateCode = address.state_code || "";
    const displayName = [city, stateCode].filter(Boolean).join(" - ");

    return displayName || data?.display_name || null;
  } catch {
    return null;
  }
}

async function searchLocation() {
  const query = els.searchLocationInput.value.trim();

  if (!query) {
    setStatusBadge("warn", "Informe um local para buscar");
    return;
  }

  setStatusBadge("warn", "Buscando local...");

  try {
    const url = new URL(CONFIG.NOMINATIM_SEARCH_URL);
    url.searchParams.set("q", query);
    url.searchParams.set("format", "jsonv2");
    url.searchParams.set("limit", "1");
    url.searchParams.set("addressdetails", "1");
    url.searchParams.set("countrycodes", "br");
    url.searchParams.set("accept-language", "pt-BR");

    const response = await fetch(url.toString(), {
      headers: {
        "Accept-Language": "pt-BR",
      },
    });

    if (!response.ok) {
      throw new Error("Falha na pesquisa");
    }

    const results = await response.json();
    const place = Array.isArray(results) ? results[0] : null;

    if (!place) {
      setStatusBadge("bad", "Local não encontrado");
      return;
    }

    const lat = parseDecimal(place.lat);
    const lng = parseDecimal(place.lon);

    if (!isValidCoordinate(lat, lng)) {
      setStatusBadge("bad", "Coordenadas inválidas");
      return;
    }

    setReference(lat, lng, {
      autoLabel: place.display_name || query,
      source: "search",
      centerMap: true,
      zoom: 10,
      autoCalculate: true,
    });
  } catch {
    setStatusBadge("bad", "Erro ao buscar local");
  }
}

function applyManualCoordinates() {
  const lat = parseDecimal(els.manualLatitudeInput.value);
  const lng = parseDecimal(els.manualLongitudeInput.value);

  if (!isValidCoordinate(lat, lng)) {
    setStatusBadge("bad", "Latitude/longitude inválidas");
    return;
  }

  setReference(lat, lng, {
    autoLabel: "Coordenadas manuais",
    source: "manual",
    centerMap: true,
    zoom: CONFIG.MAP_ZOOM_ON_REFERENCE,
    autoCalculate: true,
  });

  reverseGeocodeLabel(lat, lng).then((label) => {
    if (label) applyAutoLabel(label);
  });
}

function applyReferenceLabelInput() {
  if (!state.reference) return;

  state.reference.customLabel = els.referenceLabelInput.value.trim();
  updateReferenceUi();
  updateKpis(state.visibleRows);
  updateResultsMetaFromState();
}

function recalculateDistances() {
  if (!state.reference) {
    renderTableMessage("Selecione um ponto de referência para calcular as distâncias.");
    updateKpis([]);
    setResultsMeta("Nenhum cálculo executado ainda.");
    setStatusBadge("warn", "Aguardando seleção");
    return;
  }

  if (!state.cityBase.length) {
    renderTableMessage("A base de municípios não foi carregada.");
    updateKpis([]);
    setResultsMeta("Nenhuma base disponível para cálculo.");
    setStatusBadge("bad", "Base de cidades indisponível");
    return;
  }

  setStatusBadge("warn", "Calculando...");

  state.distanceRows = state.cityBase
    .map((row) => ({
      ...row,
      distanceKm: haversineKm(state.reference.lat, state.reference.lng, row.lat, row.lng),
    }))
    .sort((a, b) => a.distanceKm - b.distanceKm);

  applyFiltersAndRender();
  setStatusBadge("ok", "Cálculo atualizado");
}

function applyFiltersAndRender() {
  const ufFilter = els.ufFilterSelect.value.trim().toUpperCase();
  const citySearch = normalizeText(els.citySearchInput.value);

  let rows = [...state.distanceRows];

  if (ufFilter) {
    rows = rows.filter((row) => row.uf === ufFilter);
  }

  if (citySearch) {
    rows = rows.filter((row) => {
      const city = normalizeText(row.city);
      const munUf = normalizeText(row.munUf);
      const uf = normalizeText(row.uf);
      return city.includes(citySearch) || munUf.includes(citySearch) || uf.includes(citySearch);
    });
  }

  state.visibleRows = rows;
  renderTable(rows);
  updateKpis(rows);
  updateResultsMetaFromState();
}

function updateResultsMetaFromState() {
  if (!state.reference) {
    setResultsMeta("Nenhum cálculo executado ainda.");
    return;
  }

  const total = state.distanceRows.length;
  const visible = state.visibleRows.length;
  const source = state.dataSourceUsed ? ` Base: ${state.dataSourceUsed}.` : "";

  setResultsMeta(
    `${visible} de ${total} municípios exibidos, ordenados por distância geográfica em linha reta a partir de ${getReferenceDisplayName()}.${source}`
  );
}

function exportVisibleRows() {
  if (!state.reference || !state.visibleRows.length) {
    setStatusBadge("warn", "Nada para exportar");
    return;
  }

  const lines = [
    [
      "id",
      "uf",
      "cidade",
      "mun_uf",
      "distancia_km",
      "origem",
      "origem_lat",
      "origem_lng",
    ].join(";"),
  ];

  for (const row of state.visibleRows) {
    lines.push(
      [
        row.id,
        row.uf,
        row.city,
        row.munUf,
        row.distanceKm.toFixed(4).replace(".", ","),
        getReferenceDisplayName(),
        String(state.reference.lat).replace(".", ","),
        String(state.reference.lng).replace(".", ","),
      ]
        .map((value) => `"${String(value).replaceAll('"', '""')}"`)
        .join(";")
    );
  }

  const csvContent = "\uFEFF" + lines.join("\n");
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);

  const link = document.createElement("a");
  const referenceName = toSlugFileName(getReferenceDisplayName()) || "origem";
  link.href = url;
  link.download = `distancias_${referenceName}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);

  URL.revokeObjectURL(url);
}

function scrollToMapSmooth() {
  const mapElement = document.getElementById("referenceMap");
  if (!mapElement) return;

  mapElement.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

function buildDestinationPopupHtml(destinationName, distanceMeters, durationSeconds) {
  return `
    <strong>${escapeHtml(destinationName)}</strong><br>
    Distância da rota: ${escapeHtml(formatRouteDistanceMeters(distanceMeters))}<br>
    Tempo estimado: ${escapeHtml(formatDuration(durationSeconds))}
  `;
}

async function calculateRouteOnMap(destLat, destLng, destCity = "", destUf = "") {
  if (!state.reference) {
    setStatusBadge("warn", "Defina a origem antes de calcular a rota");
    return;
  }

  if (!isValidCoordinate(destLat, destLng)) {
    setStatusBadge("bad", "Destino inválido");
    return;
  }

  const originLat = state.reference.lat;
  const originLng = state.reference.lng;
  const destinationName = [destCity, destUf].filter(Boolean).join(" / ") || "Destino";
  const requestId = ++state.routeRequestId;

  clearRoute();
  state.routeRequestId = requestId;

  setStatusBadge("warn", "Calculando rota...");

  try {
    const url = new URL(
      `${CONFIG.OSRM_ROUTE_URL}/${originLng},${originLat};${destLng},${destLat}`
    );
    url.searchParams.set("overview", "full");
    url.searchParams.set("geometries", "geojson");
    url.searchParams.set("steps", "false");

    const response = await fetch(url.toString(), { cache: "no-store" });

    if (!response.ok) {
      throw new Error("Falha ao consultar rota");
    }

    const data = await response.json();

    if (requestId !== state.routeRequestId) {
      return;
    }

    const route = Array.isArray(data?.routes) ? data.routes[0] : null;
    const coordinates = Array.isArray(route?.geometry?.coordinates)
      ? route.geometry.coordinates
      : [];

    if (!route || !coordinates.length) {
      throw new Error("Rota não encontrada");
    }

    const latLngs = coordinates.map(([lng, lat]) => [lat, lng]);

    state.routeLayer = L.polyline(latLngs, {
      weight: 5,
      opacity: 0.9,
    }).addTo(state.map);

    state.destinationMarker = L.marker([destLat, destLng]).addTo(state.map);

    state.activeRoute = {
      destLat,
      destLng,
      destCity,
      destUf,
      distanceMeters: route.distance,
      durationSeconds: route.duration,
    };

    state.destinationMarker.bindPopup(
      buildDestinationPopupHtml(destinationName, route.distance, route.duration)
    );

    updateMapMetaFromRoute();

    const bounds = state.routeLayer.getBounds();
    state.map.fitBounds(bounds, { padding: [40, 40] });
    state.destinationMarker.openPopup();
    scrollToMapSmooth();

    setTimeout(() => {
      state.map.invalidateSize();
      if (state.routeLayer) {
        state.map.fitBounds(state.routeLayer.getBounds(), { padding: [40, 40] });
      }
    }, 450);

    setStatusBadge("ok", "Rota calculada");
  } catch {
    if (requestId !== state.routeRequestId) {
      return;
    }

    clearRoute();
    updateReferenceUi();
    setStatusBadge("bad", "Não foi possível calcular a rota");
  }
}

function initializeMap() {
  state.map = L.map("referenceMap", { zoomControl: true }).setView(
    CONFIG.DEFAULT_CENTER,
    CONFIG.DEFAULT_ZOOM
  );

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap",
  }).addTo(state.map);

  state.map.on("click", async (event) => {
    const { lat, lng } = event.latlng;

    setReference(lat, lng, {
      autoLabel: "Ponto selecionado no mapa",
      source: "map",
      centerMap: false,
      autoCalculate: true,
    });

    const label = await reverseGeocodeLabel(lat, lng);
    if (label) {
      applyAutoLabel(label);
    }
  });
}

async function useCurrentLocation() {
  if (!navigator.geolocation) {
    setStatusBadge("bad", "Geolocalização não suportada");
    return;
  }

  setStatusBadge("warn", "Obtendo localização...");

  navigator.geolocation.getCurrentPosition(
    async (position) => {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;

      setReference(lat, lng, {
        autoLabel: "Minha localização",
        source: "geolocation",
        centerMap: true,
        zoom: 12,
        autoCalculate: true,
      });

      const label = await reverseGeocodeLabel(lat, lng);
      if (label) {
        applyAutoLabel(label);
      }
    },
    () => {
      setStatusBadge("bad", "Não foi possível obter a localização");
    },
    {
      enableHighAccuracy: true,
      timeout: 10000,
      maximumAge: 0,
    }
  );
}

async function loadBase() {
  setStatusBadge("warn", "Carregando base...");
  const { rows, source } = await fetchCityBase();

  const normalized = rows
    .map((row, index) => normalizeCityRecord(row, index))
    .filter(Boolean);

  state.cityBase = normalized;
  state.dataSourceUsed = source;

  populateUfFilter(normalized);
  updateSummaryCards();

  if (!normalized.length) {
    setStatusBadge("bad", "Base não carregada");
    setMapMeta("Mapa pronto. A base de municípios ainda não está disponível.");
    setResultsMeta(
      "Nenhuma base foi encontrada. Ajuste o caminho do arquivo em CONFIG.DATA_SOURCES."
    );
    renderTableMessage("A base de municípios não foi carregada.");
    updateKpis([]);
    return;
  }

  if (!state.reference) {
    setStatusBadge("warn", "Aguardando seleção");
    setResultsMeta(
      `${normalized.length} municípios carregados. Defina um ponto de referência para iniciar o cálculo.`
    );
  } else {
    recalculateDistances();
  }
}

function bindEvents() {
  els.btnSearchLocation.addEventListener("click", searchLocation);
  els.btnApplyManualCoords.addEventListener("click", applyManualCoordinates);
  els.btnUseCurrentLocation.addEventListener("click", useCurrentLocation);
  els.btnClearReference.addEventListener("click", clearReference);
  els.btnRecalculateDistances.addEventListener("click", recalculateDistances);
  els.btnSortByDistance.addEventListener("click", () => {
    state.distanceRows.sort((a, b) => a.distanceKm - b.distanceKm);
    applyFiltersAndRender();
  });
  els.btnExportVisibleRows.addEventListener("click", exportVisibleRows);

  els.searchLocationInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      searchLocation();
    }
  });

  els.referenceLabelInput.addEventListener("input", applyReferenceLabelInput);
  els.ufFilterSelect.addEventListener("change", applyFiltersAndRender);
  els.citySearchInput.addEventListener("input", applyFiltersAndRender);

  els.distanceTableBody.addEventListener("click", (event) => {
    const button = event.target.closest(".route-btn");
    if (!button) return;

    const lat = parseDecimal(button.dataset.lat);
    const lng = parseDecimal(button.dataset.lng);
    const city = String(button.dataset.city ?? "").trim();
    const uf = String(button.dataset.uf ?? "").trim();

    if (!isValidCoordinate(lat, lng)) return;
    calculateRouteOnMap(lat, lng, city, uf);
  });
}

async function init() {
  initializeMap();
  bindEvents();
  updateReferenceUi();
  updateKpis([]);
  renderTableMessage("Selecione um ponto de referência para calcular as distâncias.");
  await loadBase();
}

init().catch((error) => {
  console.error("Erro ao inicializar calc_dist.js:", error);
  setStatusBadge("bad", "Erro na inicialização");
  renderTableMessage("Ocorreu um erro ao inicializar a página.");
});