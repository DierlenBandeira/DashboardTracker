function formatTimestampLabel(msgTm) {
  if (!msgTm) return "-";
  const d = new Date(msgTm * 1000);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${dd}/${mm} ${hh}:${mi}:${ss}`;
}

function formatTimestampFull(msgTm) {
  if (!msgTm) return "-";
  const d = new Date(msgTm * 1000);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${dd}/${mm}/${yyyy} ${hh}:${mi}:${ss}`;
}

function formatMsgTmToDateTime(msgTm) {
  return formatTimestampFull(msgTm);
}

function formatAge(sec) {
  if (sec == null || !Number.isFinite(sec)) return "-";
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  const rem = sec % 60;
  if (min < 60) return `${min}min ${rem}s`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return `${h}h ${m}min`;
}

function badgeHtml(text, cls) {
  return `<span class="badge ${cls}"><span class="badge-dot"></span>${text}</span>`;
}

function extractNumericValue(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  let s = String(value).trim();
  if (!s) return null;

  s = s.replace(/\u00A0/g, " ");
  s = s.replace(/\s+/g, " ").trim();

  const match = s.match(/-?\d+(?:[.,]\d+)?/);
  if (!match) return null;

  let num = match[0];

  if (num.includes(",") && num.includes(".")) {
    if (num.lastIndexOf(",") > num.lastIndexOf(".")) {
      num = num.replace(/\./g, "").replace(",", ".");
    } else {
      num = num.replace(/,/g, "");
    }
  } else if (num.includes(",")) {
    num = num.replace(",", ".");
  }

  const parsed = Number(num);
  return Number.isFinite(parsed) ? parsed : null;
}

function toNum(x) {
  return extractNumericValue(x);
}

function parseBrazilDateTime(text) {
  if (!text) return null;
  const s = String(text).trim();

  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [, dd, mm, yyyy, hh, mi, ss = "00"] = m;
    return new Date(
      Number(yyyy),
      Number(mm) - 1,
      Number(dd),
      Number(hh),
      Number(mi),
      Number(ss)
    );
  }

  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;

  return null;
}

function normalizeOnOff(value) {
  const s = String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
  if (!s) return null;

  if (s.includes("desligado")) return 0;
  if (s.includes("ligado")) return 1;

  return null;
}

function normalizeBrake(value) {
  const s = String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
  if (!s) return null;

  if (s.includes("desacionado")) return 0;
  if (s.includes("acionado")) return 1;

  return null;
}

function formatValueForTip(key, value) {
  if (value == null) return "-";

  if (key === "motor" || key === "ar_cond") {
    return Number(value) === 1 ? "Ligado" : "Desligado";
  }

  if (key === "freio") {
    return Number(value) === 1 ? "Acionado" : "Desacionado";
  }

  return Number(value).toFixed(2);
}

function clamp(v, a, b) {
  return Math.max(a, Math.min(b, v));
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}