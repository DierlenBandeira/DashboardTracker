import json
import mimetypes
import os
import time
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, unquote, parse_qs
from pathlib import Path

import requests

# -----------------------------
# Config
# -----------------------------
WIALON_HOST = os.getenv("WIALON_HOST", "https://hst-api.wialon.us").rstrip("/")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))

HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", os.getenv("HTTP_PORT", "8790")))
TIMEOUT_SECONDS = float(os.getenv("TIMEOUT_SECONDS", "12"))
STALE_THRESHOLD_SEC = float(os.getenv("STALE_THRESHOLD_SEC", "180"))
LIVE_BUFFER_SIZE = int(os.getenv("LIVE_BUFFER_SIZE", "720"))

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"

HTML_PATH = FRONTEND_DIR / "index.html"
CSS_PATH = FRONTEND_DIR / "dashboard.css"
LOGO_PATH = FRONTEND_DIR / "logo.png"

CORS_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:8790,http://127.0.0.1:8790,https://dashboard-tracker-weld.vercel.app",
    ).split(",")
    if origin.strip()
]

CORS_ALLOW_METHODS = "GET, POST, OPTIONS"
CORS_ALLOW_HEADERS = "Content-Type"

SENSOR_IDX = {
    "velocidade": 8,
    "rpm": 9,
    "consumido": 6,
    "temperatura_motor": 13,
    "pct_acelerado": 31,
    "ar_cond": 38,
    "altitude": 46,
}

AVL_LOCK = threading.Lock()
AVL_CURSOR_TM = int(time.time()) - 120

SID_LOCK = threading.Lock()
SID = os.getenv("WIALON_SID", "").strip()

UNIT_LOCK = threading.Lock()
ITEM_ID = int(os.getenv("WIALON_ITEM_ID", "401833488"))

STATE_LOCK = threading.Lock()
STATE = {
    "ok": False,
    "last_update_epoch": None,
    "error": None,
    "data": None,
    "raw": None,
    "ev_raw": None,
}

LIVE_BUFFER_LOCK = threading.Lock()
LIVE_BUFFER_BY_UNIT = {}

LAST_MSG_TM_LOCK = threading.Lock()
LAST_MSG_TM_BY_UNIT = {}

LAST_SIGNATURE_LOCK = threading.Lock()
LAST_SIGNATURE_BY_UNIT = {}

LAST_PLOT_KIND_LOCK = threading.Lock()
LAST_PLOT_KIND_BY_UNIT = {}

BOOTSTRAP_DONE_LOCK = threading.Lock()
BOOTSTRAP_DONE_BY_UNIT = {}


# -----------------------------
# Utils
# -----------------------------
def normalize_resp(resp):
    if isinstance(resp, list):
        if not resp:
            return {}
        return resp[0] if isinstance(resp[0], dict) else {}
    return resp if isinstance(resp, dict) else {}


def to_num(v):
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def first_not_none(*values):
    for v in values:
        if v is not None:
            return v
    return None


def normalize_bool_like(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    n = to_num(v)
    if n is not None:
        return 1 if n != 0 else 0
    s = str(v).strip().lower()
    if s in {"ligado", "on", "true", "sim"}:
        return 1
    if s in {"desligado", "off", "false", "nao", "não"}:
        return 0
    return None


def sensor_field(raw_value, display_value=None):
    return {"raw": raw_value, "display": display_value}


# -----------------------------
# State helpers
# -----------------------------
def get_sid():
    with SID_LOCK:
        return SID


def set_sid(new_sid: str):
    global SID
    with SID_LOCK:
        SID = (new_sid or "").strip()


def get_item_id():
    with UNIT_LOCK:
        return ITEM_ID


def set_item_id(new_id: int):
    global ITEM_ID
    with UNIT_LOCK:
        ITEM_ID = int(new_id)


def get_live_buffer(item_id: int):
    with LIVE_BUFFER_LOCK:
        if item_id not in LIVE_BUFFER_BY_UNIT:
            LIVE_BUFFER_BY_UNIT[item_id] = deque(maxlen=LIVE_BUFFER_SIZE)
        return LIVE_BUFFER_BY_UNIT[item_id]


def append_live_sample(item_id: int, sample: dict):
    buf = get_live_buffer(item_id)
    with LIVE_BUFFER_LOCK:
        buf.append(sample)


def list_live_samples(item_id: int, limit: int | None = None):
    buf = get_live_buffer(item_id)
    with LIVE_BUFFER_LOCK:
        data = list(buf)
    if limit is not None and limit > 0:
        return data[-limit:]
    return data


def clear_live_buffer(item_id: int):
    with LIVE_BUFFER_LOCK:
        LIVE_BUFFER_BY_UNIT.pop(item_id, None)


def get_last_msg_tm(item_id: int):
    with LAST_MSG_TM_LOCK:
        return LAST_MSG_TM_BY_UNIT.get(item_id)


def set_last_msg_tm(item_id: int, value):
    with LAST_MSG_TM_LOCK:
        if value is None:
            LAST_MSG_TM_BY_UNIT.pop(item_id, None)
        else:
            LAST_MSG_TM_BY_UNIT[item_id] = int(value)


def get_last_signature(item_id: int):
    with LAST_SIGNATURE_LOCK:
        return LAST_SIGNATURE_BY_UNIT.get(item_id)


def set_last_signature(item_id: int, value):
    with LAST_SIGNATURE_LOCK:
        if value is None:
            LAST_SIGNATURE_BY_UNIT.pop(item_id, None)
        else:
            LAST_SIGNATURE_BY_UNIT[item_id] = value


def get_last_plot_kind(item_id: int):
    with LAST_PLOT_KIND_LOCK:
        return LAST_PLOT_KIND_BY_UNIT.get(item_id)


def set_last_plot_kind(item_id: int, value):
    with LAST_PLOT_KIND_LOCK:
        if value is None:
            LAST_PLOT_KIND_BY_UNIT.pop(item_id, None)
        else:
            LAST_PLOT_KIND_BY_UNIT[item_id] = value


def get_bootstrap_done(item_id: int):
    with BOOTSTRAP_DONE_LOCK:
        return bool(BOOTSTRAP_DONE_BY_UNIT.get(item_id))


def set_bootstrap_done(item_id: int, done: bool):
    with BOOTSTRAP_DONE_LOCK:
        if done:
            BOOTSTRAP_DONE_BY_UNIT[item_id] = True
        else:
            BOOTSTRAP_DONE_BY_UNIT.pop(item_id, None)


def reset_runtime_state(item_id: int):
    global AVL_CURSOR_TM

    with AVL_LOCK:
        AVL_CURSOR_TM = int(time.time()) - 120

    clear_live_buffer(item_id)
    set_last_msg_tm(item_id, None)
    set_last_signature(item_id, None)
    set_last_plot_kind(item_id, None)
    set_bootstrap_done(item_id, False)

    with STATE_LOCK:
        STATE["ok"] = False
        STATE["error"] = None
        STATE["data"] = None
        STATE["raw"] = None
        STATE["ev_raw"] = None


# -----------------------------
# Static serving
# -----------------------------
def load_dashboard_html() -> str:
    if HTML_PATH.exists():
        return HTML_PATH.read_text(encoding="utf-8")
    return f"""
    <!doctype html>
    <html lang="pt-br">
    <head><meta charset="utf-8" /><title>Monitor Rastreasul</title></head>
    <body><h2>index.html não encontrado</h2><p>Arquivo esperado em: {HTML_PATH}</p></body>
    </html>
    """


def guess_content_type(file_path: Path) -> str:
    content_type, _ = mimetypes.guess_type(str(file_path))
    if content_type:
        if content_type.startswith("text/") or content_type in (
            "application/javascript",
            "application/json",
            "application/xml",
        ):
            return f"{content_type}; charset=utf-8"
        return content_type

    suffix = file_path.suffix.lower()
    if suffix == ".js":
        return "application/javascript; charset=utf-8"
    if suffix == ".css":
        return "text/css; charset=utf-8"
    if suffix == ".html":
        return "text/html; charset=utf-8"
    if suffix == ".json":
        return "application/json; charset=utf-8"
    if suffix == ".png":
        return "image/png"
    if suffix in (".jpg", ".jpeg"):
        return "image/jpeg"
    if suffix == ".svg":
        return "image/svg+xml"
    if suffix == ".ico":
        return "image/x-icon"
    return "application/octet-stream"


def resolve_static_path(request_path: str) -> Path | None:
    raw_path = unquote(urlparse(request_path).path)

    if raw_path in ("", "/"):
        return HTML_PATH

    frontend_root = FRONTEND_DIR.resolve()
    relative = raw_path.lstrip("/")

    if not relative:
        return HTML_PATH

    candidate = (FRONTEND_DIR / relative).resolve()

    try:
        candidate.relative_to(frontend_root)
    except ValueError:
        return None

    if candidate.is_file():
        return candidate

    if candidate.is_dir():
        index_candidate = (candidate / "index.html").resolve()
        try:
            index_candidate.relative_to(frontend_root)
        except ValueError:
            return None
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    html_candidate = (FRONTEND_DIR / f"{relative}.html").resolve()
    try:
        html_candidate.relative_to(frontend_root)
    except ValueError:
        return None

    if html_candidate.exists() and html_candidate.is_file():
        return html_candidate

    return None


def get_allowed_origin(request_handler) -> str:
    request_origin = request_handler.headers.get("Origin")
    if not request_origin:
        return "*"
    if request_origin in CORS_ALLOWED_ORIGINS:
        return request_origin
    return "null"


# -----------------------------
# Wialon calls
# -----------------------------
def wialon_ajax(svc: str, params_obj: dict):
    url = f"{WIALON_HOST}/wialon/ajax.html"
    payload = {
        "svc": svc,
        "sid": get_sid(),
        "params": json.dumps(params_obj, ensure_ascii=False),
    }
    r = requests.post(url, data=payload, timeout=TIMEOUT_SECONDS)
    r.raise_for_status()
    return r.json()


def wialon_avl_evts(tm_cursor: int):
    url = f"{WIALON_HOST}/avl_evts"
    payload = {
        "sid": get_sid(),
        "params": json.dumps({"tm": int(tm_cursor)}, ensure_ascii=False),
    }
    r = requests.post(url, data=payload, timeout=TIMEOUT_SECONDS)
    r.raise_for_status()
    return r.json()


def build_params_obj():
    return {"itemIds": [get_item_id()]}


# -----------------------------
# calc_last parsing
# -----------------------------
def pick_sensor(resp, idx: int):
    resp = normalize_resp(resp)
    sensors = resp.get("sensors")
    if not isinstance(sensors, dict):
        return None
    item = sensors.get(str(idx))
    if not isinstance(item, dict):
        return None
    fmt = item.get("format")
    return {
        "raw": item.get("value"),
        "display": fmt.get("value") if isinstance(fmt, dict) else None,
    }


def pick_pos_metric(resp, key: str):
    resp = normalize_resp(resp)
    pos = resp.get("pos")
    if not isinstance(pos, dict):
        return None
    item = pos.get(key)
    if not isinstance(item, dict):
        return None
    fmt = item.get("format")
    return {
        "raw": item.get("value"),
        "display": fmt.get("value") if isinstance(fmt, dict) else None,
    }


def extract_calc_last_tm(resp):
    r = normalize_resp(resp)

    pos = r.get("pos")
    if isinstance(pos, dict) and isinstance(pos.get("t"), (int, float)):
        return int(pos["t"])

    if isinstance(r.get("tm"), (int, float)):
        return int(r["tm"])

    return None


def build_status_from_calc_last(calc_last_resp, source="calc_last", chosen_reason="bootstrap"):
    r = normalize_resp(calc_last_resp)
    fields = {
        "velocidade": pick_pos_metric(r, "s"),
        "altitude": pick_pos_metric(r, "z"),
        "rpm": pick_sensor(r, SENSOR_IDX["rpm"]),
        "consumido": pick_sensor(r, SENSOR_IDX["consumido"]),
        "pct_acelerado": pick_sensor(r, SENSOR_IDX["pct_acelerado"]),
        "temperatura_motor": pick_sensor(r, SENSOR_IDX["temperatura_motor"]),
        "ar_cond": pick_sensor(r, SENSOR_IDX["ar_cond"]),
        "freio": pick_sensor(r, 40),
        "arla": pick_sensor(r, 41),
        "consumido_delta": pick_sensor(r, 42),
        "peso_total": pick_sensor(r, 43),
        "motor": pick_sensor(r, 39),
    }
    return {
        "tm": r.get("tm"),
        "pos_tm": extract_calc_last_tm(r),
        "msg_tm": extract_calc_last_tm(r),
        "item_id": get_item_id(),
        "fields": fields,
        "source": source,
        "chosen_reason": chosen_reason,
    }


def values_from_status(status: dict):
    f = status.get("fields") or {}
    return {
        "velocidade": to_num((f.get("velocidade") or {}).get("raw")),
        "altitude": to_num((f.get("altitude") or {}).get("raw")),
        "rpm": to_num((f.get("rpm") or {}).get("raw")),
        "consumido": to_num((f.get("consumido") or {}).get("raw")),
        "pct_acelerado": to_num((f.get("pct_acelerado") or {}).get("raw")),
        "motor": to_num((f.get("motor") or {}).get("raw")),
        "temperatura_motor": to_num((f.get("temperatura_motor") or {}).get("raw")),
        "ar_cond": to_num((f.get("ar_cond") or {}).get("raw")),
        "freio": to_num((f.get("freio") or {}).get("raw")),
        "arla": to_num((f.get("arla") or {}).get("raw")),
        "consumido_delta": to_num((f.get("consumido_delta") or {}).get("raw")),
        "peso_total": to_num((f.get("peso_total") or {}).get("raw")),
    }


# -----------------------------
# avl_evts parsing
# -----------------------------
def get_sensor_value_from_map(sensor_map: dict, idx: int):
    if not isinstance(sensor_map, dict):
        return None
    item = sensor_map.get(str(idx))
    if item is None:
        item = sensor_map.get(idx)
    if isinstance(item, dict):
        if "value" in item:
            return item.get("value")
        if "raw" in item:
            return item.get("raw")
        to = item.get("to")
        if isinstance(to, dict) and "v" in to:
            return to.get("v")
    return None


def get_sensor_display_from_map(sensor_map: dict, idx: int):
    if not isinstance(sensor_map, dict):
        return None
    item = sensor_map.get(str(idx))
    if item is None:
        item = sensor_map.get(idx)
    if isinstance(item, dict):
        fmt = item.get("format")
        if isinstance(fmt, dict):
            return fmt.get("value")
        return item.get("display")
    return None


def extract_units_update_for_item(ev_resp: dict, item_id: int):
    units_update = ev_resp.get("units_update") or ev_resp.get("unitsUpdate")
    if not isinstance(units_update, dict):
        return None
    block = units_update.get(str(item_id))
    if block is None:
        block = units_update.get(item_id)
    return block if isinstance(block, dict) else None


def extract_global_sensors_map(ev_resp: dict, item_id: int):
    sensors = ev_resp.get("sensors")
    if not isinstance(sensors, dict):
        return {}
    block = sensors.get(str(item_id))
    if block is None:
        block = sensors.get(item_id)
    return block if isinstance(block, dict) else {}


def extract_best_event_for_item(ev_resp: dict, item_id: int):
    events = ev_resp.get("events")
    if not isinstance(events, list):
        return None

    best = None
    for e in events:
        if not isinstance(e, dict):
            continue
        if e.get("i") != item_id:
            continue
        d = e.get("d")
        if not isinstance(d, dict):
            continue
        t = d.get("t")
        if not isinstance(t, (int, float)):
            continue
        if best is None or int(t) > int(best.get("d", {}).get("t", 0)):
            best = e
    return best


def build_status_from_avl(ev_resp: dict, item_id: int):
    unit_update = extract_units_update_for_item(ev_resp, item_id) or {}
    unit_update_sensors = unit_update.get("sensors") if isinstance(unit_update.get("sensors"), dict) else {}
    global_sensors = extract_global_sensors_map(ev_resp, item_id)
    event = extract_best_event_for_item(ev_resp, item_id)

    d = event.get("d") if isinstance(event, dict) else {}
    d = d if isinstance(d, dict) else {}
    pos = d.get("pos") if isinstance(d.get("pos"), dict) else {}
    p = d.get("p") if isinstance(d.get("p"), dict) else {}

    msg_tm = d.get("t")
    if not isinstance(msg_tm, (int, float)):
        msg_tm = None
    else:
        msg_tm = int(msg_tm)

    velocidade = first_not_none(
        to_num(pos.get("s")),
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["velocidade"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["velocidade"])),
    )

    altitude = first_not_none(
        to_num(pos.get("z")),
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["altitude"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["altitude"])),
    )

    rpm = first_not_none(
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["rpm"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["rpm"])),
    )

    consumido = first_not_none(
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["consumido"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["consumido"])),
    )

    pct_acelerado = first_not_none(
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["pct_acelerado"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["pct_acelerado"])),
        to_num(p.get("can_acc_pedal")),
    )

    temperatura_motor = first_not_none(
        to_num(get_sensor_value_from_map(global_sensors, SENSOR_IDX["temperatura_motor"])),
        to_num(get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["temperatura_motor"])),
        to_num(p.get("can_coolant_temp")),
    )

    ar_cond = normalize_bool_like(
        first_not_none(
            get_sensor_value_from_map(global_sensors, SENSOR_IDX["ar_cond"]),
            get_sensor_value_from_map(unit_update_sensors, SENSOR_IDX["ar_cond"]),
        )
    )

    freio = normalize_bool_like(p.get("can_breaks"))

    fields = {
        "velocidade": sensor_field(velocidade),
        "altitude": sensor_field(altitude),
        "rpm": sensor_field(
            rpm,
            first_not_none(
                get_sensor_display_from_map(global_sensors, SENSOR_IDX["rpm"]),
                get_sensor_display_from_map(unit_update_sensors, SENSOR_IDX["rpm"]),
            ),
        ),
        "consumido": sensor_field(
            consumido,
            first_not_none(
                get_sensor_display_from_map(global_sensors, SENSOR_IDX["consumido"]),
                get_sensor_display_from_map(unit_update_sensors, SENSOR_IDX["consumido"]),
            ),
        ),
        "pct_acelerado": sensor_field(
            pct_acelerado,
            first_not_none(
                get_sensor_display_from_map(global_sensors, SENSOR_IDX["pct_acelerado"]),
                get_sensor_display_from_map(unit_update_sensors, SENSOR_IDX["pct_acelerado"]),
            ),
        ),
        "temperatura_motor": sensor_field(
            temperatura_motor,
            first_not_none(
                get_sensor_display_from_map(global_sensors, SENSOR_IDX["temperatura_motor"]),
                get_sensor_display_from_map(unit_update_sensors, SENSOR_IDX["temperatura_motor"]),
            ),
        ),
        "ar_cond": sensor_field(
            ar_cond,
            first_not_none(
                get_sensor_display_from_map(global_sensors, SENSOR_IDX["ar_cond"]),
                get_sensor_display_from_map(unit_update_sensors, SENSOR_IDX["ar_cond"]),
            ),
        ),
        "freio": sensor_field(freio),
        "arla": sensor_field(to_num(p.get("can_adblue_level"))),
        "consumido_delta": sensor_field(None),
        "peso_total": sensor_field(
            first_not_none(
                to_num(p.get("can_gross_comb_weight")),
                to_num(p.get("can_weight")),
            )
        ),
        "motor": sensor_field(normalize_bool_like(p.get("can_engine_turned_on"))),
    }

    useful = any((f or {}).get("raw") is not None for f in fields.values()) or msg_tm is not None

    return {
        "tm": msg_tm,
        "pos_tm": msg_tm,
        "msg_tm": msg_tm,
        "item_id": item_id,
        "fields": fields,
        "source": "avl_evts",
        "chosen_reason": "avl_match" if useful else "avl_no_useful_unit_data",
        "useful": useful,
    }


# -----------------------------
# Sample / status builders
# -----------------------------
def build_values_signature(values: dict):
    return (
        values.get("velocidade"),
        values.get("altitude"),
        values.get("rpm"),
        values.get("consumido"),
        values.get("pct_acelerado"),
        values.get("motor"),
        values.get("temperatura_motor"),
        values.get("ar_cond"),
        values.get("freio"),
        values.get("arla"),
        values.get("consumido_delta"),
        values.get("peso_total"),
    )


def sample_from_status(status: dict, state_name: str, is_new: bool, is_repeated: bool, source: str, chosen_reason: str):
    values = values_from_status(status)
    msg_tm = status.get("msg_tm")
    sample_tm = msg_tm if isinstance(msg_tm, (int, float)) else int(time.time())

    return {
        "sample_tm": int(sample_tm),
        "msg_tm": int(msg_tm) if isinstance(msg_tm, (int, float)) else None,
        "item_id": status.get("item_id"),
        "state": state_name,
        "is_gap": False,
        "is_new_sample": is_new,
        "is_repeated": is_repeated,
        "values": values,
        "source": source,
        "chosen_reason": chosen_reason,
    }


def build_status_from_sample(sample: dict, now_epoch: float):
    msg_tm = sample.get("msg_tm")
    age_sec = None
    if isinstance(msg_tm, (int, float)):
        age_sec = now_epoch - float(msg_tm)

    stale = False if age_sec is None else age_sec > STALE_THRESHOLD_SEC
    values = sample.get("values") or {}
    state_name = sample.get("state") or "new"

    return {
        "tm": msg_tm,
        "pos_tm": msg_tm,
        "msg_tm": msg_tm,
        "item_id": sample.get("item_id"),
        "age_sec": age_sec,
        "stale": stale,
        "unit_has_event": state_name != "stale",
        "snapshot_refreshed": bool(sample.get("is_new_sample")),
        "backend_snapshot_reloaded": sample.get("source") == "calc_last",
        "event_msg_tm": msg_tm,
        "has_new_sample": bool(sample.get("is_new_sample")),
        "has_fresh_data": not stale,
        "sample_state": state_name,
        "source": sample.get("source"),
        "chosen_reason": sample.get("chosen_reason"),
        "fields": {
            "velocidade": sensor_field(values.get("velocidade")),
            "altitude": sensor_field(values.get("altitude")),
            "rpm": sensor_field(values.get("rpm")),
            "consumido": sensor_field(values.get("consumido")),
            "pct_acelerado": sensor_field(values.get("pct_acelerado")),
            "motor": sensor_field(values.get("motor")),
            "temperatura_motor": sensor_field(values.get("temperatura_motor")),
            "ar_cond": sensor_field(values.get("ar_cond")),
            "freio": sensor_field(values.get("freio")),
            "arla": sensor_field(values.get("arla")),
            "consumido_delta": sensor_field(values.get("consumido_delta")),
            "peso_total": sensor_field(values.get("peso_total")),
        },
    }


def choose_best_status(avl_status: dict | None, calc_status: dict | None, item_id: int):
    last_msg_tm = get_last_msg_tm(item_id)
    last_signature = get_last_signature(item_id)

    avl_values = values_from_status(avl_status) if avl_status else None
    calc_values = values_from_status(calc_status) if calc_status else None

    avl_sig = build_values_signature(avl_values) if avl_values else None
    calc_sig = build_values_signature(calc_values) if calc_values else None

    avl_tm = avl_status.get("msg_tm") if avl_status else None
    calc_tm = calc_status.get("msg_tm") if calc_status else None

    avl_new = False
    calc_new = False

    if avl_status and avl_status.get("useful"):
        if avl_tm is not None and avl_tm != last_msg_tm:
            avl_new = True
        elif avl_sig is not None and avl_sig != last_signature:
            avl_new = True

    if calc_status:
        if calc_tm is not None and calc_tm != last_msg_tm:
            calc_new = True
        elif calc_sig is not None and calc_sig != last_signature:
            calc_new = True

    if avl_new:
        return avl_status, "avl_evts", "avl_changed"
    if calc_new:
        return calc_status, "calc_last", "calc_last_changed"
    if avl_status and avl_status.get("useful"):
        return avl_status, "avl_evts", "avl_same_snapshot"
    if calc_status:
        return calc_status, "calc_last", "calc_last_same_snapshot"
    return None, None, None


def maybe_append_repeated_sample(item_id: int, now_epoch: int):
    samples = list_live_samples(item_id, limit=1)
    if not samples:
        return None

    last_sample = samples[-1]
    if last_sample.get("is_gap"):
        return None

    last_msg_tm = last_sample.get("msg_tm")
    age_sec = None
    if isinstance(last_msg_tm, (int, float)):
        age_sec = now_epoch - float(last_msg_tm)

    if age_sec is None or age_sec > STALE_THRESHOLD_SEC:
        return None

    repeated = {
        "sample_tm": now_epoch,
        "msg_tm": last_msg_tm,
        "item_id": item_id,
        "state": "repeated",
        "is_gap": False,
        "is_new_sample": False,
        "is_repeated": True,
        "values": dict(last_sample.get("values") or {}),
        "source": "repeated",
        "chosen_reason": "no_change_after_avl_and_calc_last",
    }
    append_live_sample(item_id, repeated)
    set_last_plot_kind(item_id, "sample")
    return repeated


def maybe_append_gap(item_id: int, now_epoch: int):
    samples = list_live_samples(item_id, limit=1)
    last_msg_tm = None
    if samples:
        last_msg_tm = samples[-1].get("msg_tm")

    age_sec = None
    if isinstance(last_msg_tm, (int, float)):
        age_sec = now_epoch - float(last_msg_tm)

    if age_sec is None or age_sec <= STALE_THRESHOLD_SEC:
        return None

    if get_last_plot_kind(item_id) == "gap":
        return None

    gap = {
        "sample_tm": now_epoch,
        "msg_tm": last_msg_tm,
        "item_id": item_id,
        "state": "stale",
        "is_gap": True,
        "is_new_sample": False,
        "is_repeated": False,
        "values": {},
        "source": "gap",
        "chosen_reason": "stale_threshold_exceeded",
    }
    append_live_sample(item_id, gap)
    set_last_plot_kind(item_id, "gap")
    return gap


def bootstrap_from_calc_last(item_id: int):
    resp = wialon_ajax("unit/calc_last", build_params_obj())

    with STATE_LOCK:
        STATE["raw"] = resp

    if isinstance(resp, dict) and "error" in resp:
        raise RuntimeError(f"Wialon calc_last error code: {resp.get('error')}")

    status = build_status_from_calc_last(resp, source="calc_last", chosen_reason="bootstrap")
    sample = sample_from_status(
        status=status,
        state_name="bootstrap",
        is_new=True,
        is_repeated=False,
        source="calc_last",
        chosen_reason="bootstrap",
    )

    append_live_sample(item_id, sample)
    set_last_plot_kind(item_id, "sample")
    if sample["msg_tm"] is not None:
        set_last_msg_tm(item_id, sample["msg_tm"])
    set_last_signature(item_id, build_values_signature(sample["values"]))
    set_bootstrap_done(item_id, True)

    return build_status_from_sample(sample, time.time())


# -----------------------------
# Worker
# -----------------------------
def poll_loop():
    global AVL_CURSOR_TM

    while True:
        try:
            sid_now = get_sid()
            if not sid_now:
                raise RuntimeError("SID vazio. Atualize pelo dashboard.")

            item_id = get_item_id()

            with AVL_LOCK:
                tm_cursor = AVL_CURSOR_TM

            ev = wialon_avl_evts(tm_cursor)

            if isinstance(ev, dict) and isinstance(ev.get("tm"), (int, float)):
                with AVL_LOCK:
                    AVL_CURSOR_TM = int(ev["tm"])

            with STATE_LOCK:
                STATE["ev_raw"] = ev

            if isinstance(ev, dict) and "error" in ev:
                raise RuntimeError(f"Wialon avl_evts error code: {ev.get('error')}")

            now_epoch = time.time()

            if not get_bootstrap_done(item_id):
                existing = list_live_samples(item_id, limit=1)
                if not existing:
                    status = bootstrap_from_calc_last(item_id)
                    with STATE_LOCK:
                        STATE["ok"] = True
                        STATE["last_update_epoch"] = now_epoch
                        STATE["error"] = None
                        STATE["data"] = status
                    time.sleep(POLL_SECONDS)
                    continue
                set_bootstrap_done(item_id, True)

            avl_status = build_status_from_avl(ev if isinstance(ev, dict) else {}, item_id)

            calc_resp = wialon_ajax("unit/calc_last", build_params_obj())
            with STATE_LOCK:
                STATE["raw"] = calc_resp

            if isinstance(calc_resp, dict) and "error" in calc_resp:
                raise RuntimeError(f"Wialon calc_last error code: {calc_resp.get('error')}")

            calc_status = build_status_from_calc_last(
                calc_resp,
                source="calc_last",
                chosen_reason="cycle_reconciliation",
            )

            chosen_status, source, chosen_reason = choose_best_status(
                avl_status=avl_status,
                calc_status=calc_status,
                item_id=item_id,
            )

            latest_status = None

            if chosen_status is not None:
                is_new = chosen_reason in {"avl_changed", "calc_last_changed"}
                if is_new:
                    sample = sample_from_status(
                        status=chosen_status,
                        state_name="new",
                        is_new=True,
                        is_repeated=False,
                        source=source,
                        chosen_reason=chosen_reason,
                    )
                    append_live_sample(item_id, sample)
                    set_last_plot_kind(item_id, "sample")
                    if sample["msg_tm"] is not None:
                        set_last_msg_tm(item_id, sample["msg_tm"])
                    set_last_signature(item_id, build_values_signature(sample["values"]))
                    latest_status = build_status_from_sample(sample, now_epoch)
                else:
                    repeated = maybe_append_repeated_sample(item_id, int(now_epoch))
                    if repeated is not None:
                        latest_status = build_status_from_sample(repeated, now_epoch)
                    else:
                        gap = maybe_append_gap(item_id, int(now_epoch))
                        if gap is not None:
                            latest_status = build_status_from_sample(gap, now_epoch)
                        else:
                            samples = list_live_samples(item_id, limit=1)
                            if samples:
                                latest_status = build_status_from_sample(samples[-1], now_epoch)
            else:
                repeated = maybe_append_repeated_sample(item_id, int(now_epoch))
                if repeated is not None:
                    latest_status = build_status_from_sample(repeated, now_epoch)
                else:
                    gap = maybe_append_gap(item_id, int(now_epoch))
                    if gap is not None:
                        latest_status = build_status_from_sample(gap, now_epoch)
                    else:
                        samples = list_live_samples(item_id, limit=1)
                        if samples:
                            latest_status = build_status_from_sample(samples[-1], now_epoch)

            if latest_status is None:
                latest_status = {
                    "tm": None,
                    "pos_tm": None,
                    "msg_tm": None,
                    "item_id": item_id,
                    "age_sec": None,
                    "stale": True,
                    "unit_has_event": False,
                    "snapshot_refreshed": False,
                    "backend_snapshot_reloaded": False,
                    "event_msg_tm": None,
                    "has_new_sample": False,
                    "has_fresh_data": False,
                    "sample_state": "stale",
                    "source": None,
                    "chosen_reason": "no_data_available",
                    "fields": {},
                }

            with STATE_LOCK:
                STATE["ok"] = True
                STATE["last_update_epoch"] = now_epoch
                STATE["error"] = None
                STATE["data"] = latest_status

        except Exception as e:
            with STATE_LOCK:
                STATE["ok"] = False
                STATE["last_update_epoch"] = time.time()
                STATE["error"] = str(e)

        time.sleep(POLL_SECONDS)


# -----------------------------
# HTTP Server
# -----------------------------
class Handler(BaseHTTPRequestHandler):
    def _set_common_headers(self, content_type: str, content_length: int | None = None):
        self.send_header("Content-Type", content_type)
        if content_length is not None:
            self.send_header("Content-Length", str(content_length))

        allowed_origin = get_allowed_origin(self)
        self.send_header("Access-Control-Allow-Origin", allowed_origin)
        self.send_header("Access-Control-Allow-Methods", CORS_ALLOW_METHODS)
        self.send_header("Access-Control-Allow-Headers", CORS_ALLOW_HEADERS)
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Vary", "Origin")

    def _send_bytes(self, content: bytes, content_type: str, code=200):
        self.send_response(code)
        self._set_common_headers(content_type, len(content))
        self.end_headers()
        self.wfile.write(content)

    def _send_text(self, text: str, content_type: str, code=200):
        self._send_bytes(text.encode("utf-8"), content_type, code)

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._set_common_headers("application/json; charset=utf-8", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, code=200):
        self._send_text(html, "text/html; charset=utf-8", code)

    def _serve_file(self, file_path: Path, content_type: str | None = None, not_found_msg: str = "arquivo nao encontrado"):
        try:
            if not file_path.exists() or not file_path.is_file():
                self._send_text(not_found_msg, "text/plain; charset=utf-8", 404)
                return
            self._send_bytes(file_path.read_bytes(), content_type or guess_content_type(file_path), 200)
        except Exception as e:
            self._send_text(f"Erro ao servir arquivo: {e}", "text/plain; charset=utf-8", 500)

    def _serve_static_from_frontend(self, request_path: str):
        file_path = resolve_static_path(request_path)
        if file_path is None:
            self._send_json({"error": "not_found"}, 404)
            return

        if not file_path.exists() or not file_path.is_file():
            self._send_json({"error": "not_found"}, 404)
            return

        self._serve_file(file_path, guess_content_type(file_path), "arquivo nao encontrado")

    def do_OPTIONS(self):
        allowed_origin = get_allowed_origin(self)
        if allowed_origin == "null":
            self.send_response(403)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", "0")
            self.send_header("Vary", "Origin")
            self.end_headers()
            return

        self.send_response(204)
        self._set_common_headers("text/plain; charset=utf-8", 0)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/health":
            self._send_json({"ok": True, "service": "dashboard_collector"}, 200)
            return

        if path == "/status":
            with STATE_LOCK:
                payload = {
                    "ok": STATE["ok"],
                    "last_update_epoch": STATE["last_update_epoch"],
                    "error": STATE["error"],
                    "data": STATE["data"],
                }
            self._send_json(payload, 200)
            return

        if path == "/live_series":
            try:
                item_id = get_item_id()
                limit_raw = (qs.get("limit") or ["180"])[0]
                try:
                    limit = max(1, min(int(limit_raw), LIVE_BUFFER_SIZE))
                except Exception:
                    limit = 180

                samples = list_live_samples(item_id, limit=limit)
                self._send_json(
                    {
                        "ok": True,
                        "item_id": item_id,
                        "count": len(samples),
                        "samples": samples,
                    },
                    200,
                )
            except Exception as e:
                self._send_json({"ok": False, "error": str(e), "samples": []}, 500)
            return

        if path == "/raw":
            with STATE_LOCK:
                payload = STATE.get("raw")
            self._send_json({"raw": payload}, 200)
            return

        if path == "/ev_raw":
            with STATE_LOCK:
                payload = STATE.get("ev_raw")
            self._send_json({"ev_raw": payload}, 200)
            return

        if path == "/units":
            try:
                sid_now = get_sid()
                if not sid_now:
                    self._send_json({"error": "SID vazio"}, 400)
                    return

                params = {
                    "spec": {
                        "itemsType": "avl_unit",
                        "propName": "sys_name",
                        "propValueMask": "*",
                        "sortType": "sys_name",
                    },
                    "force": 1,
                    "flags": 1,
                    "from": 0,
                    "to": 0,
                }

                resp = wialon_ajax("core/search_items", params)

                if isinstance(resp, dict) and "error" in resp:
                    code = resp.get("error")
                    if code == 1:
                        self._send_json({"error": "SID inválido/expirado. Atualize o SID."}, 401)
                        return
                    self._send_json({"error": f"Wialon error code: {code}"}, 400)
                    return

                items = (resp or {}).get("items", [])
                units = [
                    {"id": it.get("id"), "name": it.get("nm")}
                    for it in items
                    if it.get("id") and it.get("nm")
                ]
                self._send_json({"units": units}, 200)

            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return

        if path in ("/", "/dashboard.html"):
            self._send_html(load_dashboard_html(), 200)
            return

        self._serve_static_from_frontend(path)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length) if length > 0 else b"{}"
            body = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self._send_json({"ok": False, "error": "invalid json"}, 400)
            return

        if path == "/set_sid":
            new_sid = (body.get("sid") or "").strip()
            if not new_sid:
                self._send_json({"ok": False, "error": "missing sid"}, 400)
                return

            set_sid(new_sid)
            reset_runtime_state(get_item_id())

            self._send_json({"ok": True}, 200)
            return

        if path == "/set_unit":
            item_id = body.get("itemId")
            try:
                item_id = int(item_id)
            except Exception:
                self._send_json({"ok": False, "error": "invalid itemId"}, 400)
                return

            if item_id <= 0:
                self._send_json({"ok": False, "error": "invalid itemId"}, 400)
                return

            set_item_id(item_id)
            reset_runtime_state(item_id)

            self._send_json({"ok": True}, 200)
            return

        if path == "/login_fonte":
            self._send_json(
                {
                    "ok": False,
                    "error": "login_fonte ainda nao implementado. Preciso do request real de login da plataforma fonte para montar essa rota corretamente."
                },
                501,
            )
            return

        self._send_json({"error": "not_found"}, 404)

    def log_message(self, format, *args):
        return


def main():
    print(f"[PY] Executando: {Path(__file__).resolve()}", flush=True)
    print(f"[PY] Frontend dir: {FRONTEND_DIR}", flush=True)
    print(f"[PY] HTML esperado: {HTML_PATH}", flush=True)
    print(f"[PY] CSS esperado: {CSS_PATH}", flush=True)
    print(f"[PY] Logo esperada: {LOGO_PATH}", flush=True)
    print(f"[PY] Host: {HTTP_HOST}:{HTTP_PORT}", flush=True)
    print(f"[PY] CORS allowed origins: {CORS_ALLOWED_ORIGINS}", flush=True)

    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()

    server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    print(f"OK: servidor em http://localhost:{HTTP_PORT}/", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()