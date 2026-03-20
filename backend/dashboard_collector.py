# dashboard_collector.py
import json
import mimetypes
import os
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, unquote
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

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"

HTML_PATH = FRONTEND_DIR / "index.html"
CSS_PATH = FRONTEND_DIR / "dashboard.css"
LOGO_PATH = FRONTEND_DIR / "logo.png"

# CORS
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

AVL_LOCK = threading.Lock()
AVL_CURSOR_TM = int(time.time()) - 120

# Sensores numéricos
IDX = {
    "rpm": 9,
    "consumido": 6,
    "pct_acelerado": 31,
    # "arcond": 38,
}

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

MSG_LOCK = threading.Lock()
LAST_MSG_TM_BY_UNIT = {}
LAST_MSG_SEEN_EPOCH_BY_UNIT = {}


# -----------------------------
# Helpers
# -----------------------------
def normalize_resp(resp):
    if isinstance(resp, list):
        if not resp:
            return {}
        return resp[0] if isinstance(resp[0], dict) else {}
    return resp if isinstance(resp, dict) else {}


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


def build_params_obj():
    return {"itemIds": [get_item_id()]}


def load_dashboard_html() -> str:
    if HTML_PATH.exists():
        return HTML_PATH.read_text(encoding="utf-8")
    return f"""
    <!doctype html>
    <html lang="pt-br">
    <head>
      <meta charset="utf-8" />
      <title>Monitor Rastreasul</title>
    </head>
    <body>
      <h2>index.html não encontrado</h2>
      <p>Arquivo esperado em: {HTML_PATH}</p>
    </body>
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

    relative = raw_path.lstrip("/")
    candidate = (FRONTEND_DIR / relative).resolve()

    try:
        candidate.relative_to(FRONTEND_DIR.resolve())
    except ValueError:
        return None

    if candidate.is_dir():
        index_candidate = candidate / "index.html"
        return index_candidate if index_candidate.exists() else None

    return candidate


def get_allowed_origin(request_handler) -> str:
    request_origin = request_handler.headers.get("Origin")

    if not request_origin:
        return "*"

    if request_origin in CORS_ALLOWED_ORIGINS:
        return request_origin

    return "null"


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


def build_status(calc_last_resp):
    r = normalize_resp(calc_last_resp)
    out = {
        "tm": r.get("tm"),
        "pos_tm": extract_calc_last_tm(r),
        "item_id": get_item_id(),
        "fields": {
            "velocidade": pick_pos_metric(r, "s"),
            "altitude": pick_pos_metric(r, "z"),
        },
    }
    for name, idx in IDX.items():
        out["fields"][name] = pick_sensor(r, idx)
    return out


def find_best_tm(obj):
    best = None

    def consider(v):
        nonlocal best
        if isinstance(v, (int, float)):
            v = int(v)
            if best is None or v > best:
                best = v

    if isinstance(obj, dict):
        consider(obj.get("m"))
        to = obj.get("to")
        if isinstance(to, dict):
            consider(to.get("t"))

        for v in obj.values():
            child = find_best_tm(v)
            if child is not None:
                consider(child)

    elif isinstance(obj, list):
        for it in obj:
            child = find_best_tm(it)
            if child is not None:
                consider(child)

    return best


def extract_unit_msg_tm(ev_resp: dict, item_id: int):
    units_update = ev_resp.get("units_update") or ev_resp.get("unitsUpdate")
    if not isinstance(units_update, dict):
        return None

    block = units_update.get(str(item_id))
    if block is None:
        block = units_update.get(item_id)
    if block is None:
        return None

    return find_best_tm(block)


def extract_evt_d_t(ev_resp: dict, item_id: int):
    best = None
    events = ev_resp.get("events")
    if not isinstance(events, list):
        return None
    for e in events:
        if not isinstance(e, dict) or e.get("i") != item_id:
            continue
        d = e.get("d")
        if isinstance(d, dict) and isinstance(d.get("t"), (int, float)):
            t = int(d["t"])
            if best is None or t > best:
                best = t
    return best


def unit_has_event(ev_resp: dict, item_id: int) -> bool:
    events = ev_resp.get("events")
    if not isinstance(events, list):
        return False
    for e in events:
        if isinstance(e, dict) and e.get("i") == item_id:
            return True
    return False


# -----------------------------
# Worker (loop)
# -----------------------------
def poll_loop():
    global AVL_CURSOR_TM
    last_applied_msg_tm = None

    while True:
        try:
            sid_now = get_sid()
            if not sid_now:
                raise RuntimeError("SID vazio. Atualize pelo dashboard ou POST /set_sid.")

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

            msg_tm = None
            if isinstance(ev, dict):
                evt_tm = extract_evt_d_t(ev, item_id)
                unit_tm = extract_unit_msg_tm(ev, item_id)

                if evt_tm is not None and unit_tm is not None:
                    msg_tm = max(evt_tm, unit_tm)
                elif evt_tm is not None:
                    msg_tm = evt_tm
                else:
                    msg_tm = unit_tm

            now_epoch = time.time()

            with MSG_LOCK:
                if msg_tm is not None:
                    LAST_MSG_TM_BY_UNIT[item_id] = msg_tm
                    LAST_MSG_SEEN_EPOCH_BY_UNIT[item_id] = now_epoch

                cached_msg_tm = LAST_MSG_TM_BY_UNIT.get(item_id)

            has_evt = unit_has_event(ev if isinstance(ev, dict) else {}, item_id)

            status = None
            did_refresh_snapshot = False

            if cached_msg_tm is not None and cached_msg_tm != last_applied_msg_tm:
                params_obj = build_params_obj()
                resp = wialon_ajax("unit/calc_last", params_obj)

                with STATE_LOCK:
                    STATE["raw"] = resp

                if isinstance(resp, dict) and "error" in resp:
                    raise RuntimeError(f"Wialon calc_last error code: {resp.get('error')}")

                status = build_status(resp)
                last_applied_msg_tm = cached_msg_tm
                did_refresh_snapshot = True
            else:
                with STATE_LOCK:
                    status = STATE["data"] if isinstance(STATE.get("data"), dict) else None

            if status is None:
                params_obj = build_params_obj()
                resp = wialon_ajax("unit/calc_last", params_obj)

                with STATE_LOCK:
                    STATE["raw"] = resp

                if isinstance(resp, dict) and "error" in resp:
                    raise RuntimeError(f"Wialon calc_last error code: {resp.get('error')}")

                status = build_status(resp)
                did_refresh_snapshot = True

            calc_last_tm = status.get("pos_tm") or status.get("tm") or cached_msg_tm

            age_sec = None
            if calc_last_tm is not None:
                age_sec = now_epoch - float(calc_last_tm)

            stale_threshold_sec = 180.0
            stale = False if calc_last_tm is None else age_sec > stale_threshold_sec

            status["msg_tm"] = calc_last_tm
            status["age_sec"] = age_sec
            status["stale"] = stale
            status["unit_has_event"] = has_evt
            status["snapshot_refreshed"] = did_refresh_snapshot
            status["event_msg_tm"] = cached_msg_tm

            with STATE_LOCK:
                STATE["ok"] = True
                STATE["last_update_epoch"] = now_epoch
                STATE["error"] = None
                STATE["data"] = status

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
        path = urlparse(self.path).path

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

            global AVL_CURSOR_TM
            with AVL_LOCK:
                AVL_CURSOR_TM = int(time.time()) - 120

            with MSG_LOCK:
                LAST_MSG_TM_BY_UNIT.pop(item_id, None)
                LAST_MSG_SEEN_EPOCH_BY_UNIT.pop(item_id, None)

            self._send_json({"ok": True}, 200)
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