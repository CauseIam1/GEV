# GEV — God's Eye View Service (GEV.md contract implementation)
# Cesium tactical globe command bridge for the Polaris ecosystem.
#   - Serves the operator viewer at / (templates/gev.html)
#   - POST /api/gev/command : Polaris Gateway tools (gev_tools.py) push ops
#   - broadcasts 'gev_update' to viewer screens (room gev_dashboard)
#   - viewer census replies come back on the 'gev_census' socket event
# Zero-key v1: CARTO Dark Matter / OSM / Esri World Imagery, flat terrain.
# GEV_ION_TOKEN (free) unlocks Cesium World Terrain + OSM Buildings;
# GOOGLE_MAPS_API_KEY is reserved for a future Photorealistic 3D upgrade.

import json
import os
import re
import time
import logging
import http.client
import socket
import urllib.error
import urllib.request
from collections import deque
from threading import Lock
from typing import Dict, Optional

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from flask_socketio import SocketIO, join_room, leave_room, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("GEV_SECRET", "gev-gods-eye-view-2026")
CORS(app, resources={r"/api/*": {"origins": "*"}})

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

GEV_PORT = int(os.environ.get("GEV_PORT", "5001"))
GEV_ROOM = "gev_dashboard"

# Injected into the viewer template (all point at LAN services)
GEV_BURST_WS_URL = os.environ.get("GEV_BURST_WS_URL", "http://192.168.50.51:8083")
GEV_GATEWAY_URL = os.environ.get("GEV_GATEWAY_URL", "http://192.168.50.51:8082")
GEV_ION_TOKEN = os.environ.get("GEV_ION_TOKEN", "")

# Local radar feed (ADS-B): readsb/tar1090 aircraft.json via compose DNS.
# GEV_ADSB_URL verified 2026-10-16 from inside this container (live contacts OK).
GEV_ADSB_URL = os.environ.get("GEV_ADSB_URL", "http://adsb-readsb:8080/data/aircraft.json")
GEV_ADSB_TIMEOUT_S = float(os.environ.get("GEV_ADSB_TIMEOUT_S", "4.0"))   # upstream HTTP timeout
GEV_ADSB_CACHE_TTL_S = float(os.environ.get("GEV_ADSB_CACHE_TTL_S", "2.0"))  # shared-poll TTL
GEV_ADSB_SEEN_MAX_S = float(os.environ.get("GEV_ADSB_SEEN_MAX_S", "60.0"))   # position staleness prune
_adsb_lock = Lock()
_adsb_cache: Dict = {"ts_epoch": 0.0, "ok": False, "payload": None, "error": None}

# Ontario 511 road events (MTO — Castle Rock ENGAGE REST API). Free developer key
# required (README_KEYS.md); the platform's hard throttle is 10 upstream calls per
# 60 s, so the shared TTL cache defaults to 60 s: N viewers can never push us past
# the cap. Empty key ⇒ honest "unconfigured" boot (the ion-token path, Oct 2026).
# Ground truth Oct 16, 2026 (live key): only https://511on.ca/api/v2/get/event
# exists on the Ontario platform — /api/v2/get/incidents 404s in both param
# variants. Records ship flat Latitude/Longitude (no geom block).
GEV_511ON_URL = os.environ.get("GEV_511ON_URL", "https://511on.ca/api/v2/get/event")
GEV_511ON_API_KEY = os.environ.get("GEV_511ON_API_KEY", "")
GEV_511ON_TIMEOUT_S = float(os.environ.get("GEV_511ON_TIMEOUT_S", "8.0"))        # upstream HTTP timeout
GEV_511ON_CACHE_TTL_S = float(os.environ.get("GEV_511ON_CACHE_TTL_S", "60.0"))   # shared-poll TTL (throttle-safe)
_511_lock = Lock()
_511_cache: Dict = {"ts_epoch": 0.0, "ok": False, "payload": None, "error": None}

# SDR dispatch toggle (v1.5 — one dongle, one owner; GEV.md Section 10)
# The NESDR Nano 3 (RTL2838, serial 7402142814) is time-shared between two
# container lives: adsb-readsb (ADS-B radar) and trunk-recorder (dispatch).
# librtlsdr opens the tuner exclusively, so exactly one owner holds it — the
# switch below never overlaps an unconfirmed stop with a start. The pool of
# reachable containers is a HARD two-name allowlist enforced by construction:
# no other container in this stack (ollama included) can ever be paused,
# stopped, or started from this code path.
GEV_DOCKER_SOCKET = os.environ.get("GEV_DOCKER_SOCKET", "/var/run/docker.sock")
SDR_DONGLE_SERIAL = "7402142814"          # NESDR Nano 3 — the boot-stable key
SDR_CONTAINERS = {                        # container name → tuner role
    "adsb-readsb": "radar",               # mikenye/readsb — radar mode
    "trunk-recorder": "dispatch",         # robotastic/trunk-recorder — dispatch mode
}
SDR_DOCKER_CALL_TIMEOUT_S = float(os.environ.get("GEV_DOCKER_TIMEOUT_S", "12.0"))
SDR_STOP_TIMEOUT_S = float(os.environ.get("GEV_SDR_STOP_TIMEOUT_S", "20.0"))
SDR_SNAPSHOT_TTL_S = float(os.environ.get("GEV_SDR_SNAPSHOT_TTL_S", "3.0"))
_sdr_lock = Lock()
_sdr_cache: Dict = {"ts_epoch": 0.0, "snapshot": None, "error": None}

# Ops the connected viewer understands (driven by gev_tools.py schemas)
GEV_OPS = {
    "set_center", "toggle_layer", "query_object", "set_focus_operator",
    "add_marker", "remove_marker", "clear_markers", "sdr_mode",
}

COMMAND_LOG = deque(maxlen=100)  # last 100 tool-driven commands (audit trail)
VIEWER_STATE = {"census": {}, "updated_epoch": 0.0}
# Bounded tactical marker registry — Polaris-dropped pins replayed to every
# viewer joining gev_dashboard, so markers survive screen reloads.
MARKERS: Dict[str, dict] = {}
MARKER_LIMIT = 50
_state_lock = Lock()
_started_epoch = time.time()

log = logging.getLogger("gev")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [GEV] %(message)s")


def _connected_viewers():
    """Honest connected-viewer count from the socket server's room table."""
    try:
        participants = list(socketio.server.manager.get_participants("/", GEV_ROOM))
        return len(participants)
    except Exception:
        return -1  # unknown — never report a fabricated count


# ============== TACTICAL MARKER REGISTRY ==============
def _validate_op_args(op: str, args: dict):
    """Hard validation for marker ops; returns an error string or None on pass."""
    if op == "add_marker":
        try:
            lat = float(args.get("lat"))
            lon = float(args.get("lon"))
        except (TypeError, ValueError):
            return "add_marker requires numeric 'lat' and 'lon'"
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return f"lat/lon out of bounds ({lat}, {lon}) — lat -90..90, lon -180..180"
        if args.get("color") is not None and not re.fullmatch(r"[0-9a-fA-F]{6}", str(args["color"])):
            return f"color '{args['color']}' must be 6 hex digits (no '#')"
        if args.get("label") is not None and len(str(args["label"])) > 80:
            return "label too long (max 80 chars)"
        if args.get("id") is not None and len(str(args["id"])) > 64:
            return "id too long (max 64 chars)"
    elif op == "remove_marker":
        if not args.get("id"):
            return "remove_marker requires 'id'"
    return None


def _apply_marker_registry(op: str, args: dict, ts: float) -> None:
    """Bounded registry update after a broadcast (the late-join replay source)."""
    if op == "add_marker":
        mid = str(args.get("id") or f"mk_{int(ts)}")
        MARKERS[mid] = {
            "id": mid,
            "lat": float(args.get("lat")),
            "lon": float(args.get("lon")),
            "label": str(args.get("label") or mid)[:80],
            "color": str(args.get("color") or "FFB300")[:6],
            "ts_epoch": ts,
        }
        while len(MARKERS) > MARKER_LIMIT:  # hard cap: drop the oldest pins first
            oldest = min(MARKERS, key=lambda k: MARKERS[k]["ts_epoch"])
            del MARKERS[oldest]
    elif op == "remove_marker":
        MARKERS.pop(str(args.get("id")), None)
    elif op == "clear_markers":
        MARKERS.clear()


# ============== ROUTES ==============
@app.route("/")
def index():
    """Serve the operator's globe HUD (identity: rich — GEV.md)."""
    return render_template(
        "gev.html",
        GEV_BURST_WS_URL=GEV_BURST_WS_URL,
        GEV_GATEWAY_URL=GEV_GATEWAY_URL,
        GEV_ION_TOKEN=GEV_ION_TOKEN,
    )


@app.route("/api/gev/health", methods=["GET"])
def gev_health():
    """Lightweight liveness for smoke tests and the Holodeck link checker."""
    with _state_lock:
        last_command = dict(COMMAND_LOG[-1]) if COMMAND_LOG else None
        marker_count = len(MARKERS)
    with _adsb_lock:  # cache state only — health never polls the upstream feed
        adsb_ok = _adsb_cache["ok"]
        adsb_err = _adsb_cache["error"]
        adsb_age = (round(time.time() - _adsb_cache["ts_epoch"], 1)
                    if _adsb_cache["ts_epoch"] else None)
        adsb_positions = (_adsb_cache["payload"] or {}).get("with_positions", 0)
    with _511_lock:  # cache state only — health never polls the MTO API
        five11_ok = _511_cache["ok"]
        five11_err = _511_cache["error"]
        five11_age = (round(time.time() - _511_cache["ts_epoch"], 1)
                      if _511_cache["ts_epoch"] else None)
        five11_events = (_511_cache["payload"] or {}).get("events_stored", 0)
    with _sdr_lock:  # cache state only — health never talks to the docker engine
        sdr_snap = _sdr_cache["snapshot"]
        sdr_err = _sdr_cache["error"]
        sdr_age = (round(time.time() - _sdr_cache["ts_epoch"], 1)
                   if _sdr_cache["ts_epoch"] else None)
    if not GEV_511ON_API_KEY:
        five11_status = "unconfigured"
    elif five11_ok:
        five11_status = "online"
    else:
        five11_status = "never_polled" if five11_age is None else "degraded"
    return jsonify({
        "status": "online",
        "service": "gev",
        "viewers_connected": _connected_viewers(),
        "commands_recorded": len(COMMAND_LOG),
        "markers_stored": marker_count,
        "ion_token_configured": bool(GEV_ION_TOKEN),
        "adsb_feed": {
            "url": GEV_ADSB_URL,
            "status": "online" if adsb_ok else ("never_polled" if adsb_age is None else "degraded"),
            "aircraft_positions": adsb_positions,
            "last_poll_age_s": adsb_age,
            "last_error": adsb_err,
        },
        "ontario511": {
            "url": GEV_511ON_URL,
            "configured": bool(GEV_511ON_API_KEY),
            "status": five11_status,
            "events_stored": five11_events,
            "last_poll_age_s": five11_age,
            "last_error": five11_err,
            "attribution": "data: 511on.ca (Open Government License — Ontario)",
        },
        "sdr": {
            "mode": (sdr_snap or {}).get("mode", "unknown"),
            "both_running": (sdr_snap or {}).get("both_running"),
            "radar_container_state": (sdr_snap or {}).get("radar_container", {}).get("state"),
            "dispatch_container_state": (sdr_snap or {}).get("dispatch_container", {}).get("state"),
            "dongle_serial": SDR_DONGLE_SERIAL,
            "last_scan_age_s": sdr_age,
            "last_error": sdr_err,
        },
        "uptime_seconds": round(time.time() - _started_epoch, 1),
        "last_command": last_command,
    })

@app.route("/api/gev/command", methods=["POST"])
def gev_command():
    """
    Receive one viewer command from the Polaris Gateway (gev_tools.py).

    Body: {"op": "set_center", "args": {"lon": -117.16, "lat": 32.72}}
    Broadcasts {"op","args","ts"} as 'gev_update' to the gev_dashboard room.
    """
    try:
        data = request.get_json(force=True, silent=True)
        if not isinstance(data, dict):
            return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

        op = str(data.get("op", "")).strip()
        if op not in GEV_OPS:
            return jsonify({
                "success": False,
                "error": f"Unknown op '{op}'",
                "known_ops": sorted(GEV_OPS),
            }), 400

        args = data.get("args") if isinstance(data.get("args"), dict) else {}

        err = _validate_op_args(op, args)
        if err:
            return jsonify({"success": False, "error": err}), 400

        payload = {"op": op, "args": args, "ts": time.time()}
        socketio.emit("gev_update", payload, room=GEV_ROOM)

        with _state_lock:
            COMMAND_LOG.append({"op": op, "args": args, "ts": payload["ts"]})
            _apply_marker_registry(op, args, payload["ts"])

        viewers = _connected_viewers()
        log.info(f"🎯 Command '{op}' accepted → {viewers} viewer(s) listening")
        return jsonify({
            "success": True,
            "op": op,
            "accepted": True,
            "viewers_connected": viewers,
            "note": "Delivered to the gev_dashboard room; marker ops are stored in the bounded MARKERS registry and replayed to late joiners.",
        })
    except Exception as exc:  # never let a tool call kill the bridge
        log.error(f"❌ Command bridge error: {exc}")
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/gev/census", methods=["GET"])
def gev_census():
    """Last object census reported by the viewer (queried via query_object)."""
    with _state_lock:
        return jsonify({
            "success": True,
            "census": VIEWER_STATE["census"],
            "updated_epoch": VIEWER_STATE["updated_epoch"],
        })


@app.route("/api/gev/history", methods=["GET"])
def gev_history():
    """Recent command audit trail (HUD + debugging)."""
    with _state_lock:
        return jsonify({"success": True, "commands": list(COMMAND_LOG)})


# ============== ADS-B RADAR FEED (readsb/tar1090 → viewer radar layer) ==============
def _normalize_aircraft(raw: dict) -> Optional[dict]:
    """One readsb record → normalized contact; None when skipped (no fix / stale)."""
    lat, lon = raw.get("lat"), raw.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    seen_pos = raw.get("seen_pos")
    if isinstance(seen_pos, (int, float)) and seen_pos > GEV_ADSB_SEEN_MAX_S:
        return None  # position older than the radar layer's staleness contract

    def _num(value):
        return float(value) if isinstance(value, (int, float)) else None

    alt_raw = raw.get("alt_baro", raw.get("alt_geom"))
    on_ground = (alt_raw == "ground")
    alt_ft = None if on_ground or not isinstance(alt_raw, (int, float)) else float(alt_raw)
    return {
        "hex": str(raw.get("hex", "")).strip().lower()[:12] or None,
        "callsign": str(raw.get("r") or raw.get("flight") or "").strip()[:12] or None,
        "lat": float(lat),
        "lon": float(lon),
        "alt_ft": alt_ft,
        "alt_m": round(alt_ft * 0.3048, 1) if alt_ft is not None else None,
        "on_ground": on_ground,
        "gs_kt": _num(raw.get("gs")),
        "track_deg": _num(raw.get("track")),
        "baro_rate_fpm": _num(raw.get("baro_rate")),
        "squawk": str(raw.get("squawk") or "").strip()[:7] or None,
        "emergency": str(raw.get("emergency") or "").strip() or None,
        "seen_pos_s": _num(seen_pos),
        "rssi_db": _num(raw.get("rssi")),
    }


def _fetch_adsb() -> dict:
    """Fetch + normalize aircraft.json with a tiny shared TTL cache so N viewers
    produce at most one upstream hit per TTL window instead of one per poll."""
    now = time.time()
    with _adsb_lock:
        cached = _adsb_cache["payload"]
        if cached is not None and (now - _adsb_cache["ts_epoch"]) < GEV_ADSB_CACHE_TTL_S:
            return cached
    try:
        with urllib.request.urlopen(GEV_ADSB_URL, timeout=GEV_ADSB_TIMEOUT_S) as resp:
            body = resp.read().decode("utf-8")
        data = json.loads(body)
    except (urllib.error.URLError, ValueError, OSError) as exc:
        err = f"{type(exc).__name__}: {exc}"
        with _adsb_lock:
            _adsb_cache.update({"ts_epoch": now, "ok": False, "payload": None, "error": err})
        log.warning(f"🛩 ADS-B feed unreachable ({GEV_ADSB_URL}): {err}")
        # HTTP 200 with a degraded flag: the viewer keeps polling gracefully.
        return {
            "success": False,
            "status": "degraded",
            "radar_url": GEV_ADSB_URL,
            "aircraft": [],
            "with_positions": 0,
            "error": err,
            "ts_epoch": now,
        }
    records = data.get("aircraft") if isinstance(data, dict) else None
    aircraft = [
        normalized
        for normalized in (_normalize_aircraft(a) for a in (records or []) if isinstance(a, dict))
        if normalized
    ]
    payload = {
        "success": True,
        "status": "online",
        "radar_url": GEV_ADSB_URL,
        "aircraft": aircraft,
        "with_positions": len(aircraft),
        "source_aircraft": len(records or []),
        "source_messages": data.get("messages") if isinstance(data, dict) else None,
        "ts_epoch": now,
    }
    with _adsb_lock:
        _adsb_cache.update({"ts_epoch": now, "ok": True, "payload": payload, "error": None})
    return payload


@app.route("/api/gev/adsb/aircraft", methods=["GET"])
def gev_adsb_aircraft():
    """Normalized live ADS-B contacts — fronts readsb aircraft.json for the radar layer."""
    return jsonify(_fetch_adsb())


# ============== ONTARIO 511 ROAD EVENTS (MTO Castle Rock ENGAGE API) ==============
def _epoch_or_none(value) -> Optional[int]:
    """Coerce Castle Rock timestamps (epoch numbers or ISO 8601 strings) → int epoch."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        pass
    try:  # ISO 8601 fallback (e.g. "2026-10-16T14:05:00-04:00")
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def _normalize_road_event(raw: dict) -> Optional[dict]:
    """One Castle Rock ENGAGE event → normalized road event; None when skipped.

    Defensive by contract: unmapped/renamed fields pass through as None, and
    position-less records are dropped — a road event's position is never
    fabricated (same honesty rule as the radar layer's bear).

    Field mapping grounded in live Ontario truth (Oct 16, 2026, 602 records):
    flat Latitude/Longitude (no geom block), RoadwayName, DirectionOfTravel
    ("Northbound"…"Toronto Bound"), EventType ("roadwork"|"accidentsAndIncidents"),
    Severity always "Unknown", IsFullClosure bool, LanesAffected, LastUpdated
    epoch int. Legacy Castle Rock keys (geom/roads/type/severity/closed) kept as
    fallbacks so variant deployments still normalize.
    """
    lat, lon = None, None
    geom = raw.get("geom")
    if isinstance(geom, dict):
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            try:
                lon, lat = float(coords[0]), float(coords[1])  # GeoJSON: [lon, lat]
            except (TypeError, ValueError):
                lat, lon = None, None
    if lat is None and isinstance(raw.get("Latitude"), (int, float)):
        lat = float(raw["Latitude"])
    if lon is None and isinstance(raw.get("Longitude"), (int, float)):
        lon = float(raw["Longitude"])
    if lat is None or lon is None:
        return None
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None

    roads = raw.get("roads")
    road_name = direction = None
    if isinstance(roads, list) and roads and isinstance(roads[0], dict):
        first = roads[0]
        road_name = str(first.get("name") or "").strip()[:60] or None
        direction = str(first.get("direction") or "").strip()[:20] or None
    if not road_name:  # Ontario flat keys, e.g. "HWY 401"
        road_name = str(raw.get("RoadwayName") or "").strip()[:60] or None
    if not direction:  # e.g. "Eastbound", "Fort Erie Bound", "Toronto Bound"
        direction = str(raw.get("DirectionOfTravel") or "").strip()[:20] or None

    kind = str(raw.get("type") or raw.get("EventType") or "Event").strip()[:40] or "Event"
    severity_text = str(raw.get("severity") or raw.get("Severity") or "").strip()
    # Ontario ships Severity="Unknown" universally — surface None, not a fake tier.
    severity = None if severity_text.upper() in ("", "UNKNOWN") else severity_text.upper()[:12]
    headline = str(raw.get("headline") or raw.get("description") or raw.get("Description") or "").strip()[:160] or None
    closed = (bool(raw.get("closed")) or bool(raw.get("IsFullClosure"))
              or "closure" in kind.lower() or "closed" in (headline or "").lower())

    event_id = str(raw.get("id") or raw.get("ID") or "").strip()[:64]
    if not event_id:
        fallback_ts = (raw.get("creation_time") or raw.get("update_time")
                       or raw.get("LastUpdated") or raw.get("Reported"))
        event_id = f"511_{_epoch_or_none(fallback_ts) or int(time.time())}"
    return {
        "id": event_id,
        "kind": kind,
        "severity": severity,
        "road": road_name,
        "direction": direction,
        "lanes_blocked": str(raw.get("lanes_blocked") or raw.get("LanesAffected") or "").strip()[:40] or None,
        "closed": closed,
        "headline": headline,
        "lat": lat,
        "lon": lon,
        "updated_epoch": _epoch_or_none(raw.get("update_time") or raw.get("creation_time")
                                        or raw.get("LastUpdated") or raw.get("Reported")),
        "source": "511on.ca",
    }


def _fetch_ontario511() -> dict:
    """Fetch + normalize Ontario 511 road events with a shared 60 s TTL cache.

    The MTO platform throttles at 10 upstream calls / 60 s; one cache window ≈ one
    upstream call regardless of viewer count. Unconfigured ⇒ no upstream hit and an
    honest status; upstream failure ⇒ HTTP 200 "degraded" so the viewer keeps
    polling gracefully — the road layer can never boot-fail or error-spam.
    """
    now = time.time()
    if not GEV_511ON_API_KEY:
        return {
            "success": False,
            "status": "unconfigured",
            "events": [],
            "events_stored": 0,
            "error": "GEV_511ON_API_KEY not set — see README_KEYS.md (free 511on.ca developer key)",
            "ts_epoch": now,
        }
    with _511_lock:
        cached = _511_cache["payload"]
        if cached is not None and (now - _511_cache["ts_epoch"]) < GEV_511ON_CACHE_TTL_S:
            return cached
    url = (f"{GEV_511ON_URL}{'&' if '?' in GEV_511ON_URL else '?'}"
           f"key={GEV_511ON_API_KEY}&format=json")
    try:
        with urllib.request.urlopen(url, timeout=GEV_511ON_TIMEOUT_S) as resp:
            body = resp.read().decode("utf-8")
        data = json.loads(body)
    except (urllib.error.URLError, ValueError, OSError) as exc:
        err = f"{type(exc).__name__}: {exc}"
        if "400" in err:
            err += " — 511on.ca rejected the request (key missing/invalid, see README_KEYS.md)"
        with _511_lock:
            _511_cache.update({"ts_epoch": now, "ok": False, "payload": None, "error": err})
        log.warning(f"🛣 Ontario 511 feed unreachable ({GEV_511ON_URL}): {err}")
        return {
            "success": False,
            "status": "degraded",
            "events": [],
            "events_stored": 0,
            "error": err,
            "ts_epoch": now,
        }
    records = data if isinstance(data, list) else (data.get("events") or data.get("items") or [])
    events = [
        normalized
        for normalized in (_normalize_road_event(r) for r in (records or []) if isinstance(r, dict))
        if normalized
    ]
    payload = {
        "success": True,
        "status": "online",
        "events": events,
        "events_stored": len(events),
        "source_events": len(records or []),
        "attribution": "data: 511on.ca (Open Government License — Ontario)",
        "ts_epoch": now,
    }
    with _511_lock:
        _511_cache.update({"ts_epoch": now, "ok": True, "payload": payload, "error": None})
    return payload


@app.route("/api/gev/511/events", methods=["GET"])
def gev_511_events():
    """Normalized Ontario 511 road events — fronts the MTO ENGAGE API for the road layer."""
    return jsonify(_fetch_ontario511())


# ============== SDR DISPATCH TOGGLE (v1.5 — one dongle, one owner) ==============
class DockerUnixConnection(http.client.HTTPConnection):
    """HTTP-over-unix-socket for the Docker Engine API — stdlib only (no docker
    SDK), matching the zero-dependency bootstrap style of this service."""

    def __init__(self, socket_path: str, timeout: float = 10.0):
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._socket_path)
        self.sock = sock  # http.client drives the conversation from here


def _docker_request(method: str, path: str, body: Optional[dict] = None,
                    timeout: Optional[float] = None) -> dict:
    """One Docker Engine API call; never raises — every failure normalizes into
    {"ok": False, ...} exactly like the adsb/511 feed paths."""
    conn = DockerUnixConnection(GEV_DOCKER_SOCKET,
                                timeout=timeout or SDR_DOCKER_CALL_TIMEOUT_S)
    payload = None
    headers = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        conn.request(method, f"/v1.41{path}", body=payload, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw) if raw else {}
        return {"ok": 200 <= resp.status < 300, "status": resp.status,
                "data": data if isinstance(data, (dict, list)) else {}}
    except (OSError, ValueError, http.client.HTTPException) as exc:
        return {"ok": False, "status": 0, "data": {},
                "error": f"{type(exc).__name__}: {exc}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _sdr_snapshot(force: bool = False) -> dict:
    """Inspect the two SDR containers and derive the honest tuner-ownership
    state. mode: radar | dispatch | offline, plus the both_running anomaly
    flag. Cached for SDR_SNAPSHOT_TTL_S so N pollers cost one engine hit."""
    now = time.time()
    if not force:
        with _sdr_lock:
            if _sdr_cache["snapshot"] is not None and \
                    (now - _sdr_cache["ts_epoch"]) < SDR_SNAPSHOT_TTL_S:
                return _sdr_cache["snapshot"]
    result = _docker_request("GET", "/containers/json?all=1")
    snapshot = {
        "success": False, "mode": "unknown", "both_running": False,
        "radar_container": {"state": "unknown"},
        "dispatch_container": {"state": "unknown"},
        "docker_error": None, "dongle_serial": SDR_DONGLE_SERIAL, "ts_epoch": now,
    }
    if not result["ok"]:
        snapshot["docker_error"] = result.get("error") or \
            f"docker engine API status {result['status']}"
        with _sdr_lock:
            _sdr_cache.update({"ts_epoch": now, "snapshot": snapshot,
                               "error": snapshot["docker_error"]})
        return snapshot
    states = {}
    for entry in result["data"] or []:
        names = entry.get("Names") or []
        name = names[0].lstrip("/") if names else ""
        if name in SDR_CONTAINERS:
            states[name] = str(entry.get("State") or "").lower()
    radar_running = states.get("adsb-readsb") == "running"
    dispatch_running = states.get("trunk-recorder") == "running"
    snapshot["success"] = True
    snapshot["radar_container"] = {"state": states.get("adsb-readsb", "not_created")}
    snapshot["dispatch_container"] = {"state": states.get("trunk-recorder", "not_created")}
    snapshot["both_running"] = radar_running and dispatch_running
    if radar_running or dispatch_running:
        # Tuner-owner priority lands on radar if both are somehow running —
        # the honest both_running flag tells the operator to check the logs.
        snapshot["mode"] = "radar" if radar_running else "dispatch"
    else:
        snapshot["mode"] = "offline"
    with _sdr_lock:
        _sdr_cache.update({"ts_epoch": now, "snapshot": snapshot, "error": None})
    return snapshot


def _sdr_set_mode(target: str, note: Optional[str] = None) -> dict:
    """Stop the current tuner owner, CONFIRM it is down, then start the other.
    An unconfirmed stop always aborts the start — two processes grabbing one
    librtlsdr dongle is a USB ownership fight, not redundancy."""
    ts = time.time()
    stop_name = "adsb-readsb" if target == "dispatch" else "trunk-recorder"
    start_name = "trunk-recorder" if target == "dispatch" else "adsb-readsb"

    # 1) STOP the active owner. Engine call is synchronous (t=grace): returns
    #    only when the container is down or the deadline passes.
    stop_res = _docker_request(
        "POST", f"/containers/{stop_name}/stop?t={int(SDR_STOP_TIMEOUT_S)}",
        timeout=SDR_STOP_TIMEOUT_S + 10.0)
    # 304 = already stopped, 404 = never created — both mean "dock is free"
    if not (stop_res["ok"] or stop_res["status"] in (304, 404)):
        detail = stop_res.get("error") or \
            (stop_res["data"].get("message") if isinstance(stop_res["data"], dict) else None) \
            or f"status {stop_res['status']}"
        return {
            "success": False,
            "error": (f"could not stop {stop_name} — tuner hand-off aborted "
                      f"(never starts on top of an unconfirmed stop): {detail}"),
            "mode": "unknown", "ts_epoch": ts,
        }

    # 2) START the other owner only once the dock is confirmed free.
    #    304 = already running — accepted as done.
    start_res = _docker_request("POST", f"/containers/{start_name}/start")
    if not (start_res["ok"] or start_res["status"] == 304):
        detail = start_res.get("error") or \
            (start_res["data"].get("message") if isinstance(start_res["data"], dict) else None) \
            or f"status {start_res['status']}"
        return {
            "success": False,
            "error": (f"{stop_name} is down but {start_name} failed to start "
                      f"({detail}) — if the container was never created, run: "
                      f"docker compose up -d {start_name}"),
            "mode": "offline", "ts_epoch": ts,
        }

    snap = _sdr_snapshot(force=True)
    return {
        "success": True, "changed": True, "mode": target,
        "stopped": stop_name, "started": start_name,
        "both_running": snap.get("both_running"), "snapshot": snap,
        "note": (note + " — " if note else "")
                + f"tuner handed to {target}: stopped {stop_name}, started {start_name}",
        "ts_epoch": ts,
    }


@app.route("/api/gev/sdr/mode", methods=["GET", "POST"])
def gev_sdr_mode():
    """SDR tuner ownership — GET status, or POST {"mode": "radar"|"dispatch"}."""
    if request.method == "GET":
        return jsonify(_sdr_snapshot())
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False,
                        "error": "Invalid JSON payload — expected {\"mode\": \"radar\"|\"dispatch\"}"}), 400
    target = str(data.get("mode", "")).strip().lower()
    if target not in ("radar", "dispatch"):
        return jsonify({"success": False,
                        "error": f"Unknown mode '{target}' — known: radar, dispatch",
                        "known_modes": ["radar", "dispatch"]}), 400
    snap = _sdr_snapshot(force=True)
    if not snap.get("success"):
        return jsonify({"success": False,
                        "error": f"Docker engine unreachable: {snap.get('docker_error')}"}), 502
    if snap.get("mode") == target:
        return jsonify({"success": True, "changed": False, "mode": target,
                        "note": f"SDR already in {target} mode — no switch needed",
                        "snapshot": snap, "ts_epoch": time.time()})
    result = _sdr_set_mode(target, str(data.get("note") or "").strip()[:120] or None)
    if result.get("success"):
        payload = {"op": "sdr_mode", "args": {"mode": target}, "ts": time.time()}
        socketio.emit("gev_update", payload, room=GEV_ROOM)
        with _state_lock:
            COMMAND_LOG.append({"op": "sdr_mode", "args": {"mode": target}, "ts": payload["ts"]})
        log.info(f"📻 SDR mode → {target} — {result.get('note')}")
        return jsonify(result), 200
    log.warning(f"📻 SDR mode switch refused ({target}): {result.get('error')}")
    return jsonify(result), 502

# ============== SOCKET BRIDGE ==============
@socketio.on("connect")
def _on_connect():
    log.info(f"🔌 Socket client connected: {request.sid}")
    emit("gev_status", {"service": "gev", "ops": sorted(GEV_OPS), "ts_epoch": time.time()})


@socketio.on("join_gev")
def _on_join_gev(data):
    """Mirror of burst_receiver's join_radar — attach the screen to the room."""
    room = (data.get("room", GEV_ROOM) if isinstance(data, dict) else GEV_ROOM) or GEV_ROOM
    join_room(room)
    log.info(f"🌍 Client {request.sid} joined room: {room}")
    if room == GEV_ROOM:
        # Late-join replay: hand the whole bounded marker registry to the new screen
        with _state_lock:
            markers = list(MARKERS.values())
        if markers:
            emit("gev_markers", {"markers": markers, "ts_epoch": time.time()})
            log.info(f"📍 Replayed {len(markers)} marker(s) to {request.sid}")


@socketio.on("leave_gev")
def _on_leave_gev(data):
    room = (data.get("room", GEV_ROOM) if isinstance(data, dict) else GEV_ROOM) or GEV_ROOM
    leave_room(room)
    log.info(f"🌍 Client {request.sid} left room: {room}")


@socketio.on("gev_census")
def _on_gev_census(data):
    """Viewer replies to query_object — stored for gev_query_object polling."""
    try:
        if not isinstance(data, dict):
            return
        with _state_lock:
            VIEWER_STATE["census"] = data
            VIEWER_STATE["updated_epoch"] = time.time()
        log.info(f"🧭 Census update stored: {str(data)[:120]}")
    except Exception as exc:
        log.error(f"Census storage error: {exc}")


if __name__ == "__main__":
    log.info("🚀 GEV — God's Eye View coming online (zero-key v1)...")
    socketio.run(app, host="0.0.0.0", port=GEV_PORT, allow_unsafe_werkzeug=True)