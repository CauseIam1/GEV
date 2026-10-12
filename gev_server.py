# GEV — God's Eye View Service (GEV.md contract implementation)
# Cesium tactical globe command bridge for the Polaris ecosystem.
#   - Serves the operator viewer at / (templates/gev.html)
#   - POST /api/gev/command : Polaris Gateway tools (gev_tools.py) push ops
#   - broadcasts 'gev_update' to viewer screens (room gev_dashboard)
#   - viewer census replies come back on the 'gev_census' socket event
# Zero-key v1: CARTO Dark Matter / OSM / Esri World Imagery, flat terrain.
# GEV_ION_TOKEN (free) unlocks Cesium World Terrain + OSM Buildings;
# GOOGLE_MAPS_API_KEY is reserved for a future Photorealistic 3D upgrade.

import os
import time
import logging
from collections import deque
from threading import Lock

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

# Ops the connected viewer understands (driven by gev_tools.py schemas)
GEV_OPS = {"set_center", "toggle_layer", "query_object", "set_focus_operator"}

COMMAND_LOG = deque(maxlen=100)  # last 100 tool-driven commands (audit trail)
VIEWER_STATE = {"census": {}, "updated_epoch": 0.0}
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
    return jsonify({
        "status": "online",
        "service": "gev",
        "viewers_connected": _connected_viewers(),
        "commands_recorded": len(COMMAND_LOG),
        "ion_token_configured": bool(GEV_ION_TOKEN),
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

        payload = {"op": op, "args": args, "ts": time.time()}
        socketio.emit("gev_update", payload, room=GEV_ROOM)

        with _state_lock:
            COMMAND_LOG.append({"op": op, "args": args, "ts": payload["ts"]})

        viewers = _connected_viewers()
        log.info(f"🎯 Command '{op}' accepted → {viewers} viewer(s) listening")
        return jsonify({
            "success": True,
            "op": op,
            "accepted": True,
            "viewers_connected": viewers,
            "note": "Delivered to the gev_dashboard room; commands are not replayed to late joiners.",
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