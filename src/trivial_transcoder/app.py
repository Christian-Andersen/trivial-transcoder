from __future__ import annotations

import asyncio
import datetime
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import httpx
import pandas as pd
import psutil
import solara
import solara.lab

# --- Constants ---
DB_PATH = "trivial_transcoder.db"
DEFAULT_JELLYFIN_URL = "http://localhost:8096"
FFMPEG_PROGRESS_PATTERN = re.compile(r"(frame|fps|time|speed|bitrate)= *([^ ]+)")
HIGH_USAGE_THRESHOLD = 80
STDERR_LOG_LINES = 10


# --- Data Models ---
@dataclass
class TranscodeItem:
    name: str
    path: str
    status: str  # "queued", "processing", "completed", "failed"
    progress: float = 0.0
    speed: str = "N/A"
    time_remaining: str = "N/A"
    error: str = ""
    added_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TranscodeItem:
        return cls(**data)


# --- SQLite Persistence ---
class Database:
    def __init__(self, path: str = DB_PATH) -> None:
        self.path = str(Path(path).absolute())
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
            conn.commit()
        finally:
            conn.close()

    def set(self, key: str, value: object) -> None:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
            conn.commit()
        finally:
            conn.close()

    def get(self, key: str, default: object = None) -> object:
        conn = sqlite3.connect(self.path)
        try:
            res = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
            return json.loads(res[0]) if res else default
        except (sqlite3.Error, json.JSONDecodeError, TypeError):
            return default
        finally:
            conn.close()


db = Database()

# --- Global Reactive State ---
jellyfin_url = solara.reactive(value=cast("str", db.get("jellyfin_url", DEFAULT_JELLYFIN_URL)))
jellyfin_api_key = solara.reactive(value=cast("str", db.get("jellyfin_api_key", "")))
jellyfin_user_id = solara.reactive(value=cast("str", db.get("jellyfin_user_id", "")))

# Transcode Settings
preset = solara.reactive(value=cast("int", db.get("preset", 4)))
crf = solara.reactive(value=cast("int", db.get("crf", 25)))
grain = solara.reactive(value=cast("int", db.get("grain", 10)))

# Path Mapping (jellyfin_prefix:app_prefix)
path_map = solara.reactive(value=cast("str", db.get("path_map", "")))

# Notifications
ntfy_url = solara.reactive(value=cast("str", db.get("ntfy_url", "")))

# Theme (True=Dark, False=Light, None=Auto)
theme_dark = solara.reactive(value=cast("bool | None", db.get("theme_dark", True)))
solara.lab.theme.dark = theme_dark.value

# Queue & Worker State
_raw_queue = cast("list[dict[str, Any]]", db.get("transcode_queue", []))
transcode_queue = solara.reactive(value=[TranscodeItem.from_dict(i) for i in _raw_queue])
is_processing = solara.reactive(value=False)
# Keeping a list to prevent garbage collection of tasks
_background_tasks: set[asyncio.Task[Any]] = set()


# --- Persistence logic ---
def persist_state() -> None:
    db.set("jellyfin_url", jellyfin_url.value)
    db.set("jellyfin_api_key", jellyfin_api_key.value)
    db.set("jellyfin_user_id", jellyfin_user_id.value)
    db.set("preset", preset.value)
    db.set("crf", crf.value)
    db.set("grain", grain.value)
    db.set("path_map", path_map.value)
    db.set("ntfy_url", ntfy_url.value)
    db.set("theme_dark", theme_dark.value)
    db.set("transcode_queue", [asdict(item) for item in transcode_queue.value])


# Subscribe to changes
jellyfin_url.subscribe(lambda _: persist_state())
jellyfin_api_key.subscribe(lambda _: persist_state())
jellyfin_user_id.subscribe(lambda _: persist_state())
preset.subscribe(lambda _: persist_state())
crf.subscribe(lambda _: persist_state())
grain.subscribe(lambda _: persist_state())
path_map.subscribe(lambda _: persist_state())
ntfy_url.subscribe(lambda _: persist_state())
theme_dark.subscribe(lambda val: (setattr(solara.lab.theme, "dark", val), persist_state()))
transcode_queue.subscribe(lambda _: persist_state())


# --- Notification Helper ---
async def send_notification(message: str) -> None:
    if not ntfy_url.value:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(ntfy_url.value, content=message.encode("utf-8"), timeout=5.0)
    except httpx.HTTPError as e:
        print(f"Notification HTTP error: {e}")
    except RuntimeError as e:
        print(f"Notification runtime error: {e}")


# --- Path Mapping Helper ---
def apply_path_mapping(j_path: str) -> str:
    mapping = path_map.value.strip()
    if not mapping or ":" not in mapping:
        return j_path
    j_prefix, app_prefix = mapping.split(":", 1)
    if not j_path.startswith(j_prefix):
        msg = f"Path '{j_path}' does not start with Jellyfin prefix '{j_prefix}'"
        raise ValueError(msg)
    return j_path.replace(j_prefix, app_prefix, 1)


# --- Transcode Logic ---
async def parse_ffmpeg_progress(line: str, item_path: str) -> None:
    matches = FFMPEG_PROGRESS_PATTERN.findall(line)
    if not matches:
        return

    stats = dict(matches)
    queue = list(transcode_queue.value)
    idx = next((i for i, x in enumerate(queue) if x.path == item_path), None)
    if idx is not None:
        item = queue[idx]
        if "speed" in stats:
            item.speed = stats["speed"]
        transcode_queue.value = queue


def get_ffmpeg_command(input_path: Path, temp_path: Path) -> list[str]:
    return [
        "ffmpeg",
        "-i",
        str(input_path),
        "-map",
        "0",
        "-vf",
        "scale=1920:-1:flags=lanczos",
        "-c:v",
        "libsvtav1",
        "-preset",
        str(preset.value),
        "-crf",
        str(crf.value),
        "-pix_fmt",
        "yuv420p10le",
        "-svtav1-params",
        f"tune=0:enable-hdr=1:scd=1:film-grain={grain.value}:enable-restoration=1:enable-cdef=1",
        "-c:a",
        "libopus",
        "-b:a",
        "128k",
        "-ac",
        "2",
        "-c:s",
        "copy",
        "-y",
        str(temp_path),
    ]


async def read_stderr(stream: asyncio.StreamReader, path_str: str, log_buffer: list[str]) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        decoded = line.decode(errors="replace").strip()
        if decoded:
            log_buffer.append(decoded)
            if len(log_buffer) > STDERR_LOG_LINES:
                log_buffer.pop(0)
            await parse_ffmpeg_progress(decoded, path_str)


def finalize_files(temp_path: Path, input_path: Path, *, success: bool) -> bool:
    """Perform file replacement or cleanup. Returns True if temp file existed."""
    if not temp_path.exists():
        return False
    if success:
        temp_path.replace(input_path)
    else:
        temp_path.unlink()
    return True


async def finalize_item(item: TranscodeItem, temp_path: Path, returncode: int | None, error_msg: str) -> None:
    queue = list(transcode_queue.value)
    idx = next((i for i, x in enumerate(queue) if x.path == item.path), None)
    if idx is None:
        return

    success = returncode == 0
    if success:
        if finalize_files(temp_path, Path(item.path), success=True):
            queue[idx].status = "completed"
            await send_notification(f"✅ Finished: {item.name}")
        else:
            queue[idx].status = "failed"
            queue[idx].error = "Temp file missing"
    else:
        queue[idx].status = "failed"
        queue[idx].error = error_msg
        finalize_files(temp_path, Path(item.path), success=False)
    transcode_queue.value = queue


async def process_item(item: TranscodeItem) -> None:
    input_path = Path(item.path)
    temp_path = input_path.with_suffix("").with_name(f"{input_path.stem}_temp_av1.mkv")
    cmd = get_ffmpeg_command(input_path, temp_path)
    stderr_log: list[str] = []

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        if process.stderr:
            await asyncio.gather(process.wait(), read_stderr(process.stderr, str(input_path), stderr_log))
        else:
            await process.wait()

        error_msg = f"Exit code {process.returncode}"
        if process.returncode != 0 and stderr_log:
            error_msg = "\n".join(stderr_log)

        await finalize_item(item, temp_path, process.returncode, error_msg)

    except (OSError, ValueError, RuntimeError) as e:
        queue = list(transcode_queue.value)
        idx = next((i for i, x in enumerate(queue) if x.path == str(input_path)), None)
        if idx is not None:
            queue[idx].status = "failed"
            queue[idx].error = str(e)
            transcode_queue.value = queue
        finalize_files(temp_path, input_path, success=False)


async def run_worker() -> None:
    if is_processing.value:
        return

    is_processing.value = True
    try:
        while True:
            queue = list(transcode_queue.value)
            next_idx = next((i for i, it in enumerate(queue) if it.status == "queued"), None)

            if next_idx is None:
                break

            item = queue[next_idx]
            item.status = "processing"
            transcode_queue.value = list(queue)
            await process_item(item)
    finally:
        is_processing.value = False


# --- UI Components ---


@solara.component
def health_monitor() -> None:
    cpu_percent = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()

    with solara.Card("System Health", margin=2):
        with solara.Row(justify="space-around"):
            with solara.Column(align="center"):
                solara.Markdown("**CPU Usage**")
                cpu_color = "blue" if cpu_percent < HIGH_USAGE_THRESHOLD else "red"
                solara.ProgressLinear(value=cpu_percent, color=cpu_color)
                solara.Markdown(f"{cpu_percent}%")

            with solara.Column(align="center"):
                solara.Markdown("**Memory**")
                mem_color = "green" if mem.percent < HIGH_USAGE_THRESHOLD else "orange"
                solara.ProgressLinear(value=mem.percent, color=mem_color)
                solara.Markdown(f"{mem.percent}%")

        if hasattr(psutil, "sensors_temperatures"):
            try:
                temps = cast("dict[str, list[Any]]", psutil.sensors_temperatures())
                if temps:
                    solara.Markdown("---")
                    for name, entries in temps.items():
                        if entries:
                            solara.Markdown(f"**{name.capitalize()}:** {entries[0].current}°C")
            except AttributeError, OSError, RuntimeWarning:
                pass


@solara.component
def settings_tab() -> None:
    with solara.Card("Jellyfin Configuration", margin=2):
        solara.InputText("Jellyfin URL", value=jellyfin_url, continuous_update=True)
        solara.InputText("API Key", value=jellyfin_api_key, continuous_update=True, password=True)
        solara.InputText("User ID (UUID)", value=jellyfin_user_id, continuous_update=True)

    with solara.Card("Path Mapping", margin=2):
        solara.Markdown("Map Jellyfin internal paths to local paths (e.g. `/media:/mnt/media`)")
        solara.InputText("Prefix Mapping (jellyfin:local)", value=path_map, continuous_update=True)

    with solara.Card("Transcode Settings (AV1)", margin=2):
        solara.SliderInt("Preset (Lower = Slower/Better)", value=preset, min=0, max=13)
        solara.SliderInt("CRF (Quality)", value=crf, min=0, max=63)
        solara.SliderInt("Grain Synthesis", value=grain, min=0, max=50)

    with solara.Card("Notifications", margin=2):
        solara.InputText("ntfy.sh Topic URL", value=ntfy_url, continuous_update=True)
        if ntfy_url.value:

            def on_test() -> None:
                task = asyncio.create_task(send_notification("🔔 Trivial Transcoder Test"))
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)

            solara.Button("Send Test Notification", on_click=on_test)

    with solara.Card("Theme", margin=2):
        def set_theme(label: str) -> None:
            mapping: dict[str, bool | None] = {"Dark": True, "Light": False, "Auto (System)": None}
            theme_dark.value = mapping[label]

        current_label = {True: "Dark", False: "Light", None: "Auto (System)"}.get(theme_dark.value, "Auto (System)")

        solara.Select(
            label="Theme Mode",
            value=current_label,
            values=["Dark", "Light", "Auto (System)"],
            on_value=set_theme,
        )


def parse_jellyfin_item(item: dict[str, Any]) -> dict[str, Any]:
    bitrate = None
    if item.get("MediaSources"):
        bitrate = item["MediaSources"][0].get("Bitrate")
        if not item.get("Size") and item["MediaSources"][0].get("Size"):
            item["Size"] = item["MediaSources"][0]["Size"]

    if not bitrate and "MediaStreams" in item:
        v_stream = next((s for s in item["MediaStreams"] if s.get("Type") == "Video"), None)
        if v_stream:
            bitrate = v_stream.get("BitRate")

    raw_path = cast("str", item.get("Path", ""))
    mapped_path = apply_path_mapping(raw_path)
    played = item.get("UserData", {}).get("Played", False)

    return {
        "watched": "✅" if played else "❌",
        "size (GB)": round(item.get("Size", 0) / (1024**3), 2) if item.get("Size") else 0,
        "bitrate (Mbps)": round(bitrate / (10**6), 2) if bitrate else 0,
        "name": item.get("Name"),
        "path": mapped_path,
    }


async def perform_scan() -> pd.DataFrame:
    if not jellyfin_url.value or not jellyfin_api_key.value:
        return pd.DataFrame()

    headers = {"X-Emby-Token": jellyfin_api_key.value, "Accept": "application/json"}
    params: dict[str, Any] = {
        "Recursive": "true",
        "IncludeItemTypes": "Movie,Episode",
        "Fields": "Path,MediaStreams,UserData,MediaSources,Size",
    }
    if jellyfin_user_id.value.strip():
        params["UserId"] = jellyfin_user_id.value.strip()

    url = f"{jellyfin_url.value.rstrip('/')}/Items"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, params=params, timeout=15.0)
            response.raise_for_status()
            items = response.json().get("Items", [])
        except httpx.HTTPError as e:
            return pd.DataFrame([{"error": str(e)}])

    try:
        data = [parse_jellyfin_item(i) for i in items]
    except ValueError as e:
        return pd.DataFrame([{"error": str(e)}])

    df = pd.DataFrame(data)
    if not df.empty and "bitrate (Mbps)" in df.columns:
        df = df.sort_values("bitrate (Mbps)", ascending=False).reset_index(drop=True)
    return df


@solara.component
def media_tab() -> None:
    refresh_trigger = solara.use_reactive(0)
    error_msg = solara.use_reactive(cast("str | None", None))
    scan_task = solara.lab.use_task(
        perform_scan,
        dependencies=[refresh_trigger.value, jellyfin_url.value, jellyfin_api_key.value, jellyfin_user_id.value],
    )

    with solara.Column():
        with solara.Row(justify="space-between", style={"align-items": "center"}):
            solara.Markdown("### 📂 Jellyfin Library")
            solara.Button(
                "Refresh Scan",
                icon_name="mdi-refresh",
                on_click=lambda: refresh_trigger.set(refresh_trigger.value + 1),
            )

        if error_msg.value:
            with solara.Row(justify="space-between", style={"align-items": "center", "background-color": "#422", "padding": "10px", "border-radius": "4px", "margin": "10px 0"}):
                solara.Error(error_msg.value)
                solara.Button("Clear", on_click=lambda: error_msg.set(None), text=True)

        if scan_task.pending:
            solara.ProgressLinear(value=True)
        elif scan_task.finished:
            df = cast("pd.DataFrame", scan_task.value)
            if df is None or df.empty:
                solara.Warning("No items found.")
            elif "error" in df.columns:
                solara.Error(f"Error: {df['error'].iloc[0]}")
            else:

                def add_to_queue(row: dict[str, Any]) -> None:
                    error_msg.set(None)
                    path = Path(row["path"])
                    if not path.exists():
                        error_msg.set(f"❌ File not found: {path}. Check your Path Mapping in Settings.")
                        return

                    if any(item.path == row["path"] for item in transcode_queue.value):
                        return

                    new_item = TranscodeItem(
                        name=row["name"],
                        path=row["path"],
                        status="queued",
                        added_at=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M"),
                    )
                    transcode_queue.value = [*transcode_queue.value, new_item]

                with solara.Column(gap="0px"):
                    # Table Header
                    with solara.Row(style={"font-weight": "bold", "border-bottom": "2px solid #555", "padding": "10px 0"}):
                        solara.Text("Watched", style={"width": "80px", "text-align": "center"})
                        solara.Text("Size (GB)", style={"width": "100px", "text-align": "right"})
                        solara.Text("Bitrate (Mbps)", style={"width": "120px", "text-align": "right"})
                        solara.Text("Name", style={"flex-grow": "1", "margin-left": "20px"})
                        solara.Text("Path", style={"flex-grow": "1", "margin-left": "20px", "overflow": "hidden", "text-overflow": "ellipsis", "white-space": "nowrap"})
                        solara.Text("Action", style={"width": "100px", "text-align": "center"})

                    # Table Rows
                    for _, row in df.iterrows():
                        r_dict = row.to_dict()
                        with solara.Row(style={"border-bottom": "1px solid #444", "padding": "5px 0", "align-items": "center"}):
                            solara.Text(r_dict["watched"], style={"width": "80px", "text-align": "center"})
                            solara.Text(f"{r_dict['size (GB)']:.2f}", style={"width": "100px", "text-align": "right"})
                            solara.Text(f"{r_dict['bitrate (Mbps)']:.2f}", style={"width": "120px", "text-align": "right"})
                            solara.Text(r_dict["name"], style={"flex-grow": "1", "margin-left": "20px", "font-weight": "500"})
                            solara.Text(r_dict["path"], style={"flex-grow": "1", "margin-left": "20px", "font-size": "0.9em", "color": "#aaa", "overflow": "hidden", "text-overflow": "ellipsis", "white-space": "nowrap"})
                            with solara.Row(justify="center", style={"width": "100px"}):
                                solara.Button(
                                    "ADD",
                                    icon_name="mdi-plus-thick",
                                    color="primary",
                                    text=True,
                                    on_click=lambda r=r_dict: add_to_queue(r),
                                )


@solara.component
def queue_item_view(item: TranscodeItem) -> None:
    with solara.Card(item.name, margin=1):
        with solara.Row(justify="space-between"):
            status_map = {"queued": "grey", "processing": "blue", "completed": "green", "failed": "red"}
            color = status_map.get(item.status, "grey")
            solara.Markdown(f"**Status:** <span style='color: {color}'>{item.status.upper()}</span>")

            if item.status != "processing":

                def remove(path: str = item.path) -> None:
                    transcode_queue.value = [x for x in transcode_queue.value if x.path != path]

                solara.Button("Remove", icon_name="mdi-delete", on_click=remove, color="error", text=True)

        if item.status == "processing":
            solara.Markdown(f"**Speed:** {item.speed}")

        solara.Markdown(f"**Path:** `{item.path}`")
        if item.error:
            with solara.Error():
                solara.Markdown("**Error Details:**")
                solara.Preformatted(item.error)


@solara.component
def queue_tab() -> None:
    def start_worker() -> None:
        task = asyncio.create_task(run_worker())
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)

    with solara.Column(margin=2):
        with solara.Row(justify="space-between", style={"align-items": "center"}):
            solara.Markdown("### ⚙️ Transcode Queue")

            with solara.Row():
                if any(it.status == "queued" for it in transcode_queue.value) and not is_processing.value:
                    solara.Button("Start Processing", icon_name="mdi-play", on_click=start_worker, color="primary")

                if any(it.status == "completed" for it in transcode_queue.value):

                    def clear_completed() -> None:
                        transcode_queue.value = [it for it in transcode_queue.value if it.status != "completed"]

                    solara.Button("Clear Completed", icon_name="mdi-delete-sweep", on_click=clear_completed, text=True)

        if not transcode_queue.value:
            solara.Info("Queue is empty.")
            return

        if is_processing.value:
            solara.Markdown("🚀 **Transcoding active...**")
            solara.ProgressLinear(value=True)

        for item in transcode_queue.value:
            queue_item_view(item)


@solara.component
def page() -> None:
    with solara.lab.Tabs():
        with solara.lab.Tab("Library", icon_name="mdi-movie-search"):
            media_tab()
        with solara.lab.Tab("Queue", icon_name="mdi-tray-full"):
            queue_tab()
        with solara.lab.Tab("Health", icon_name="mdi-heart-pulse"):
            health_monitor()
        with solara.lab.Tab("Settings", icon_name="mdi-cog"):
            settings_tab()
