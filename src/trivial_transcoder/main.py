import asyncio
import datetime
import html
import json
import os
import re
import sys
from contextlib import suppress
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import httpx
import psutil
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import AfterValidator, BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings

FFMPEG_PROGRESS_PATTERN = re.compile(r"(frame|fps|time|speed|bitrate)=([^ ]+)")


class TaskStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


def exists(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(path)
    return path


class Settings(BaseSettings):
    jellyfin_url: HttpUrl
    jellyfin_api_key: str
    jellyfin_user_id: str
    jellyfin_path_prefix: Path | None = None
    app_path_prefix: Annotated[Path, AfterValidator(exists)] | None = None
    cache_dir: str | None = None
    preset: Annotated[int, Field(ge=0, le=13)]
    crf: Annotated[int, Field(ge=1, le=63)]
    grain: Annotated[int, Field(ge=0, le=100)]
    port: Annotated[int, Field(ge=1, le=65535)] = 7000
    startup_sample_size: Annotated[int, Field(default=100, ge=1)]
    max_threads: Annotated[int | None, Field(default=None, ge=-1)] = None
    nice_value: Annotated[int, Field(default=0, ge=0, le=19)] = 0


settings = Settings()  # ty:ignore[missing-argument]


class TranscodeItem(BaseModel):
    name: str
    path: str
    status: TaskStatus = TaskStatus.QUEUED
    progress: float = 0.0
    total_frames: int = 0
    percent: float = 0.0
    speed: str = "N/A"
    time_remaining: str = "N/A"
    error: str = ""
    added_at: str = ""

    def __post_init__(self) -> None:
        if not self.added_at:
            self.added_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M")


class WorkerState:
    def __init__(self) -> None:
        self.transcode_queue: list[TranscodeItem] = []
        self.is_processing = False
        self.active_processes: dict[str, asyncio.subprocess.Process] = {}
        self.lock: asyncio.Lock = asyncio.Lock()


def escape_html(text: str) -> str:
    return html.escape(text)


state = WorkerState()

app = FastAPI(title="Trivial Transcoder")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def apply_path_mapping(j_path: Path | str) -> Path:
    jellyfin_path = Path(j_path)
    if (settings.jellyfin_path_prefix is None) or (settings.app_path_prefix is None):
        return jellyfin_path
    try:
        relative_j_path = jellyfin_path.relative_to(settings.jellyfin_path_prefix)
    except ValueError as err:
        if jellyfin_path.is_absolute() and not str(jellyfin_path).startswith(str(settings.jellyfin_path_prefix)):
            msg = f"Path {j_path} is outside Jellyfin path prefix"
            raise ValueError(msg) from err
        return jellyfin_path
    mapped = settings.app_path_prefix / relative_j_path
    resolved = mapped.resolve()
    if not str(resolved).startswith(str(settings.app_path_prefix.resolve())):
        msg = "Path would escape app_path_prefix"
        raise ValueError(msg)
    return mapped


async def get_total_frames(path: str) -> int:
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-count_packets",
            "-show_entries",
            "stream=nb_read_packets",
            "-of",
            "csv=p=0",
            path,
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        if process.returncode == 0:
            return int(stdout.decode().strip())
    except OSError, ValueError:
        pass
    return 0


async def parse_ffmpeg_progress(line: str, item: TranscodeItem) -> None:
    matches = FFMPEG_PROGRESS_PATTERN.findall(line)
    if not matches:
        return
    stats = dict(matches)
    if "speed" in stats:
        item.speed = stats["speed"].rstrip("x")
    if "frame" in stats:
        try:
            item.progress = float(stats["frame"])
            if item.total_frames > 0:
                item.percent = min(99.9, (item.progress / item.total_frames) * 100)
        except ValueError:
            pass


def get_ffmpeg_command(input_path: Path, temp_path: Path) -> list[str]:
    cmd = [
        "ffmpeg",
        "-progress",
        "pipe:2",
        "-i",
        str(input_path),
        "-map",
        "0",
        "-c",
        "copy",
        "-c:V",
        "libsvtav1",
        "-filter:V",
        "scale=1920:-1:flags=lanczos",
        "-preset",
        str(settings.preset),
        "-crf",
        str(settings.crf),
        "-pix_fmt",
        "yuv420p10le",
        "-svtav1-params",
        f"tune=0:scd=1:film-grain={settings.grain}:enable-restoration=1:enable-cdef=1",
        "-c:a",
        "libopus",
        "-b:a",
        "128k",
        "-ac",
        "2",
    ]
    if settings.max_threads is not None and settings.max_threads > 0:
        cmd.extend(["-threads", str(settings.max_threads)])
    cmd.extend(["-y", str(temp_path)])
    return cmd


async def process_item(item: TranscodeItem) -> None:  # noqa: C901, PLR0912, PLR0915
    input_path = Path(item.path)

    if settings.cache_dir:
        cache_path = Path(settings.cache_dir)
        await asyncio.to_thread(cache_path.mkdir, parents=True, exist_ok=True)
        temp_path = cache_path / f"{input_path.stem}_temp_av1.mkv"
    else:
        temp_path = input_path.with_suffix("").with_name(f"{input_path.stem}_temp_av1.mkv")

    if item.total_frames == 0:
        if item.status == TaskStatus.CANCELLED:
            return
        item.total_frames = await get_total_frames(item.path)

    if item.status == TaskStatus.CANCELLED:
        return

    cmd = get_ffmpeg_command(input_path, temp_path)
    stderr_log: list[str] = []

    def _set_nice() -> None:
        if settings.nice_value > 0:
            os.nice(settings.nice_value)

    preexec_fn: Any = _set_nice if sys.platform != "win32" and settings.nice_value > 0 else None

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=preexec_fn,
        )
        state.active_processes[item.path] = process

        async def read_stderr(stream: asyncio.StreamReader) -> None:
            buffer = ""
            while True:
                chunk = await stream.read(1024)
                if not chunk:
                    break
                buffer += chunk.decode(errors="replace")
                while "\r" in buffer or "\n" in buffer:
                    r_pos = buffer.find("\r")
                    n_pos = buffer.find("\n")
                    if r_pos == -1:
                        pos = n_pos
                    elif n_pos == -1:
                        pos = r_pos
                    else:
                        pos = min(r_pos, n_pos)
                    line = buffer[:pos].strip()
                    buffer = buffer[pos + 1 :]
                    if line:
                        if not any(x in line for x in ["frame=", "fps=", "speed=", "bitrate="]):
                            stderr_log.append(line)
                        await parse_ffmpeg_progress(line, item)

        if process.stderr:
            await asyncio.gather(process.wait(), read_stderr(process.stderr))
        else:
            await process.wait()

        if item.status == TaskStatus.CANCELLED:
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)
            return

        if process.returncode == 0:
            if await asyncio.to_thread(temp_path.exists) and await asyncio.to_thread(input_path.exists):
                try:
                    await asyncio.to_thread(temp_path.replace, input_path)
                    item.status = TaskStatus.COMPLETED
                    item.percent = 100.0
                except OSError as e:
                    item.status = TaskStatus.FAILED
                    item.error = f"Failed to replace file: {e}"
                    if await asyncio.to_thread(temp_path.exists):
                        await asyncio.to_thread(temp_path.unlink)
            else:
                item.status = TaskStatus.FAILED
                item.error = "Temp file or input file missing"
        else:
            item.status = TaskStatus.FAILED
            item.error = f"Exit code {process.returncode}: " + "\n".join(stderr_log)
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)

    except (OSError, asyncio.CancelledError) as e:
        item.status = TaskStatus.FAILED
        item.error = str(e)
        if await asyncio.to_thread(temp_path.exists):
            await asyncio.to_thread(temp_path.unlink)
    finally:
        state.active_processes.pop(item.path, None)


async def run_worker() -> None:
    async with state.lock:
        if state.is_processing:
            return
        state.is_processing = True
    try:
        while True:
            async with state.lock:
                next_item = next((it for it in state.transcode_queue if it.status == TaskStatus.QUEUED), None)
                if not next_item:
                    break

            next_item.status = TaskStatus.PROCESSING
            await process_item(next_item)
    finally:
        async with state.lock:
            state.is_processing = False


async def cancel_item(index: int) -> None:
    if 0 <= index < len(state.transcode_queue):
        item = state.transcode_queue[index]
        if item.status == TaskStatus.PROCESSING:
            item.status = TaskStatus.CANCELLED
            process = state.active_processes.get(item.path)
            if process:
                with suppress(Exception):
                    process.terminate()
        elif item.status == TaskStatus.QUEUED:
            item.status = TaskStatus.CANCELLED


def get_stray_ffmpeg_processes() -> list[dict[str, Any]]:
    strays = []
    try:
        current_pid = psutil.Process().pid
    except psutil.NoSuchProcess:
        current_pid = None
    for proc in psutil.process_iter(["pid", "name", "cmdline", "ppid"]):
        try:
            if proc.info["name"] == "ffmpeg":
                cmdline = proc.info["cmdline"] or []
                if any("_temp_av1.mkv" in arg for arg in cmdline) and proc.info["ppid"] != current_pid:
                    strays.append(proc.info)
        except psutil.NoSuchProcess, psutil.AccessDenied:
            continue
    return strays


def kill_all_ffmpeg() -> int:
    count = 0
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            if proc.info["name"] == "ffmpeg":
                cmdline = proc.info["cmdline"] or []
                if any("_temp_av1.mkv" in arg for arg in cmdline):
                    proc.kill()
                    count += 1
        except psutil.NoSuchProcess, psutil.AccessDenied:
            continue
    return count


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {"settings": settings.model_dump_json()})


@app.get("/health", response_class=HTMLResponse)
async def health(request: Request) -> HTMLResponse:
    cpu = psutil.cpu_percent()
    mem = psutil.virtual_memory().percent
    temps = {}
    if hasattr(psutil, "sensors_temperatures"):
        try:
            raw_temps = psutil.sensors_temperatures()
            if raw_temps:
                for name, entries in raw_temps.items():
                    if entries:
                        temps[name.capitalize()] = entries[0].current
        except KeyError, AttributeError:
            pass
    return templates.TemplateResponse(request, "health.html", {"cpu": cpu, "mem": mem, "temps": temps})


@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, scan: bool = False, error: str | None = None) -> HTMLResponse:  # noqa: FBT002, FBT001
    items = []
    if scan:
        headers = {"X-Emby-Token": settings.jellyfin_api_key, "Accept": "application/json"}
        params = {
            "Recursive": "true",
            "IncludeItemTypes": "Movie,Episode",
            "Fields": "Path,MediaStreams,UserData,MediaSources,Size,SeriesName",
        }
        if settings.jellyfin_user_id.strip():
            params["UserId"] = str(settings.jellyfin_user_id).strip()

        jellyfin_base = str(settings.jellyfin_url).rstrip("/")
        url = f"{jellyfin_base}/Items"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=params, timeout=15.0)
                response.raise_for_status()
                raw_items = response.json().get("Items", [])
                for i in raw_items:
                    bitrate = 0
                    size = i.get("Size", 0)
                    if i.get("MediaSources"):
                        bitrate = i["MediaSources"][0].get("Bitrate", 0)
                        if not size:
                            size = i["MediaSources"][0].get("Size", 0)

                    raw_path = i.get("Path", "")
                    try:
                        mapped_path = apply_path_mapping(raw_path)
                    except ValueError as e:
                        error = f"Path mapping error: {e}"
                        continue
                    played = i.get("UserData", {}).get("Played", False)
                    series_name = i.get("SeriesName")
                    name = i.get("Name")
                    display_name = f"{series_name} - {name}" if series_name else name

                    items.append(
                        {
                            "name": display_name,
                            "path": str(mapped_path),
                            "size (GB)": round(size / (1024**3), 2) if size else 0,
                            "bitrate (Mbps)": round(bitrate / (10**6), 2) if bitrate else 0,
                            "watched": played,
                        }
                    )
                items.sort(key=lambda x: x["bitrate (Mbps)"], reverse=True)
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                error = f"Jellyfin Error: {e!s}"

    return templates.TemplateResponse(request, "library.html", {"items": items, "error": error})


@app.post("/queue/add")
async def queue_add(
    background_tasks: BackgroundTasks,
    name: Annotated[str, Form(...)],
    path: Annotated[str, Form(...)],
) -> Response:
    if not await asyncio.to_thread(Path(path).is_file):
        escaped_path = escape_html(path)
        return Response(
            status_code=204,
            headers={
                "HX-Trigger": json.dumps(
                    {"show-toast": {"message": f"❌ File not found: {escaped_path}", "type": "error"}}
                )
            },
        )

    if any(
        item.path == path for item in state.transcode_queue if item.status in [TaskStatus.QUEUED, TaskStatus.PROCESSING]
    ):
        escaped_name = escape_html(name)
        return Response(
            status_code=204,
            headers={
                "HX-Trigger": json.dumps(
                    {
                        "show-toast": {"message": f"⚠️ Item already in queue: {escaped_name}", "type": "warning"},
                        "queue-updated": {},
                    }
                )
            },
        )

    state.transcode_queue.append(TranscodeItem(name=name, path=path, status=TaskStatus.QUEUED))

    async with state.lock:
        if not state.is_processing:
            background_tasks.add_task(run_worker)

    escaped_name = escape_html(name)
    return Response(
        status_code=204,
        headers={
            "HX-Trigger": json.dumps(
                {
                    "show-toast": {"message": f"✅ Added to queue: {escaped_name}", "type": "success"},
                    "queue-updated": {},
                }
            )
        },
    )


@app.get("/queue", response_class=HTMLResponse)
async def get_queue(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "queue.html",
        {
            "items": state.transcode_queue,
            "is_processing": state.is_processing,
            "any_queued": any(it.status == TaskStatus.QUEUED for it in state.transcode_queue),
            "any_completed": any(
                it.status in {TaskStatus.COMPLETED, TaskStatus.CANCELLED, TaskStatus.FAILED}
                for it in state.transcode_queue
            ),
        },
    )


@app.post("/queue/start", response_class=HTMLResponse)
async def queue_start(request: Request, background_tasks: BackgroundTasks) -> HTMLResponse:
    background_tasks.add_task(run_worker)
    return await get_queue(request)


@app.post("/queue/clear", response_class=HTMLResponse)
async def queue_clear(request: Request) -> HTMLResponse:
    state.transcode_queue[:] = [
        it for it in state.transcode_queue if it.status in {TaskStatus.PROCESSING, TaskStatus.QUEUED}
    ]
    return await get_queue(request)


@app.delete("/queue/remove/{index}", response_class=HTMLResponse)
async def queue_remove(request: Request, index: int) -> HTMLResponse:
    if 0 <= index < len(state.transcode_queue) and state.transcode_queue[index].status != TaskStatus.PROCESSING:
        state.transcode_queue.pop(index)
    return await get_queue(request)


@app.post("/queue/cancel/{index}", response_class=HTMLResponse)
async def queue_cancel(request: Request, index: int) -> HTMLResponse:
    await cancel_item(index)
    return await get_queue(request)


@app.get("/system/strays", response_class=HTMLResponse)
async def system_strays(_request: Request) -> HTMLResponse:
    strays = get_stray_ffmpeg_processes()
    return HTMLResponse(f"<span>{len(strays)} orphan tasks</span>")


@app.post("/system/cleanup", response_class=HTMLResponse)
async def system_cleanup(_request: Request) -> Response:
    count = kill_all_ffmpeg()
    return Response(
        status_code=204,
        headers={
            "HX-Trigger": json.dumps({"show-toast": {"message": f"🧹 Killed {count} ffmpeg processes", "type": "info"}})
        },
    )


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.port)  # noqa: S104
