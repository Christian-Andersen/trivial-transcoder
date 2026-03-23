import asyncio
import re
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import psutil

from .db import db, get_settings
from .models import TranscodeItem

FFMPEG_PROGRESS_PATTERN = re.compile(r"(frame|fps|time|speed|bitrate)= *([^ ]+)")
STDERR_LOG_LINES = 15


class WorkerState:
    def __init__(self) -> None:
        _raw_queue = cast("list[dict[str, Any]]", db.get("transcode_queue", []))
        self.transcode_queue: list[TranscodeItem] = [TranscodeItem.from_dict(i) for i in _raw_queue]
        self.is_processing = False
        self.active_processes: dict[str, asyncio.subprocess.Process] = {}

    def persist_queue(self) -> None:
        db.set("transcode_queue", [asdict(item) for item in self.transcode_queue])


state = WorkerState()


def apply_path_mapping(j_path: str) -> str:
    path_map = str(get_settings().get("path_map", ""))
    if not path_map or ":" not in path_map:
        return j_path
    j_prefix, app_prefix = path_map.split(":", 1)
    if not j_path.startswith(j_prefix):
        return j_path
    return j_path.replace(j_prefix, app_prefix, 1)


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
    settings = get_settings()
    return [
        "ffmpeg",
        "-progress",
        "pipe:2",
        "-i",
        str(input_path),
        "-map",
        "0",
        "-vf",
        "scale=1920:-1:flags=lanczos",
        "-c:v",
        "libsvtav1",
        "-preset",
        str(settings.get("preset")),
        "-crf",
        str(settings.get("crf")),
        "-pix_fmt",
        "yuv420p10le",
        "-svtav1-params",
        f"tune=0:enable-hdr=1:scd=1:film-grain={settings.get('grain')}:enable-restoration=1:enable-cdef=1",
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


async def process_item(item: TranscodeItem) -> None:  # noqa: C901, PLR0912, PLR0915
    input_path = Path(item.path)
    cache_dir = get_settings().get("cache_dir", "")

    if cache_dir:
        cache_path = Path(cache_dir)
        await asyncio.to_thread(cache_path.mkdir, parents=True, exist_ok=True)
        temp_path = cache_path / f"{input_path.stem}_temp_av1.mkv"
    else:
        temp_path = input_path.with_suffix("").with_name(f"{input_path.stem}_temp_av1.mkv")

    if item.total_frames == 0:
        item.total_frames = await get_total_frames(item.path)

    cmd = get_ffmpeg_command(input_path, temp_path)
    stderr_log: list[str] = []

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
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
                            if len(stderr_log) > STDERR_LOG_LINES:
                                stderr_log.pop(0)
                        await parse_ffmpeg_progress(line, item)

        if process.stderr:
            await asyncio.gather(process.wait(), read_stderr(process.stderr))
        else:
            await process.wait()

        if item.status == "cancelled":
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)
            return

        if process.returncode == 0:
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.replace, input_path)
                item.status = "completed"
                item.percent = 100.0
            else:
                item.status = "failed"
                item.error = "Temp file missing"
        else:
            item.status = "failed"
            item.error = f"Exit code {process.returncode}: " + "\n".join(stderr_log)
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)

    except (OSError, asyncio.CancelledError) as e:
        item.status = "failed"
        item.error = str(e)
        if await asyncio.to_thread(temp_path.exists):
            await asyncio.to_thread(temp_path.unlink)
    finally:
        state.active_processes.pop(item.path, None)
        state.persist_queue()


async def run_worker() -> None:
    if state.is_processing:
        return

    state.is_processing = True
    try:
        while True:
            next_item = next((it for it in state.transcode_queue if it.status == "queued"), None)
            if not next_item:
                break

            next_item.status = "processing"
            state.persist_queue()
            await process_item(next_item)
    finally:
        state.is_processing = False


async def cancel_item(index: int) -> None:
    if 0 <= index < len(state.transcode_queue):
        item = state.transcode_queue[index]
        if item.status == "processing":
            item.status = "cancelled"
            process = state.active_processes.get(item.path)
            if process:
                with suppress(Exception):
                    process.terminate()
        elif item.status == "queued":
            item.status = "cancelled"
        state.persist_queue()


def get_stray_ffmpeg_processes() -> list[dict[str, Any]]:
    strays = []
    current_pid = psutil.Process().pid
    for proc in psutil.process_iter(["pid", "name", "cmdline", "ppid"]):
        try:
            if proc.info["name"] == "ffmpeg":
                cmdline = proc.info["cmdline"] or []
                if any("_temp_av1.mkv" in arg for i, arg in enumerate(cmdline)) and proc.info["ppid"] != current_pid:
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
