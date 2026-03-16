import json
from pathlib import Path

import httpx
import psutil
from fastapi import BackgroundTasks, FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .db import get_settings, save_settings
from .models import TranscodeItem
from .worker import (
    apply_path_mapping,
    cancel_item,
    get_stray_ffmpeg_processes,
    kill_all_ffmpeg,
    run_worker,
    state,
)

app = FastAPI(title="Trivial Transcoder")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "settings": get_settings()})


@app.get("/health", response_class=HTMLResponse)
async def health(request: Request):
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
        except Exception:
            pass
    return templates.TemplateResponse("health.html", {"request": request, "cpu": cpu, "mem": mem, "temps": temps})


@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, scan: bool = False, error: str | None = None):
    items = []
    if scan:
        settings = get_settings()
        if not settings["jellyfin_url"] or not settings["jellyfin_api_key"]:
            error = "Jellyfin URL and API Key are required for scanning."
        else:
            headers = {"X-Emby-Token": str(settings["jellyfin_api_key"]), "Accept": "application/json"}
            params = {
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Episode",
                "Fields": "Path,MediaStreams,UserData,MediaSources,Size",
            }
            if str(settings["jellyfin_user_id"]).strip():
                params["UserId"] = str(settings["jellyfin_user_id"]).strip()

            url = f"{str(settings['jellyfin_url']).rstrip('/')}/Items"
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
                        mapped_path = apply_path_mapping(raw_path)
                        played = i.get("UserData", {}).get("Played", False)

                        items.append(
                            {
                                "name": i.get("Name"),
                                "path": mapped_path,
                                "size (GB)": round(size / (1024**3), 2) if size else 0,
                                "bitrate (Mbps)": round(bitrate / (10**6), 2) if bitrate else 0,
                                "watched": played,
                            }
                        )
                    items.sort(key=lambda x: x["bitrate (Mbps)"], reverse=True)
                except Exception as e:
                    error = f"Jellyfin Error: {e!s}"

    return templates.TemplateResponse("library.html", {"request": request, "items": items, "error": error})


@app.post("/queue/add")
async def queue_add(request: Request, background_tasks: BackgroundTasks, name: str = Form(...), path: str = Form(...)):
    if not Path(path).exists():
        return Response(
            status_code=204,
            headers={
                "HX-Trigger": json.dumps({"show-toast": {"message": f"❌ File not found: {path}", "type": "error"}})
            },
        )

    if any(item.path == path for item in state.transcode_queue if item.status in ["queued", "processing"]):
        return Response(
            status_code=204,
            headers={
                "HX-Trigger": json.dumps(
                    {
                        "show-toast": {"message": f"⚠️ Item already in queue: {name}", "type": "warning"},
                        "queue-updated": {},
                    }
                )
            },
        )

    state.transcode_queue.append(TranscodeItem(name=name, path=path, status="queued"))
    state.persist_queue()

    if not state.is_processing:
        background_tasks.add_task(run_worker)

    return Response(
        status_code=204,
        headers={
            "HX-Trigger": json.dumps(
                {"show-toast": {"message": f"✅ Added to queue: {name}", "type": "success"}, "queue-updated": {}}
            )
        },
    )


@app.get("/queue", response_class=HTMLResponse)
async def get_queue(request: Request):
    return templates.TemplateResponse(
        "queue.html",
        {
            "request": request,
            "items": state.transcode_queue,
            "is_processing": state.is_processing,
            "any_queued": any(it.status == "queued" for it in state.transcode_queue),
            "any_completed": any(it.status in ["completed", "cancelled", "failed"] for it in state.transcode_queue),
        },
    )


@app.post("/queue/start", response_class=HTMLResponse)
async def queue_start(request: Request, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_worker)
    return await get_queue(request)


@app.post("/queue/clear", response_class=HTMLResponse)
async def queue_clear(request: Request):
    state.transcode_queue[:] = [
        it for it in state.transcode_queue if it.status == "processing" or it.status == "queued"
    ]
    state.persist_queue()
    return await get_queue(request)


@app.delete("/queue/remove/{index}", response_class=HTMLResponse)
async def queue_remove(request: Request, index: int):
    if 0 <= index < len(state.transcode_queue):
        if state.transcode_queue[index].status != "processing":
            state.transcode_queue.pop(index)
            state.persist_queue()
    return await get_queue(request)


@app.post("/queue/cancel/{index}", response_class=HTMLResponse)
async def queue_cancel(request: Request, index: int):
    await cancel_item(index)
    return await get_queue(request)


@app.get("/system/strays", response_class=HTMLResponse)
async def system_strays(request: Request):
    strays = get_stray_ffmpeg_processes()
    return HTMLResponse(f"<span>{len(strays)} orphan tasks</span>")


@app.post("/system/cleanup", response_class=HTMLResponse)
async def system_cleanup(request: Request):
    count = kill_all_ffmpeg()
    return Response(
        status_code=204,
        headers={
            "HX-Trigger": json.dumps({"show-toast": {"message": f"🧹 Killed {count} ffmpeg processes", "type": "info"}})
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_get(request: Request):
    return templates.TemplateResponse(
        "settings.html", {"request": request, "settings": get_settings(), "strays": len(get_stray_ffmpeg_processes())}
    )


@app.post("/settings", response_class=HTMLResponse)
async def settings_post(
    request: Request,
    jellyfin_url: str = Form(...),
    jellyfin_api_key: str = Form(...),
    jellyfin_user_id: str = Form(""),
    path_map: str = Form(""),
    cache_dir: str = Form(""),
    preset: int = Form(...),
    crf: int = Form(...),
    grain: int = Form(...),
    theme_dark: str = Form("true"),
):
    new_settings = {
        "jellyfin_url": jellyfin_url,
        "jellyfin_api_key": jellyfin_api_key,
        "jellyfin_user_id": jellyfin_user_id,
        "path_map": path_map,
        "cache_dir": cache_dir,
        "preset": preset,
        "crf": crf,
        "grain": grain,
        "theme_dark": theme_dark == "true",
    }
    save_settings(new_settings)
    return HTMLResponse(
        '<div class="bg-green-600 text-white p-4 rounded-lg mb-4 animate-bounce">Settings Saved! Refreshing... <script>setTimeout(() => window.location.reload(), 1000)</script></div>'
    )
