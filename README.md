# Trivial Transcoder

A simple Jellyfin video transcoder app built with FastAPI and HTMX.

## Features
- AV1 transcoding with SVT-AV1.
- Jellyfin library integration.
- Path mapping for Docker/Network environments.
- Real-time queue and system health monitoring via HTMX.
- Persistent settings in SQLite.

## Running the app

```bash
uv sync
uv run uvicorn src.trivial_transcoder:app --reload
```

Then visit `http://localhost:8000`
