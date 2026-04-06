# Trivial Transcoder

A simple Jellyfin video transcoder app built with FastAPI and HTMX.

## Features
- AV1 transcoding with SVT-AV1.
- Jellyfin library integration.
- Path mapping for Docker/Network environments.
- Real-time queue and system health monitoring via HTMX.
- Persistent settings in SQLite.

## Running the app

Suggested to use docker compose, most environment variables must be defined.

```yaml
services:
  transcoder:
    build: https://github.com/Christian-Andersen/trivial-transcoder.git
    ports:
      - "7000:7000"
    environment:
      - JELLYFIN_URL=http://jellyfin:8096
      - JELLYFIN_API_KEY=JELLYFIN_API_KEY
      - JELLYFIN_USER_ID=JELLYFIN_USER_ID
      - JELLYFIN_PATH_PREFIX=/jellyfin/media
      - APP_PATH_PREFIX=/media
      - CACHE_DIR=/media/cache
      - PRESET=4
      - CRF=25
      - GRAIN=10
      - THEME_DARK=true
    volumes:
      - "/media:/media"
    restart: unless-stopped
    user: "1000:1000"
```
