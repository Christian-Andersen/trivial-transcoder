import json
import sqlite3
from pathlib import Path

DB_PATH = "trivial_transcoder.db"
DEFAULT_JELLYFIN_URL = "http://localhost:8096"


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
        except sqlite3.Error, json.JSONDecodeError, TypeError:
            return default
        finally:
            conn.close()


db = Database()


from typing import Any


# Settings helper
def get_settings() -> dict[str, Any]:
    return {
        "jellyfin_url": db.get("jellyfin_url", DEFAULT_JELLYFIN_URL),
        "jellyfin_api_key": db.get("jellyfin_api_key", ""),
        "jellyfin_user_id": db.get("jellyfin_user_id", ""),
        "preset": db.get("preset", 4),
        "crf": db.get("crf", 25),
        "grain": db.get("grain", 10),
        "path_map": db.get("path_map", ""),
        "cache_dir": db.get("cache_dir", ""),
        "theme_dark": db.get("theme_dark", True),
    }


def save_settings(settings: dict[str, Any]) -> None:
    for key, value in settings.items():
        db.set(key, value)
