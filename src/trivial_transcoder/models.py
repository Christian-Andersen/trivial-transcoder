from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any


@dataclass
class TranscodeItem:
    name: str
    path: str
    status: str  # "queued", "processing", "completed", "failed", "cancelled"
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TranscodeItem:
        # Filter out keys that might have been added in newer versions but are missing in old DB records
        valid_keys = {
            "name",
            "path",
            "status",
            "progress",
            "total_frames",
            "percent",
            "speed",
            "time_remaining",
            "error",
            "added_at",
        }
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered_data)
