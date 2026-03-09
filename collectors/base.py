import os
import json
import logging
import requests
from datetime import datetime, timezone
from abc import ABC, abstractmethod
import duckdb
from config.settings import RAW_DIR, DB_PATH, NOTIFY_API_URL, INTERNAL_API_KEY

logger = logging.getLogger("collectors")


class CollectorBase(ABC):
    source_name: str  # override in subclass

    def save_raw(self, payload: dict | list | str, subdir: str = None) -> str:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts    = datetime.now(timezone.utc).strftime("%H%M%S")
        folder = os.path.join(RAW_DIR, subdir or self.source_name, today)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{ts}.json")
        content = json.dumps(payload, indent=2) if not isinstance(payload, str) else payload
        with open(path, "w") as f:
            f.write(content)
        return path

    def record_health(self, status: str, error: str = None):
        conn = duckdb.connect(DB_PATH)
        now  = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO collector_health
                (source_name, last_attempt, last_success, last_status,
                 consecutive_failures, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_name) DO UPDATE SET
                last_attempt         = excluded.last_attempt,
                last_success         = CASE WHEN excluded.last_status = 'ok'
                                           THEN excluded.last_attempt
                                           ELSE last_success END,
                last_status          = excluded.last_status,
                consecutive_failures = CASE WHEN excluded.last_status = 'ok'
                                           THEN 0
                                           ELSE consecutive_failures + 1 END,
                error_message        = excluded.error_message
        """, [self.source_name, now, now if status == "ok" else None, status, 0, error])
        conn.close()

    def _notify(self, source_name: str) -> None:
        """Fire-and-forget POST to Go API SSE broker."""
        try:
            requests.post(
                NOTIFY_API_URL,
                data=source_name.encode(),
                headers={
                    "Content-Type": "text/plain",
                    "X-Internal-Key": INTERNAL_API_KEY,
                },
                timeout=2,
            )
        except Exception:
            pass  # Non-critical — SSE push is best-effort

    @abstractmethod
    def collect(self) -> dict:
        pass

    def run(self) -> dict:
        logger.info(f"[{self.source_name}] starting collection")
        try:
            result = self.collect()
            self.record_health("ok")
            self._notify(self.source_name)
            logger.info(f"[{self.source_name}] success: {result}")
            return result
        except Exception as e:
            self.record_health("error", str(e))
            logger.error(f"[{self.source_name}] failed: {e}")
            return {"status": "error", "error": str(e)}
