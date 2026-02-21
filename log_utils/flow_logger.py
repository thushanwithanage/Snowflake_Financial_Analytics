import json
import os
from datetime import datetime, timezone
from config.settings import DATA_CONFIG
from prefect import get_run_logger

def get_log_path(flow_name: str) -> str:
    log_folder = DATA_CONFIG["log_folder"]
    os.makedirs(log_folder, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return os.path.join(log_folder, f"{flow_name}_{timestamp}.json")

class FlowLogger:
    def __init__(self, log_path: str, entity: str = "flow"):
        self.log_path = log_path
        self.entity = entity
        self.prefect_logger = get_run_logger()

    def _write(self, level: str, message: str, extra: dict = None):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "entity": self.entity,
            "message": message,
            **(extra or {})
        }
        logs = []
        if os.path.exists(self.log_path):
            with open(self.log_path, "r") as f:
                logs = json.load(f)
        logs.append(entry)
        with open(self.log_path, "w") as f:
            json.dump(logs, f, indent=2)

        # Forward to Prefect logger
        prefect_log = getattr(self.prefect_logger, level.lower(), self.prefect_logger.info)
        prefect_log(f"[{self.entity}] {message}")

    def info(self, message: str, extra: dict = None):
        self._write("INFO", message, extra)

    def warning(self, message: str, extra: dict = None):
        self._write("WARNING", message, extra)

    def error(self, message: str, extra: dict = None):
        self._write("ERROR", message, extra)