# logging_config.py
import logging
import yaml
from pathlib import Path
"""
Central logging configuration for the ingestion project.

Initializes root logging based on paths defined in config/config.yaml,
ensuring logs are written to a configured log file and echoed to the
console. Safe to call multiple times without adding duplicate handlers.
"""


def setup_logging(config_path: str = "config/config.yaml") -> None:
    """
    Configure root logging to write to the log file specified in config.yaml
    (paths.log_file) and also echo to the console.
    Safe to call multiple times; subsequent calls are no-ops if handlers exist.
    """
    if logging.getLogger().handlers:
        # Already configured → don't add duplicate handlers
        return

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    log_file = cfg["paths"]["log_file"]
    log_path = Path(log_file)

    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ],
    )
