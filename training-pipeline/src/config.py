"""
training-pipeline/src/config.py
Loads training.yaml and exposes a typed config dict.
All modules import from here — nothing hardcoded elsewhere.
"""

import os
import yaml
from pathlib import Path

_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "configs" / "training.yaml"


def load_config(path: str = None) -> dict:
    """
    Load YAML config. Path resolution order:
      1. argument passed directly
      2. CONFIG_PATH env var
      3. configs/training.yaml (relative to this file)
    """
    config_path = path or os.environ.get("CONFIG_PATH", str(_DEFAULT_CONFIG_PATH))
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


# Module-level singleton — import this everywhere
CFG = load_config()