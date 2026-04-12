from __future__ import annotations

import sys
from pathlib import Path


def ensure_frozen_upstream_importable() -> None:
    frozen_root = Path(__file__).resolve().parents[1] / "source_code_v.0.15.0"
    frozen_root_str = str(frozen_root)
    if frozen_root.exists() and frozen_root_str not in sys.path:
        sys.path.insert(0, frozen_root_str)


ensure_frozen_upstream_importable()
