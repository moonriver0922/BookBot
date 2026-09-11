from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    # Reuse top-level run.py CLI implementation.
    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root))
    from run import main as run_main

    run_main()


if __name__ == "__main__":
    main()
