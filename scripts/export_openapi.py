#!/usr/bin/env python3
"""Exports api/main.py's FastAPI OpenAPI schema to openapi.json (repo root).
CI (.github/workflows/ci.yml) re-runs this and diffs the result against the
committed copy, failing the build on undocumented drift — so a real API
schema change always shows up as an explicit, reviewable diff in the same
PR, not a silent surprise for API consumers.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from api.main import app  # noqa: E402

OUTPUT_PATH = pathlib.Path(__file__).resolve().parents[1] / "openapi.json"


def main() -> None:
    schema = app.openapi()
    OUTPUT_PATH.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
