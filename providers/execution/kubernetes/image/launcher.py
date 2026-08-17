"""Generic entry-point launcher baked into the Portage Spark base image
(docs/architecture/spec.md §17 — application artifact and execution
runtime image are separate concerns). Takes a dotted "<module.path>.<callable>"
(a workload's entryPoint) as its single argument, imports the module, and
calls the callable with no arguments. Providers never generate per-job
launcher code — see providers/execution/kubernetes/provider.py's
LAUNCHER_PATH, which points at this file baked into the image at build time.
"""

import importlib
import sys


def main() -> None:
    entry_point = sys.argv[1]
    module_path, _, func_name = entry_point.rpartition(".")
    module = importlib.import_module(module_path)
    getattr(module, func_name)()


if __name__ == "__main__":
    main()
