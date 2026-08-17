"""The long-running reconciler loop — a thin wrapper around the tested
functions in reconciler/service.py. Run as its own process, separate from
the API server (spec §4.5 — control-plane components fail independently).
"""

import asyncio
import logging
import os

from control_plane.db import get_session_maker
from reconciler.service import reconcile_once

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reconciler")


async def run_forever(interval_seconds: float) -> None:
    session_maker = get_session_maker()
    logger.info("reconciler starting, interval=%ss", interval_seconds)
    while True:
        async with session_maker() as session:
            try:
                await reconcile_once(session)
            except Exception:
                logger.exception("reconcile_once failed")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run_forever(float(os.environ.get("PORTAGE_RECONCILE_INTERVAL_SECONDS", "5"))))
