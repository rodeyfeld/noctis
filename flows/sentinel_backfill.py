"""
Sentinel Backfill Flow

Fills the sentinel archive (ephemeris) with past Sentinel scenes, newest first.

This flow:
1. Walks backwards from `before` in `window_days` windows until `until`
2. Syncs every collection for each window with sentinel-sync's task, so inserts
   skip scenes already stored and a rerun only fills the gaps
3. Stops before ephemeris grows past `max_database_gb`, so the archive can't fill
   its volume; the newest scenes are the ones kept

A window that fails for a collection is logged and skipped rather than ending the
backfill; the run fails at the end listing them, and rerunning fills them in.
"""

import asyncio
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from prefect import flow, get_run_logger, task

from flows.sentinel_sync import (
    MAX_CONCURRENT_COLLECTIONS,
    SENTINEL_COLLECTIONS,
    sync_sentinel_collection,
)
from utils import get_ephemeris_connector

# --- Configuration ---

# Sentinel-1A, the first of the collections, started acquiring in April 2014
EARLIEST_ACQUISITION = date(2014, 4, 1)

# A week of every collection is a few hundred thousand scenes, a few hundred
# STAC pages per collection
DEFAULT_WINDOW_DAYS = 7

# ephemeris's volume is 48Gi; leave room for WAL, vacuum and index builds
DEFAULT_MAX_DATABASE_GB = 40.0


# --- Tasks ---


@task(name="archive_database_gb", tags=["postgres"])
async def archive_database_gb() -> float:
    """Size of the ephemeris database on disk, in GB."""
    async with get_ephemeris_connector() as ephemeris:
        size = await ephemeris.fetchval("SELECT pg_database_size(current_database())")
    return size / 1024**3


# --- Main Flow ---


@flow(
    name="sentinel-backfill",
    description="Backfill past Sentinel scenes into ephemeris, newest first, up to a size cap",
)
async def sentinel_backfill(
    before: date | None = None,
    until: date = EARLIEST_ACQUISITION,
    window_days: int = DEFAULT_WINDOW_DAYS,
    collections: list[str] | None = None,
    max_database_gb: float = DEFAULT_MAX_DATABASE_GB,
) -> dict[str, Any]:
    """
    Main flow: backfill Sentinel scenes acquired between `until` and `before`.

    Args:
        before: Backfill acquisitions before this day (exclusive, midnight UTC);
            defaults to today. Set it to resume where a stopped run left off.
        until: Earliest day to backfill (inclusive)
        window_days: Days of acquisitions per STAC search
        collections: STAC collection ids; defaults to all Sentinel collections
        max_database_gb: Stop once ephemeris is at least this big

    Returns:
        Summary dict with the range covered, counts, and any failed windows
    """
    logger = get_run_logger()

    collections = collections or SENTINEL_COLLECTIONS
    floor = datetime.combine(until, time.min, tzinfo=UTC)
    window_end = datetime.combine(before or datetime.now(UTC).date(), time.min, tzinfo=UTC)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COLLECTIONS)

    async def sync_one(collection: str, start: datetime, end: datetime) -> dict[str, Any]:
        async with semaphore:
            return await sync_sentinel_collection(collection, start, end)

    inserted = 0
    fetched = 0
    failed: list[str] = []
    stopped_for_size = False

    logger.info(
        f"Backfilling {len(collections)} collections from {window_end.date()} back to "
        f"{floor.date()} in {window_days}-day windows, up to {max_database_gb} GB"
    )

    while window_end > floor:
        size_gb = await archive_database_gb()
        if size_gb >= max_database_gb:
            stopped_for_size = True
            logger.warning(
                f"ephemeris is {size_gb:.1f} GB (cap {max_database_gb} GB); stopping. "
                f"Acquisitions before {window_end.date()} weren't backfilled."
            )
            break

        window_start = max(window_end - timedelta(days=window_days), floor)
        results = await asyncio.gather(
            *(sync_one(c, window_start, window_end) for c in collections),
            return_exceptions=True,
        )

        window_inserted = 0
        for collection, result in zip(collections, results):
            if isinstance(result, BaseException):
                failed.append(f"{collection} {window_start.date()}..{window_end.date()}")
                logger.error(f"{collection} {window_start.date()}..{window_end.date()}: {result}")
            else:
                window_inserted += result["inserted"]
                fetched += result["fetched"]
        inserted += window_inserted

        logger.info(
            f"{window_start.date()}..{window_end.date()}: +{window_inserted} inserted, "
            f"ephemeris at {size_gb:.1f} GB before this window"
        )
        window_end = window_start

    summary = {
        "reached": window_end.date().isoformat(),
        "stopped_for_size": stopped_for_size,
        "inserted": inserted,
        "fetched": fetched,
        "failed": failed,
    }
    logger.info(f"Sentinel backfill done: {summary}")

    if failed:
        raise RuntimeError(f"{len(failed)} collection windows failed; rerun to fill them: {failed}")

    return summary
