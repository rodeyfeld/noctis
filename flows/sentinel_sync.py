"""
Sentinel Sync Flow

Daily pull of new Sentinel scenes from the Copernicus STAC API into the
sentinel archive database (ephemeris).

This flow:
1. Searches each Sentinel collection for scenes acquired in a rolling window
   (the `lookback_days` days before the target date)
2. Pages through results with the STAC `fields` extension to keep responses small
3. Inserts scenes into imagery_finder_archiveitem, skipping any external_id that
   already exists for the Copernicus provider

The window is derived from the target date, not "now", and inserts skip existing
scenes, so re-running a day (or overlapping windows) never creates duplicates.
The lookback exists because Copernicus publishes some products a day or more
after acquisition (Sentinel-3 NTC especially).
"""

import asyncio
import json
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import httpx
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from utils import get_ephemeris_connector

# --- Configuration ---

STAC_SEARCH_URL = "https://stac.dataspace.copernicus.eu/v1/search"

# Matches oracle's provider name so both write the same ArchiveItem rows
PROVIDER_NAME = "Copernicus"

# Same collections oracle backfills.
# See: https://stac.dataspace.copernicus.eu/v1/collections
SENTINEL_COLLECTIONS = [
    # Sentinel-1 (SAR)
    "sentinel-1-grd",
    "sentinel-1-slc",
    # Sentinel-2 (Optical)
    "sentinel-2-l1c",
    "sentinel-2-l2a",
    # Sentinel-3 OLCI
    "sentinel-3-olci-1-efr-ntc",
    "sentinel-3-olci-1-err-ntc",
    "sentinel-3-olci-2-lfr-ntc",
    "sentinel-3-olci-2-lrr-ntc",
    "sentinel-3-olci-2-wfr-ntc",
    "sentinel-3-olci-2-wrr-ntc",
    # Sentinel-3 SLSTR
    "sentinel-3-sl-1-rbt-ntc",
    "sentinel-3-sl-2-lst-ntc",
    "sentinel-3-sl-2-wst-ntc",
    "sentinel-3-sl-2-frp-ntc",
    # Sentinel-3 SYNERGY
    "sentinel-3-syn-2-syn-ntc",
    "sentinel-3-syn-2-aod-ntc",
    "sentinel-3-syn-2-vg1-ntc",
    "sentinel-3-syn-2-v10-ntc",
    # Sentinel-3 Altimetry
    "sentinel-3-sr-2-lan-ntc",
    "sentinel-3-sr-2-wat-ntc",
    # Sentinel-6
    "sentinel-6-p4-2-ntc",
]

# Only request what we store; Sentinel-2 items carry ~60 assets otherwise
STAC_FIELDS = ",".join(
    [
        "id",
        "geometry",
        "collection",
        "properties.datetime",
        "properties.start_datetime",
        "properties.end_datetime",
        "properties.platform",
        "properties.instruments",
        "properties.constellation",
        "properties.product:type",
        "properties.eo:cloud_cover",
        "assets.thumbnail",
    ]
)

# CDSE caps heavy collections (Sentinel-2) at 200 per page
PAGE_LIMIT = 200

# CDSE starts returning 429s around 3 concurrent anonymous searches
MAX_CONCURRENT_COLLECTIONS = 2

HTTP_TIMEOUT_SECONDS = 120
PAGE_FETCH_ATTEMPTS = 5

DEFAULT_LOOKBACK_DAYS = 3

INSERT_ARCHIVE_ITEMS_SQL = """
    INSERT INTO imagery_finder_archiveitem
        (created, modified, external_id, provider, collection, sensor,
         thumbnail, start_date, end_date, metadata, geometry)
    SELECT
        NOW(),
        NOW(),
        s.external_id,
        $1::text,
        s.collection,
        s.sensor,
        s.thumbnail,
        s.start_date::timestamptz,
        s.end_date::timestamptz,
        s.metadata,
        ST_SetSRID(ST_GeomFromGeoJSON(s.geometry), 4326)
    FROM (
        SELECT DISTINCT ON (external_id) *
        FROM unnest(
            $2::text[], $3::text[], $4::text[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[]
        ) AS u(external_id, collection, sensor, thumbnail,
               start_date, end_date, metadata, geometry)
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM imagery_finder_archiveitem ai
        WHERE ai.external_id = s.external_id
          AND ai.provider = $1::text
    )
"""


# --- Helpers ---


def resolve_window(target_date: date | None, lookback_days: int) -> tuple[datetime, datetime]:
    """
    Build the acquisition window for a run.

    Args:
        target_date: Day the run is for; defaults to the scheduled start date (UTC)
        lookback_days: Number of whole days before target_date to search

    Returns:
        (start, end) as UTC datetimes, end exclusive at midnight of target_date
    """
    if target_date is None:
        scheduled = flow_run.scheduled_start_time or datetime.now(UTC)
        target_date = scheduled.astimezone(UTC).date()

    end = datetime.combine(target_date, time.min, tzinfo=UTC)
    start = end - timedelta(days=lookback_days)
    return start, end


def feature_to_row(feature: dict[str, Any], collection: str) -> tuple[str, ...] | None:
    """
    Map a STAC feature to an ArchiveItem row (all text, cast in SQL).

    Returns None for features without a geometry, which ArchiveItem requires.
    """
    if not feature.get("geometry"):
        return None

    props = feature.get("properties", {})
    instruments = props.get("instruments") or []
    acquired = props.get("datetime")

    metadata = {
        "platform": props.get("platform"),
        "constellation": props.get("constellation"),
        "product_type": props.get("product:type"),
        "cloud_cover": props.get("eo:cloud_cover"),
    }

    return (
        feature["id"],
        feature.get("collection") or collection,
        instruments[0] if instruments else "",
        feature.get("assets", {}).get("thumbnail", {}).get("href", ""),
        props.get("start_datetime") or acquired,
        props.get("end_datetime") or acquired,
        json.dumps({k: v for k, v in metadata.items() if v is not None}),
        json.dumps(feature["geometry"]),
    )


async def fetch_page(client: httpx.AsyncClient, url: str, params: dict | None) -> dict[str, Any]:
    """GET a STAC search page, retrying rate limits, server errors and network failures."""
    logger = get_run_logger()

    for attempt in range(1, PAGE_FETCH_ATTEMPTS + 1):
        try:
            resp = await client.get(url, params=params)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"STAC returned {resp.status_code}", request=resp.request, response=resp
                )
            resp.raise_for_status()
            return resp.json()
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            retryable = not isinstance(e, httpx.HTTPStatusError) or (
                e.response.status_code == 429 or e.response.status_code >= 500
            )
            if not retryable or attempt == PAGE_FETCH_ATTEMPTS:
                raise
            delay = 2**attempt * 5
            logger.warning(f"STAC request failed ({e}), retry {attempt} in {delay}s")
            await asyncio.sleep(delay)

    raise RuntimeError("unreachable")


# --- Tasks ---


@task(
    name="sync_sentinel_collection",
    retries=2,
    retry_delay_seconds=300,
    tags=["stac", "postgres"],
)
async def sync_sentinel_collection(
    collection: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    """
    Pull one collection's scenes for the window and insert the new ones.

    Each page is written in its own transaction, so a retry after a mid-run
    failure just skips what's already stored.

    Args:
        collection: STAC collection id (e.g. "sentinel-2-l2a")
        window_start: Inclusive acquisition start (UTC)
        window_end: Exclusive acquisition end (UTC)

    Returns:
        Counts of fetched, inserted and skipped (no geometry) features
    """
    logger = get_run_logger()

    # No sortby: CDSE's continuation token skips and repeats scenes that share a
    # datetime (whole Sentinel-2 datatakes do), losing over half of a day's tiles
    params = {
        "collections": collection,
        # Z suffix, not +00:00: CDSE echoes this unencoded into the next link,
        # where "+" would decode as a space
        "datetime": f"{window_start:%Y-%m-%dT%H:%M:%SZ}/{window_end:%Y-%m-%dT%H:%M:%SZ}",
        "limit": str(PAGE_LIMIT),
        "fields": STAC_FIELDS,
    }

    fetched = 0
    inserted = 0
    skipped = 0
    pages = 0

    async with (
        httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client,
        get_ephemeris_connector() as ephemeris,
    ):
        url: str | None = STAC_SEARCH_URL
        while url:
            page = await fetch_page(client, url, params)
            pages += 1
            features = page.get("features", [])
            fetched += len(features)

            rows = [r for r in (feature_to_row(f, collection) for f in features) if r]
            skipped += len(features) - len(rows)

            if rows:
                columns = list(zip(*rows))
                async with ephemeris.transaction() as conn:
                    # Serialize writers per collection so overlapping runs can't
                    # both pass the NOT EXISTS check for the same scene
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        f"noctis:sentinel-sync:{collection}",
                    )
                    status = await conn.execute(
                        INSERT_ARCHIVE_ITEMS_SQL,
                        PROVIDER_NAME,
                        *[list(c) for c in columns],
                    )
                inserted += int(status.split()[-1])

            # The next link carries the original query plus a continuation token
            next_link = next(
                (link for link in page.get("links", []) if link.get("rel") == "next"), None
            )
            url = next_link["href"] if next_link else None
            params = None

    summary = {
        "collection": collection,
        "pages": pages,
        "fetched": fetched,
        "inserted": inserted,
        "skipped": skipped,
    }
    logger.info(
        f"{collection}: {pages} pages, {fetched} fetched, "
        f"+{inserted} inserted, {skipped} skipped (no geometry)"
    )
    return summary


# --- Main Flow ---


@flow(
    name="sentinel-sync",
    description="Daily idempotent pull of Sentinel scenes from Copernicus STAC into ephemeris",
)
async def sentinel_sync(
    target_date: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    collections: list[str] | None = None,
) -> dict[str, Any]:
    """
    Main flow: sync recent Sentinel scenes into imagery_finder_archiveitem.

    Args:
        target_date: Day the run is for (window ends at its midnight UTC);
            defaults to the scheduled start date. Set it to backfill a past day.
        lookback_days: How many days of acquisitions before target_date to search
        collections: STAC collection ids to sync; defaults to all Sentinel collections

    Returns:
        Summary dict with the window and per-collection counts
    """
    logger = get_run_logger()

    window_start, window_end = resolve_window(target_date, lookback_days)
    collections = collections or SENTINEL_COLLECTIONS

    logger.info(
        f"Syncing {len(collections)} collections, "
        f"acquired {window_start.isoformat()} to {window_end.isoformat()}"
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COLLECTIONS)

    async def sync_one(collection: str) -> dict[str, Any]:
        async with semaphore:
            return await sync_sentinel_collection(collection, window_start, window_end)

    # Let every collection finish before failing the run on any error
    results = await asyncio.gather(
        *(sync_one(c) for c in collections),
        return_exceptions=True,
    )

    synced = [r for r in results if not isinstance(r, BaseException)]
    failed = {c: str(r) for c, r in zip(collections, results) if isinstance(r, BaseException)}

    summary = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "inserted": sum(r["inserted"] for r in synced),
        "fetched": sum(r["fetched"] for r in synced),
        "collections": synced,
        "failed": failed,
    }

    logger.info(
        f"Sentinel sync complete: +{summary['inserted']} inserted "
        f"of {summary['fetched']} fetched, {len(failed)} collections failed"
    )

    if failed:
        raise RuntimeError(f"Sentinel sync failed for: {', '.join(failed)}")

    return summary


# --- Deployment Entry Point ---

if __name__ == "__main__":
    asyncio.run(sentinel_sync(collections=["sentinel-1-grd"], lookback_days=1))
