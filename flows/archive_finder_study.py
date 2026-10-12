"""
Archive Finder Study Flow (Imagery Finder)

This flow processes ImageryFinder requests by:
1. Reading the imagery finder's location and date range from Atlas
2. Finding archive items in the sentinel archive (ephemeris) that intersect the
   location within the date range
3. Writing ArchiveLookupItem records linking the matches to the study, and the
   normalized ArchiveLookupResult records, to Atlas in one transaction
4. Updating the Dream status to COMPLETE

The archive lives in its own database, so matching and writing are separate steps:
matches are read out of ephemeris and written into Atlas, never joined in SQL.
Writing replaces any results the study already has, so a retried run doesn't
duplicate them.

Uses asyncpg for high-performance PostgreSQL operations with PostGIS.
"""

from typing import Any

from prefect import flow, task, get_run_logger

from utils import get_atlas_connector, get_ephemeris_connector


# --- Configuration ---

# Dream status constants (matching Django model)
DREAM_STATUS_PROCESSING = "PROCESSING"
DREAM_STATUS_COMPLETE = "COMPLETE"
DREAM_STATUS_FAILED = "FAILED"

# A finder over a large area and many years can match a huge slice of the archive.
# Keep the most recent matches and say so, rather than loading Atlas with them all.
MAX_MATCHES = 50_000

# Matching runs against ephemeris; stop a pathological query instead of letting
# it hold the archive's single CPU
MATCH_STATEMENT_TIMEOUT = "5min"

MATCH_ARCHIVE_ITEMS_SQL = """
    SELECT
        id,
        external_id,
        collection,
        provider,
        start_date,
        end_date,
        sensor,
        ST_AsEWKB(geometry) AS geometry,
        thumbnail,
        metadata
    FROM imagery_finder_archiveitem
    WHERE start_date >= $1
      AND end_date <= $2
      AND ST_Intersects(geometry, ST_GeomFromEWKB($3))
    ORDER BY start_date DESC
    LIMIT $4
"""

INSERT_LOOKUP_ITEMS_SQL = """
    INSERT INTO imagery_finder_archivelookupitem
        (created, modified, imagery_finder_id, archive_item_id, study_id)
    SELECT NOW(), NOW(), $1, archive_item_id, $2
    FROM unnest($3::bigint[]) AS u(archive_item_id)
"""

INSERT_LOOKUP_RESULTS_SQL = """
    INSERT INTO imagery_finder_archivelookupresult
        (created, modified, study_id, external_id, collection_id,
         start_date, end_date, sensor_id, geometry, thumbnail, metadata)
    SELECT
        NOW(),
        NOW(),
        $1,
        u.external_id,
        u.collection_id,
        u.start_date,
        u.end_date,
        u.sensor_id,
        ST_GeomFromEWKB(u.geometry),
        u.thumbnail,
        u.metadata
    FROM unnest(
        $2::text[], $3::bigint[], $4::timestamptz[], $5::timestamptz[],
        $6::bigint[], $7::bytea[], $8::text[], $9::text[]
    ) AS u(external_id, collection_id, start_date, end_date,
           sensor_id, geometry, thumbnail, metadata)
"""


def sensor_technique(sensor_name: str) -> str:
    """Technique for a new Sensor row, from its name (case-insensitive)."""
    sensor_upper = sensor_name.upper()
    if sensor_upper in ("EO", "MSI"):
        return "EO"
    if sensor_upper == "SAR":
        return "SAR"
    return "UNKNOWN"


# --- Tasks ---

@task(
    name="get_imagery_finder_info",
    tags=["postgres"],
)
async def get_imagery_finder_info(imagery_finder_pk: int) -> dict[str, Any]:
    """
    Fetch the imagery finder's date range and location geometry from Atlas.

    Args:
        imagery_finder_pk: Primary key of the ImageryFinder

    Returns:
        Dict with imagery finder details; geometry is EWKB (None without a location)
    """
    logger = get_run_logger()

    async with get_atlas_connector() as atlas:
        row = await atlas.fetchrow(
            """
            SELECT
                f.id,
                f.name,
                f.start_date,
                f.end_date,
                f.is_active,
                l.name AS location_name,
                ST_AsEWKB(l.geometry) AS geometry
            FROM imagery_finder_imageryfinder f
            LEFT JOIN core_location l ON f.location_id = l.id
            WHERE f.id = $1
            """,
            imagery_finder_pk,
        )

        if row is None:
            raise ValueError(f"ImageryFinder {imagery_finder_pk} not found")

        info = dict(row)
        logger.info(f"Processing ImageryFinder: {info['name']} ({info['id']})")
        return info


@task(
    name="match_archive_items",
    retries=2,
    retry_delay_seconds=30,
    tags=["postgres", "spatial"],
)
async def match_archive_items(imagery_finder: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Find archive items intersecting the imagery finder's location within its dates.

    Reads from ephemeris only. Returns at most MAX_MATCHES items, most recent first.

    Args:
        imagery_finder: Output of get_imagery_finder_info

    Returns:
        Matching archive items, with geometry as EWKB
    """
    logger = get_run_logger()

    start_date = imagery_finder["start_date"]
    end_date = imagery_finder["end_date"]
    geometry = imagery_finder["geometry"]
    if start_date is None or end_date is None or geometry is None:
        logger.warning(
            f"ImageryFinder {imagery_finder['id']} has no "
            f"{'location' if geometry is None else 'date range'}; nothing to match"
        )
        return []

    async with get_ephemeris_connector() as ephemeris:
        async with ephemeris.transaction() as conn:
            await conn.execute(f"SET LOCAL statement_timeout = '{MATCH_STATEMENT_TIMEOUT}'")
            rows = await conn.fetch(
                MATCH_ARCHIVE_ITEMS_SQL, start_date, end_date, geometry, MAX_MATCHES
            )

    if len(rows) == MAX_MATCHES:
        logger.warning(
            f"Hit the {MAX_MATCHES} match limit; keeping the most recent. "
            f"Narrow the finder's location or dates to see older scenes."
        )
    logger.info(f"Found {len(rows)} matching archive items")
    return [dict(r) for r in rows]


@task(
    name="write_study_results",
    retries=2,
    retry_delay_seconds=30,
    tags=["postgres", "transform"],
)
async def write_study_results(
    imagery_finder_pk: int,
    dream_pk: int,
    items: list[dict[str, Any]],
) -> int:
    """
    Write the matches to Atlas as ArchiveLookupItems and ArchiveLookupResults.

    This task performs the "divination" step in one transaction:
    1. Clears any lookup items/results the study already has (from a failed run)
    2. Gets or creates the Provider, Collection and Sensor rows the items name
    3. Inserts a lookup item and a normalized result per archive item

    Args:
        imagery_finder_pk: Primary key of the ImageryFinder
        dream_pk: Primary key of the Dream
        items: Output of match_archive_items

    Returns:
        Number of results created
    """
    logger = get_run_logger()

    async with get_atlas_connector() as atlas:
        async with atlas.transaction() as conn:
            study_id = await conn.fetchval(
                "SELECT study_id FROM augury_dream WHERE id = $1",
                dream_pk,
            )
            if study_id is None:
                logger.error(f"No study found for dream {dream_pk}")
                raise ValueError(f"Dream {dream_pk} has no associated study")

            await conn.execute(
                "DELETE FROM imagery_finder_archivelookupresult WHERE study_id = $1", study_id
            )
            await conn.execute(
                "DELETE FROM imagery_finder_archivelookupitem WHERE study_id = $1", study_id
            )

            if not items:
                logger.info(f"No archive items to write for study {study_id}")
                return 0

            # Get or create each distinct Provider, Collection (scoped to provider)
            # and Sensor once
            provider_ids: dict[str, int] = {}
            collection_ids: dict[tuple[str, str], int] = {}
            sensor_ids: dict[str, int] = {}

            for item in items:
                provider_name = item["provider"] or "Unknown"
                collection_name = item["collection"] or "Unknown"
                sensor_name = item["sensor"] or "Unknown"

                if provider_name not in provider_ids:
                    provider_id = await conn.fetchval(
                        "SELECT id FROM provider_provider WHERE name = $1",
                        provider_name,
                    )
                    if provider_id is None:
                        provider_id = await conn.fetchval(
                            """
                            INSERT INTO provider_provider (created, modified, name, is_active)
                            VALUES (NOW(), NOW(), $1, true)
                            RETURNING id
                            """,
                            provider_name,
                        )
                    provider_ids[provider_name] = provider_id

                collection_key = (provider_name, collection_name)
                if collection_key not in collection_ids:
                    provider_id = provider_ids[provider_name]
                    collection_id = await conn.fetchval(
                        "SELECT id FROM provider_collection WHERE name = $1 AND provider_id = $2",
                        collection_name,
                        provider_id,
                    )
                    if collection_id is None:
                        collection_id = await conn.fetchval(
                            """
                            INSERT INTO provider_collection (created, modified, name, provider_id)
                            VALUES (NOW(), NOW(), $1, $2)
                            RETURNING id
                            """,
                            collection_name,
                            provider_id,
                        )
                    collection_ids[collection_key] = collection_id

                if sensor_name not in sensor_ids:
                    sensor_id = await conn.fetchval(
                        "SELECT id FROM core_sensor WHERE name = $1",
                        sensor_name,
                    )
                    if sensor_id is None:
                        sensor_id = await conn.fetchval(
                            """
                            INSERT INTO core_sensor (created, modified, name, technique)
                            VALUES (NOW(), NOW(), $1, $2)
                            RETURNING id
                            """,
                            sensor_name,
                            sensor_technique(sensor_name),
                        )
                    sensor_ids[sensor_name] = sensor_id

            # archive_item_id points into ephemeris; the column has no FK constraint
            await conn.execute(
                INSERT_LOOKUP_ITEMS_SQL,
                imagery_finder_pk,
                study_id,
                [item["id"] for item in items],
            )

            status = await conn.execute(
                INSERT_LOOKUP_RESULTS_SQL,
                study_id,
                [item["external_id"] or "" for item in items],
                [
                    collection_ids[(item["provider"] or "Unknown", item["collection"] or "Unknown")]
                    for item in items
                ],
                [item["start_date"] for item in items],
                [item["end_date"] for item in items],
                [sensor_ids[item["sensor"] or "Unknown"] for item in items],
                [item["geometry"] for item in items],
                [item["thumbnail"] or "" for item in items],
                [item["metadata"] or "" for item in items],
            )
            results_created = int(status.split()[-1])

    logger.info(f"Created {results_created} archive lookup results for study {study_id}")
    return results_created


@task(
    name="update_dream_status",
    tags=["postgres"],
)
async def update_dream_status(dream_pk: int, status: str) -> None:
    """
    Update the Dream status in the database.

    Args:
        dream_pk: Primary key of the Dream
        status: New status value (e.g., "COMPLETE", "FAILED")
    """
    logger = get_run_logger()

    logger.info(f"Updating dream {dream_pk} status to {status}")

    async with get_atlas_connector() as atlas:
        await atlas.execute(
            "UPDATE augury_dream SET status = $1, modified = NOW() WHERE id = $2",
            status,
            dream_pk,
        )

    logger.info(f"Dream {dream_pk} status updated to {status}")


# --- Main Flow ---

@flow(
    name="imagery-finder-study",
    description="Process ImageryFinder requests, match them against the archive, and write results",
    retries=1,
    retry_delay_seconds=60,
)
async def imagery_finder_study(
    imagery_finder_pk: int,
    dream_pk: int,
) -> dict[str, Any]:
    """
    Main flow: Process an ImageryFinder study.

    This flow:
    1. Reads the ImageryFinder's location and dates from Atlas
    2. Matches archive items in ephemeris
    3. Writes ArchiveLookupItem and ArchiveLookupResult records to Atlas
    4. Updates Dream status to COMPLETE

    Args:
        imagery_finder_pk: Primary key of the ImageryFinder to process
        dream_pk: Primary key of the Dream (study execution context)

    Returns:
        Summary dict with processing results
    """
    logger = get_run_logger()

    logger.info(
        f"Starting imagery finder study: "
        f"imagery_finder={imagery_finder_pk}, dream={dream_pk}"
    )

    try:
        # Step 1: Get imagery finder info for matching and logging
        imagery_finder_info = await get_imagery_finder_info(imagery_finder_pk)

        if not imagery_finder_info.get("is_active", False):
            logger.warning(f"ImageryFinder {imagery_finder_pk} is not active")

        # Step 2: Match archive items (spatial query in ephemeris)
        items = await match_archive_items(imagery_finder_info)

        # Step 3: Write lookup items and normalized results to Atlas
        results_created = await write_study_results(imagery_finder_pk, dream_pk, items)

        # Step 4: Mark dream as complete
        await update_dream_status(dream_pk, DREAM_STATUS_COMPLETE)

        summary = {
            "imagery_finder_pk": imagery_finder_pk,
            "imagery_finder_name": imagery_finder_info.get("name"),
            "dream_pk": dream_pk,
            "items_created": len(items),
            "results_created": results_created,
            "status": DREAM_STATUS_COMPLETE,
        }

        logger.info(f"Imagery finder study complete: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Flow failed: {e}")
        # Mark dream as failed
        await update_dream_status(dream_pk, DREAM_STATUS_FAILED)
        raise


# --- Deployment Entry Point ---

if __name__ == "__main__":
    import asyncio

    # Example execution
    asyncio.run(
        imagery_finder_study(
            imagery_finder_pk=2,
            dream_pk=1,
        )
    )
