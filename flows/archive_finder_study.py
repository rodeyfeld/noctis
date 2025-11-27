"""
Archive Finder Study Flow (Imagery Finder)

Migrated from Airflow DAG: imagery_finder_dag.py

This flow processes ImageryFinder requests by:
1. Finding archive items that match the imagery finder's spatial and temporal criteria
2. Creating ArchiveLookupItem records linking matched items to the study
3. Notifying the Augur API to continue the divination process

Uses asyncpg for high-performance PostgreSQL operations with PostGIS.
"""

import os
from typing import Any

import httpx
from prefect import flow, task, get_run_logger

from utils import get_atlas_connector


# --- Configuration ---

AUGUR_API_URL = os.environ.get("NOCTIS_AUGUR_API_URL", "http://localhost:8000")
AUGUR_DIVINE_ENDPOINT = "/api/augury/divine"


# --- Tasks ---

@task(
    name="create_imagery_finder_items",
    retries=2,
    retry_delay_seconds=30,
    tags=["postgres", "spatial"],
)
async def create_imagery_finder_items(
    imagery_finder_pk: int,
    dream_pk: int,
) -> int:
    """
    Find and create archive lookup items matching the imagery finder criteria.
    
    This performs a spatial-temporal query using PostGIS to find archive items
    that intersect with the imagery finder's location and fall within its date range.
    
    Uses a temporary table approach for efficient batch processing.
    
    Args:
        imagery_finder_pk: Primary key of the ImageryFinder
        dream_pk: Primary key of the Dream (study execution)
    
    Returns:
        Number of lookup items created
    """
    logger = get_run_logger()
    
    logger.info(
        f"Processing imagery finder {imagery_finder_pk} for dream {dream_pk}"
    )
    
    async with get_atlas_connector() as atlas:
        # Create temporary table with matching items using spatial intersection
        temp_table_name = f"imagery_seeker_{imagery_finder_pk}_{dream_pk}"
        
        # First, check if temp table exists and drop it
        await atlas.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        
        # Create temp table with matching archive items
        create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table_name} AS
            SELECT 
                archive_items.id as archive_item_id,
                imagery_finders.id as imagery_finder_id
            FROM imagery_finder_archiveitem archive_items
            JOIN imagery_finder_imageryfinder imagery_finders
                ON archive_items.start_date >= imagery_finders.start_date
                AND archive_items.end_date <= imagery_finders.end_date
            JOIN core_location locations
                ON imagery_finders.location_id = locations.id
            WHERE ST_Intersects(
                archive_items.geometry,
                locations.geometry
            )
            AND imagery_finders.id = $1
        """
        
        await atlas.execute(create_temp_sql, imagery_finder_pk)
        
        # Get count of matched items
        count = await atlas.fetchval(
            f"SELECT COUNT(*) FROM {temp_table_name}"
        )
        logger.info(f"Found {count} matching archive items")
        
        if count == 0:
            return 0
        
        # Get the study_id from the dream
        study_id = await atlas.fetchval(
            "SELECT study_id FROM augury_dream WHERE id = $1",
            dream_pk,
        )
        
        if study_id is None:
            logger.error(f"No study found for dream {dream_pk}")
            raise ValueError(f"Dream {dream_pk} has no associated study")
        
        # Insert lookup items - link archive items to the study
        insert_sql = f"""
            INSERT INTO imagery_finder_archivelookupitem 
                (created, modified, imagery_finder_id, archive_item_id, study_id)
            SELECT
                NOW() as created,
                NOW() as modified,
                imagery_finder_id,
                archive_item_id,
                $1 as study_id
            FROM {temp_table_name}
            ON CONFLICT (imagery_finder_id, archive_item_id, study_id) 
            DO UPDATE SET modified = NOW()
        """
        
        await atlas.execute(insert_sql, study_id)
        
        # Cleanup temp table
        await atlas.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        
        logger.info(f"Created {count} archive lookup items for study {study_id}")
        return count


@task(
    name="notify_augur",
    retries=3,
    retry_delay_seconds=10,
    tags=["http", "augur"],
)
async def notify_augur(dream_pk: int) -> dict[str, Any]:
    """
    Notify the Augur API that the imagery finder processing is complete.
    
    This triggers the divination process to continue with the matched results.
    
    Args:
        dream_pk: Primary key of the Dream
    
    Returns:
        Response from the Augur API
    """
    logger = get_run_logger()
    
    url = f"{AUGUR_API_URL}{AUGUR_DIVINE_ENDPOINT}"
    payload = {"dream_id": dream_pk}
    
    logger.info(f"Notifying Augur at {url} for dream {dream_pk}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Augur notification successful: {result}")
        return result


@task(
    name="get_imagery_finder_info",
    tags=["postgres"],
)
async def get_imagery_finder_info(imagery_finder_pk: int) -> dict[str, Any]:
    """
    Fetch imagery finder details for logging and validation.
    
    Args:
        imagery_finder_pk: Primary key of the ImageryFinder
    
    Returns:
        Dict with imagery finder details
    """
    logger = get_run_logger()
    
    async with get_atlas_connector() as atlas:
        row = await atlas.fetchrow(
            """
            SELECT 
                if.id,
                if.name,
                if.start_date,
                if.end_date,
                if.is_active,
                l.name as location_name,
                ST_AsText(l.geometry) as geometry_wkt
            FROM imagery_finder_imageryfinder if
            LEFT JOIN core_location l ON if.location_id = l.id
            WHERE if.id = $1
            """,
            imagery_finder_pk,
        )
        
        if row is None:
            raise ValueError(f"ImageryFinder {imagery_finder_pk} not found")
        
        info = dict(row)
        logger.info(f"Processing ImageryFinder: {info['name']} ({info['id']})")
        return info


# --- Main Flow ---

@flow(
    name="imagery-finder-study",
    description="Process ImageryFinder requests and create archive lookup items",
    retries=1,
    retry_delay_seconds=60,
)
async def imagery_finder_study(
    imagery_finder_pk: int,
    dream_pk: int,
    notify: bool = True,
) -> dict[str, Any]:
    """
    Main flow: Process an ImageryFinder study.
    
    This flow finds archive items matching the imagery finder's criteria
    (spatial intersection and temporal overlap), creates lookup items,
    and notifies the Augur API to continue processing.
    
    Args:
        imagery_finder_pk: Primary key of the ImageryFinder to process
        dream_pk: Primary key of the Dream (study execution context)
        notify: Whether to notify Augur API after processing (default: True)
    
    Returns:
        Summary dict with processing results
    """
    logger = get_run_logger()
    
    logger.info(
        f"Starting imagery finder study: "
        f"imagery_finder={imagery_finder_pk}, dream={dream_pk}"
    )
    
    # Get imagery finder info for validation and logging
    imagery_finder_info = await get_imagery_finder_info(imagery_finder_pk)
    
    if not imagery_finder_info.get("is_active", False):
        logger.warning(f"ImageryFinder {imagery_finder_pk} is not active")
    
    # Create archive lookup items
    items_created = await create_imagery_finder_items(
        imagery_finder_pk=imagery_finder_pk,
        dream_pk=dream_pk,
    )
    
    # Notify Augur to continue processing
    augur_response = None
    if notify and items_created > 0:
        augur_response = await notify_augur(dream_pk)
    elif notify:
        logger.info("Skipping Augur notification - no items created")
    
    summary = {
        "imagery_finder_pk": imagery_finder_pk,
        "imagery_finder_name": imagery_finder_info.get("name"),
        "dream_pk": dream_pk,
        "items_created": items_created,
        "augur_notified": augur_response is not None,
        "augur_response": augur_response,
    }
    
    logger.info(f"Imagery finder study complete: {summary}")
    return summary


# --- Deployment Entry Point ---

if __name__ == "__main__":
    import asyncio
    
    # Example execution
    asyncio.run(
        imagery_finder_study(
            imagery_finder_pk=2,
            dream_pk=1,
            notify=False,  # Disable notification for local testing
        )
    )

