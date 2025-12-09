"""
Archive Finder Study Flow (Imagery Finder)

This flow processes ImageryFinder requests by:
1. Finding archive items that match the imagery finder's spatial and temporal criteria
2. Creating ArchiveLookupItem records linking matched items to the study
3. Transforming raw lookup items into normalized ArchiveLookupResult records
4. Updating the Dream status to COMPLETE

Uses asyncpg for high-performance PostgreSQL operations with PostGIS.
"""

import os
from typing import Any

from prefect import flow, task, get_run_logger

from utils import get_atlas_connector


# --- Configuration ---

# Dream status constants (matching Django model)
DREAM_STATUS_PROCESSING = "PROCESSING"
DREAM_STATUS_COMPLETE = "COMPLETE"
DREAM_STATUS_FAILED = "FAILED"


# --- Tasks ---

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
    name="transform_lookup_results",
    retries=2,
    retry_delay_seconds=30,
    tags=["postgres", "transform"],
)
async def transform_lookup_results(dream_pk: int) -> int:
    """
    Transform raw ArchiveLookupItems into normalized ArchiveLookupResult records.
    
    This task performs the "divination" step:
    1. Gets all ArchiveLookupItems for the study
    2. For each item, gets or creates Provider, Collection, Sensor records
    3. Creates ArchiveLookupResult records with proper FK relationships
    
    Args:
        dream_pk: Primary key of the Dream
    
    Returns:
        Number of results created
    """
    logger = get_run_logger()
    
    logger.info(f"Transforming lookup results for dream {dream_pk}")
    
    async with get_atlas_connector() as atlas:
        # Get the study_id from the dream
        study_id = await atlas.fetchval(
            "SELECT study_id FROM augury_dream WHERE id = $1",
            dream_pk,
        )
        
        if study_id is None:
            logger.error(f"No study found for dream {dream_pk}")
            raise ValueError(f"Dream {dream_pk} has no associated study")
        
        # Get all lookup items with their archive item details
        lookup_items = await atlas.fetch(
            """
            SELECT 
                ali.id as lookup_item_id,
                ai.external_id,
                ai.collection,
                ai.provider,
                ai.start_date,
                ai.end_date,
                ai.sensor,
                ai.geometry,
                ai.thumbnail,
                ai.metadata
            FROM imagery_finder_archivelookupitem ali
            JOIN imagery_finder_archiveitem ai ON ali.archive_item_id = ai.id
            WHERE ali.study_id = $1
            """,
            study_id,
        )
        
        if not lookup_items:
            logger.info(f"No lookup items found for study {study_id}")
            return 0
        
        logger.info(f"Transforming {len(lookup_items)} lookup items")
        
        # Cache for providers, collections, and sensors to minimize queries
        provider_cache: dict[str, int] = {}
        collection_cache: dict[tuple[str, int], int] = {}
        sensor_cache: dict[str, int] = {}
        
        results_created = 0
        
        for item in lookup_items:
            provider_name = item["provider"] or "Unknown"
            collection_name = item["collection"] or "Unknown"
            sensor_name = item["sensor"] or "Unknown"
            
            # Get or create Provider
            if provider_name not in provider_cache:
                provider_id = await atlas.fetchval(
                    "SELECT id FROM provider_provider WHERE name = $1",
                    provider_name,
                )
                if provider_id is None:
                    provider_id = await atlas.fetchval(
                        """
                        INSERT INTO provider_provider (created, modified, name, is_active)
                        VALUES (NOW(), NOW(), $1, true)
                        RETURNING id
                        """,
                        provider_name,
                    )
                provider_cache[provider_name] = provider_id
            
            provider_id = provider_cache[provider_name]
            
            # Get or create Collection (scoped to provider)
            collection_key = (collection_name, provider_id)
            if collection_key not in collection_cache:
                collection_id = await atlas.fetchval(
                    "SELECT id FROM provider_collection WHERE name = $1 AND provider_id = $2",
                    collection_name,
                    provider_id,
                )
                if collection_id is None:
                    collection_id = await atlas.fetchval(
                        """
                        INSERT INTO provider_collection (created, modified, name, provider_id)
                        VALUES (NOW(), NOW(), $1, $2)
                        RETURNING id
                        """,
                        collection_name,
                        provider_id,
                    )
                collection_cache[collection_key] = collection_id
            
            collection_id = collection_cache[collection_key]
            
            # Get or create Sensor
            if sensor_name not in sensor_cache:
                sensor_id = await atlas.fetchval(
                    "SELECT id FROM core_sensor WHERE name = $1",
                    sensor_name,
                )
                if sensor_id is None:
                    # Determine technique based on sensor name
                    technique = "UNKNOWN"
                    if sensor_name in ("EO", "MSI"):
                        technique = "EO"
                    elif sensor_name == "SAR":
                        technique = "SAR"
                    
                    sensor_id = await atlas.fetchval(
                        """
                        INSERT INTO core_sensor (created, modified, name, technique)
                        VALUES (NOW(), NOW(), $1, $2)
                        RETURNING id
                        """,
                        sensor_name,
                        technique,
                    )
                sensor_cache[sensor_name] = sensor_id
            
            sensor_id = sensor_cache[sensor_name]
            
            # Create ArchiveLookupResult
            await atlas.execute(
                """
                INSERT INTO imagery_finder_archivelookupresult 
                    (created, modified, study_id, external_id, collection_id, 
                     start_date, end_date, sensor_id, geometry, thumbnail, metadata)
                VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT DO NOTHING
                """,
                study_id,
                item["external_id"] or "",
                collection_id,
                item["start_date"],
                item["end_date"],
                sensor_id,
                item["geometry"],
                item["thumbnail"] or "",
                item["metadata"] or "",
            )
            results_created += 1
        
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
    description="Process ImageryFinder requests, create archive lookup items, and transform results",
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
    1. Validates the ImageryFinder exists and logs its details
    2. Finds archive items matching spatial/temporal criteria
    3. Creates ArchiveLookupItem records
    4. Transforms items into normalized ArchiveLookupResult records
    5. Updates Dream status to COMPLETE
    
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
        # Step 1: Get imagery finder info for validation and logging
        imagery_finder_info = await get_imagery_finder_info(imagery_finder_pk)
        
        if not imagery_finder_info.get("is_active", False):
            logger.warning(f"ImageryFinder {imagery_finder_pk} is not active")
        
        # Step 2: Create archive lookup items (spatial query)
        items_created = await create_imagery_finder_items(
            imagery_finder_pk=imagery_finder_pk,
            dream_pk=dream_pk,
        )
        
        # Step 3: Transform lookup items into normalized results
        results_created = 0
        if items_created > 0:
            results_created = await transform_lookup_results(dream_pk)
        else:
            logger.info("Skipping transformation - no items to transform")
        
        # Step 4: Mark dream as complete
        await update_dream_status(dream_pk, DREAM_STATUS_COMPLETE)
        
        summary = {
            "imagery_finder_pk": imagery_finder_pk,
            "imagery_finder_name": imagery_finder_info.get("name"),
            "dream_pk": dream_pk,
            "items_created": items_created,
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
