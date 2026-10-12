"""
Noctis - Prefect Workflow Orchestration

Entry point for deploying and running Noctis flows.
"""

import asyncio

from prefect import serve

from flows import imagery_finder_study, sentinel_backfill, sentinel_sync


def deploy_flows():
    """
    Deploy all Noctis flows to Prefect.
    
    This creates deployments that can be triggered via:
    - Prefect UI
    - Prefect CLI: prefect deployment run 'imagery-finder-study/default'
    - Prefect API

    sentinel-sync/daily also runs on its own cron schedule. sentinel-backfill/manual
    only runs when started (Prefect UI: Deployments > sentinel-backfill > Run).
    """
    imagery_finder_deployment = imagery_finder_study.to_deployment(
        name="default",
        description="Process ImageryFinder requests and create archive lookup items",
        tags=["study", "imagery-finder", "spatial"],
    )

    sentinel_sync_deployment = sentinel_sync.to_deployment(
        name="daily",
        description="Pull new Sentinel scenes from Copernicus STAC into ephemeris",
        tags=["sync", "sentinel", "stac"],
        cron="0 2 * * *",  # 02:00 UTC daily
    )

    sentinel_backfill_deployment = sentinel_backfill.to_deployment(
        name="manual",
        description="Backfill past Sentinel scenes into ephemeris, newest first, up to a size cap",
        tags=["sync", "sentinel", "stac", "backfill"],
    )

    # Serve all deployments
    asyncio.run(
        serve(
            imagery_finder_deployment,
            sentinel_sync_deployment,
            sentinel_backfill_deployment,
        )
    )


def main():
    """Main entry point for Noctis."""
    print("🌙 Noctis - Prefect Workflow Orchestration")
    print("=" * 50)
    print()
    print("Available flows:")
    print("  - imagery-finder-study: Spatial archive matching")
    print("  - sentinel-sync: Daily Copernicus STAC pull (02:00 UTC)")
    print("  - sentinel-backfill: Past Sentinel scenes, newest first (manual)")
    print()
    print("Starting flow server...")
    deploy_flows()


if __name__ == "__main__":
    main()
