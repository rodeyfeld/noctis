"""
Noctis - Prefect Workflow Orchestration

Entry point for deploying and running Noctis flows.
"""

import asyncio

from prefect import serve

from flows import imagery_finder_study


def deploy_flows():
    """
    Deploy all Noctis flows to Prefect.
    
    This creates deployments that can be triggered via:
    - Prefect UI
    - Prefect CLI: prefect deployment run 'imagery-finder-study/default'
    - Prefect API
    """
    imagery_finder_deployment = imagery_finder_study.to_deployment(
        name="default",
        description="Process ImageryFinder requests and create archive lookup items",
        tags=["study", "imagery-finder", "spatial"],
    )
    
    # Serve all deployments
    asyncio.run(
        serve(
            imagery_finder_deployment,
        )
    )


def main():
    """Main entry point for Noctis."""
    print("🌙 Noctis - Prefect Workflow Orchestration")
    print("=" * 50)
    print()
    print("Available flows:")
    print("  - imagery-finder-study: Spatial archive matching")
    print()
    print("Starting flow server...")
    deploy_flows()


if __name__ == "__main__":
    main()
