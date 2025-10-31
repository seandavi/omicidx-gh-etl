#!/usr/bin/env python3
"""
Quick start example for the OmicIDX Data Warehouse.

This script demonstrates:
1. Initializing a warehouse
2. Running transformations
3. Querying results
4. Exporting to parquet
"""

import duckdb
from pathlib import Path
from omicidx_etl.transformations.warehouse import (
    WarehouseConfig,
    WarehouseConnection,
    run_warehouse,
    discover_models
)

def main():
    print("=" * 80)
    print("OmicIDX Data Warehouse - Quick Start Example")
    print("=" * 80)
    print()

    # Configuration
    config = WarehouseConfig(
        db_path='example_warehouse.duckdb',
        models_dir='omicidx_etl/transformations/models',
        threads=4,
        memory_limit='2GB'
    )

    print("Step 1: Initialize warehouse")
    print("-" * 80)
    with WarehouseConnection(config) as conn:
        print("✓ Warehouse initialized with schemas: raw, staging, mart, meta")
    print()

    print("Step 2: Discover models")
    print("-" * 80)
    models = discover_models(config.models_dir)
    print(f"✓ Found {len(models)} models:")
    for model in models:
        print(f"  - {model.layer}.{model.name}")
    print()

    print("Step 3: Run transformations (dry run)")
    print("-" * 80)
    print("Note: This is a dry run. To actually run, set dry_run=False")
    results = run_warehouse(config, dry_run=True)
    print(f"✓ Would execute {len(results)} models in order")
    print()

    print("Step 4: Query the warehouse")
    print("-" * 80)
    print("Example queries you can run after transformations complete:")
    print()

    example_queries = [
        ("Count studies by study type", """
        SELECT study_type, COUNT(*) as count
        FROM staging.stg_sra_studies
        GROUP BY study_type
        ORDER BY count DESC
        LIMIT 10;
        """),

        ("Get recent studies with metadata", """
        SELECT
            accession,
            title,
            publish_date,
            study_type,
            has_complete_metadata
        FROM staging.stg_sra_studies
        WHERE publish_date >= '2024-01-01'
        ORDER BY publish_date DESC
        LIMIT 10;
        """),

        ("Platform distribution", """
        SELECT
            platform,
            library_strategy,
            COUNT(*) as experiment_count
        FROM staging.stg_sra_experiments
        GROUP BY platform, library_strategy
        ORDER BY experiment_count DESC
        LIMIT 10;
        """),

        ("Complete SRA metadata (mart)", """
        SELECT *
        FROM mart.sra_metadata
        WHERE organism = 'Homo sapiens'
        LIMIT 10;
        """)
    ]

    for title, query in example_queries:
        print(f"  {title}:")
        print(f"  {query.strip()}")
        print()

    print("Step 5: Export data")
    print("-" * 80)
    print("Example export commands:")
    print()

    export_examples = [
        "# Export mart to parquet",
        "COPY (SELECT * FROM mart.sra_metadata)",
        "TO 'exports/sra_metadata.parquet' (FORMAT parquet, COMPRESSION zstd);",
        "",
        "# Export to multiple partitioned files",
        "COPY (SELECT * FROM mart.sra_metadata)",
        "TO 'exports/sra_metadata' (FORMAT parquet, PARTITION_BY (study_type));",
        "",
        "# Export to S3/R2",
        "COPY (SELECT * FROM mart.sra_metadata)",
        "TO 's3://bucket/sra_metadata.parquet' (FORMAT parquet);"
    ]

    for line in export_examples:
        print(f"  {line}")
    print()

    print("=" * 80)
    print("Next Steps:")
    print("=" * 80)
    print()
    print("1. Update file paths in raw/ models to point to your data")
    print("   Edit: omicidx_etl/transformations/models/raw/*.sql")
    print()
    print("2. Run the warehouse:")
    print("   python -m omicidx_etl.cli warehouse run")
    print()
    print("3. Query your data:")
    print("   duckdb omicidx_warehouse.duckdb")
    print()
    print("4. Add more models for GEO, BioSample, etc.")
    print()
    print("See WAREHOUSE.md for complete documentation.")
    print()


if __name__ == "__main__":
    main()
