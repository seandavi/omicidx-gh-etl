# Data Warehouse Implementation Summary

## What We Built

A **lightweight, dbt-compatible data warehouse** for OmicIDX using DuckDB, providing:

- 📊 **3-layer architecture** (raw → staging → mart)
- 🔄 **Automatic dependency resolution** and DAG execution
- 📝 **Documentation as code** (schema.yml files)
- 🔍 **Metadata tracking** (lineage, execution history, quality metrics)
- 🚀 **CLI commands** for all warehouse operations
- 📦 **Migration path to dbt** when needed

## Architecture Decision: DuckDB (Not dbt/ClickHouse/Postgres)

### Why DuckDB?

For your use case (<100GB, bulk loads, analytical workloads):

✅ **Advantages:**
- No server overhead (file-based database)
- Native parquet support (reads directly from R2/S3)
- Excellent analytical performance
- Simple deployment (single file)
- Perfect for <100GB datasets
- Direct integration with your Python ETL

❌ **When to reconsider:**
- Need concurrent writes from multiple sources
- Dataset grows >1TB
- Need real-time streaming ingestion
- Multiple teams need simultaneous access

### Why Not dbt (Yet)?

We built a **dbt-compatible structure** without dbt itself because:

**Current state**: Single developer, <30 models, straightforward transformations
- ✅ You get: SQL-based transformations, dependency management, documentation, lineage
- ❌ You skip: Complex setup, compilation steps, learning curve

**Future state**: When you have >30 models, multiple contributors, or need advanced testing
- 🔄 Easy migration path (your SQL files already work with dbt)

## Project Structure

```
omicidx-gh-etl/
├── WAREHOUSE.md                          # Complete documentation
├── WAREHOUSE_SUMMARY.md                  # This file
├── examples/
│   └── warehouse_quickstart.py          # Getting started example
├── omicidx_etl/
│   ├── cli.py                           # Main CLI (updated)
│   ├── warehouse_cli.py                 # Warehouse commands
│   └── transformations/
│       ├── warehouse.py                 # Orchestration engine
│       ├── create_raw.py                # Original code (kept for reference)
│       └── models/
│           ├── README.md                # Model documentation
│           ├── raw/                     # Layer 1: Source data
│           │   ├── schema.yml
│           │   ├── sra_studies.sql
│           │   ├── sra_experiments.sql
│           │   ├── sra_samples.sql
│           │   ├── sra_runs.sql
│           │   └── ncbi_biosamples.sql
│           ├── staging/                 # Layer 2: Cleaned data
│           │   ├── schema.yml
│           │   ├── stg_sra_studies.sql
│           │   └── stg_sra_experiments.sql
│           └── mart/                    # Layer 3: Analytics
│               ├── schema.yml
│               └── sra_metadata.sql
```

## New CLI Commands

All available under `python -m omicidx_etl.cli warehouse`:

```bash
# Initialize warehouse database
oidx warehouse init

# List all models
oidx warehouse list-models

# Show execution plan (dry run)
oidx warehouse plan

# Run all transformations
oidx warehouse run

# Run specific models
oidx warehouse run --models stg_sra_studies --models sra_metadata

# View execution history
oidx warehouse history

# List tables
oidx warehouse tables

# Show model documentation
oidx warehouse describe sra_metadata
```

## Key Features

### 1. Three-Layer Architecture

```
Raw Layer (views)
  ↓
Staging Layer (cleaned, typed, validated)
  ↓
Mart Layer (joined, aggregated, documented)
```

**Example flow:**
```
sra/*study*.parquet → raw.sra_studies → staging.stg_sra_studies → mart.sra_metadata
```

### 2. Automatic Dependency Resolution

Models declare dependencies in `schema.yml`:

```yaml
models:
  - name: sra_metadata
    depends_on:
      - staging.stg_sra_studies
      - staging.stg_sra_experiments
```

The system automatically:
- Topologically sorts models
- Runs dependencies first
- Detects circular dependencies

### 3. Metadata Tracking

Everything tracked in `meta` schema:

- **model_runs**: Execution history, timing, errors
- **model_lineage**: Dependency graph
- **model_docs**: Documentation and column descriptions

### 4. Documentation as Code

Schema.yml files (dbt-compatible format):

```yaml
models:
  - name: stg_sra_studies
    description: Cleaned SRA studies with quality flags
    columns:
      - name: accession
        description: Unique study identifier
      - name: has_complete_metadata
        description: Quality flag for metadata completeness
```

### 5. Flexible Materialization

Three strategies (set in schema.yml):

- **view** (default): Always fresh, no space, slower queries
- **table**: Physical table, faster queries, uses space
- **external_table**: For COPY operations (exports)

## Workflow Integration

Your existing ETL continues to work:

```bash
# 1. Extract (your existing pipeline)
oidx sra extract

# 2. Load (automatic - raw layer views point to parquet)
# No action needed

# 3. Transform (new warehouse step)
oidx warehouse run

# 4. Export (mart models write to parquet for R2)
# Defined in your mart models
```

## Quick Start

### 1. Update File Paths

Edit raw models to point to your data:

```sql
-- In models/raw/sra_studies.sql
SELECT * FROM read_parquet('/YOUR/PATH/sra/*study*.parquet', union_by_name := true)
```

### 2. Initialize Warehouse

```bash
oidx warehouse init --db-path omicidx_warehouse.duckdb
```

### 3. Run Transformations

```bash
# Dry run first
oidx warehouse plan

# Then run for real
oidx warehouse run
```

### 4. Query Results

```bash
duckdb omicidx_warehouse.duckdb
```

```sql
SELECT * FROM mart.sra_metadata LIMIT 10;
```

## Adding New Models

1. **Create SQL file**: `models/staging/stg_geo_samples.sql`
2. **Add documentation**: Update `models/staging/schema.yml`
3. **Run it**: `oidx warehouse run --models stg_geo_samples`

Example SQL:

```sql
-- models/staging/stg_geo_samples.sql
SELECT
    accession,
    title,
    CAST(submission_date AS DATE) AS submission_date,
    organism,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw.geo_samples
```

Example schema.yml:

```yaml
models:
  - name: stg_geo_samples
    description: Cleaned GEO sample data
    depends_on:
      - raw.geo_samples
    columns:
      - name: accession
        description: GEO sample accession (GSM)
```

## Comparison: Before vs After

### Before (create_raw.py)

```python
# Hardcoded transformations
RAW_TABLES = [
    TableConfig(name="sra_studies", source_pattern="sra/*study*.parquet", ...),
    # ...
]

# Manual dependency management
sorted_tables = topological_sort(tables)

# No metadata tracking
# No documentation
# No lineage
```

### After (warehouse.py)

```sql
-- models/raw/sra_studies.sql
SELECT * FROM read_parquet(...);
```

```yaml
# models/raw/schema.yml
models:
  - name: sra_studies
    description: Raw SRA studies
    columns:
      - name: accession
        description: Study accession
```

```bash
# Automatic discovery, execution, tracking
oidx warehouse run
```

**Benefits:**
- ✅ SQL-first (more maintainable)
- ✅ Self-documenting
- ✅ Automatic lineage
- ✅ Execution history
- ✅ Easy to extend

## Migration Path to dbt

When you're ready (>30 models, multiple contributors):

1. Install dbt: `pip install dbt-duckdb`
2. Create `dbt_project.yml`:
   ```yaml
   name: omicidx
   profile: omicidx
   model-paths: ["omicidx_etl/transformations/models"]
   ```
3. Your SQL and schema.yml files work as-is!

## Performance Considerations

Current setup handles:
- ✅ <100GB datasets efficiently
- ✅ Direct parquet reading (no import needed)
- ✅ Parallel query execution
- ✅ Optimal for analytical workloads

**Tuning options:**

```python
WarehouseConfig(
    threads=16,              # Match CPU cores
    memory_limit='8GB',      # Adjust for your system
    temp_directory='/fast/ssd'  # Use SSD for temp files
)
```

## Exporting Data

### To Parquet

Create export model:

```sql
-- models/mart/export_sra_metadata.sql
COPY (SELECT * FROM mart.sra_metadata)
TO '/data/exports/sra_metadata.parquet' (FORMAT parquet, COMPRESSION zstd);
```

### To R2/S3

```sql
COPY (SELECT * FROM mart.sra_metadata)
TO 's3://bucket/sra_metadata.parquet' (FORMAT parquet);
```

### Remote Views

Create queryable database with views to remote files:

```python
from omicidx_etl.transformations.create_raw import create_remote_views

create_remote_views(
    tables,
    'https://store.cancerdatasci.org/omicidx/raw',
    'omicidx_remote.duckdb'
)
```

## What's Included

### Example Models (5 raw, 2 staging, 1 mart)

- ✅ Raw: SRA studies, experiments, samples, runs, biosamples
- ✅ Staging: Cleaned studies and experiments
- ✅ Mart: Combined SRA metadata

### Documentation

- ✅ [WAREHOUSE.md](WAREHOUSE.md) - Complete reference
- ✅ [models/README.md](omicidx_etl/transformations/models/README.md) - Model guidelines
- ✅ [examples/warehouse_quickstart.py](examples/warehouse_quickstart.py) - Tutorial

### Features

- ✅ Dependency resolution
- ✅ Metadata tracking
- ✅ Execution history
- ✅ CLI commands
- ✅ Documentation as code
- ✅ dbt-compatible structure

## Next Steps

### Immediate

1. **Update paths** in raw/ models to point to your data
2. **Run warehouse**: `oidx warehouse run`
3. **Query results**: Test with DuckDB CLI

### Short-term

1. **Add more models**: GEO, BioSample, PubMed transformations
2. **Create mart views**: Build your export-ready views
3. **Set up exports**: Define parquet export models
4. **Automate**: Schedule with cron or GitHub Actions

### Long-term

1. **Add testing**: Implement data quality checks
2. **Add monitoring**: Track execution metrics
3. **Consider dbt**: When complexity justifies it
4. **Scale up**: If data grows beyond DuckDB's sweet spot

## Dependencies Added

```toml
# pyproject.toml
dependencies = [
    # ... existing deps ...
    "pyyaml>=6.0",      # For schema.yml parsing
    "click>=8.0.0",     # For CLI (may already be present)
]
```

## Support

- **Documentation**: See [WAREHOUSE.md](WAREHOUSE.md)
- **Examples**: See [examples/warehouse_quickstart.py](examples/warehouse_quickstart.py)
- **Model Guide**: See [models/README.md](omicidx_etl/transformations/models/README.md)

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| **DuckDB over ClickHouse** | <100GB data, no server overhead, parquet-native |
| **DuckDB over Postgres** | Analytical workload (not OLTP), better for 100GB range |
| **Custom framework over dbt** | Simpler for single developer, easy migration path |
| **3-layer architecture** | Separation of concerns, clear data flow |
| **Views for raw layer** | No data copying, instant refresh, optimal for DuckDB |
| **Metadata in database** | Self-contained, queryable, no external dependencies |
| **dbt-compatible structure** | Future-proof, industry standard, easy migration |
| **CLI-first interface** | Scriptable, automatable, consistent with existing tools |

## Key Takeaways

1. **Start simple**: This framework gives you warehouse benefits without complexity
2. **Stay flexible**: Structure allows easy migration to dbt or other tools
3. **DuckDB is enough**: For <100GB analytical data, it's ideal
4. **Document everything**: Future you (and collaborators) will thank you
5. **Incremental adoption**: Add models as needed, no need to migrate everything at once

---

**Status**: Ready to use on `feature/duckdb-warehouse` branch

**Next**: Update paths in raw models, run first transformation, iterate!
