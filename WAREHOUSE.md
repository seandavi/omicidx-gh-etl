# OmicIDX Data Warehouse

A lightweight, DuckDB-based data warehouse with dbt-compatible structure for managing genomics metadata transformations.

## Architecture

```
omicidx_warehouse.duckdb
├── raw/           # Direct imports from parquet/ndjson (no transformations)
├── staging/       # Cleaned, validated, enriched tables
├── mart/          # Business logic views (production-ready exports)
└── meta/          # Metadata, lineage, and execution tracking
```

### Design Philosophy

This warehouse follows an **ELT (Extract-Load-Transform)** approach with three layers:

1. **Raw Layer**: Direct access to source data with minimal transformation
   - Materializes as views pointing to parquet files
   - Fast to refresh (no data copying)
   - DuckDB reads directly from files

2. **Staging Layer**: Data quality and standardization
   - Type casting and date standardization
   - Data quality flags
   - Deduplication and validation
   - Audit timestamps

3. **Mart Layer**: Business logic and analytics
   - Joins across tables
   - Aggregate calculations
   - Production-ready exports
   - Documentation-heavy

## Why Not dbt?

This framework gives you **80% of dbt's benefits with 20% of the complexity**:

### What You Get (without dbt):
- ✅ SQL-based transformations (portable, version-controlled)
- ✅ Dependency management and DAG execution
- ✅ Documentation as code (schema.yml files)
- ✅ Lineage tracking in database
- ✅ Execution history and monitoring
- ✅ DuckDB-specific optimizations (direct parquet access)
- ✅ Simple Python orchestration
- ✅ Fast iteration cycle

### When to Migrate to dbt:
- Multiple contributors need workflow conventions
- Complex incremental materialization strategies needed
- Advanced testing framework required (schema tests, data quality)
- Community packages/macros would add value
- Auto-generated documentation website needed
- \>30 complex interdependent models

### Migration Path:
Your SQL files and `schema.yml` are **already dbt-compatible**. When you're ready:
1. Install dbt-duckdb
2. Create `dbt_project.yml`
3. Move `models/` directory to dbt project
4. Your SQL and schema files work with minimal changes

## Quick Start

### 1. Initialize the Warehouse

```bash
# Create warehouse database and schemas
python -m omicidx_etl.cli warehouse init

# Or specify custom location
python -m omicidx_etl.cli warehouse init --db-path /data/warehouse.duckdb
```

### 2. List Available Models

```bash
# Discover all models in models directory
python -m omicidx_etl.cli warehouse list-models

# Output:
# RAW:
#   - sra_studies
#   - sra_experiments
#   - sra_samples
#   - sra_runs
#   - ncbi_biosamples
#
# STAGING:
#   - stg_sra_studies (depends on: raw.sra_studies)
#   - stg_sra_experiments (depends on: raw.sra_experiments)
#
# MART:
#   - sra_metadata (depends on: staging.stg_sra_studies, staging.stg_sra_experiments)
```

### 3. Preview Execution Plan

```bash
# See what would run without executing
python -m omicidx_etl.cli warehouse plan
```

### 4. Run Transformations

```bash
# Run all models
python -m omicidx_etl.cli warehouse run

# Run specific models
python -m omicidx_etl.cli warehouse run --models stg_sra_studies --models sra_metadata

# Use custom database
python -m omicidx_etl.cli warehouse run --db-path /data/warehouse.duckdb
```

### 5. Monitor Execution

```bash
# View recent runs
python -m omicidx_etl.cli warehouse history

# List all tables
python -m omicidx_etl.cli warehouse tables

# Show documentation for a model
python -m omicidx_etl.cli warehouse describe sra_metadata
```

## Directory Structure

```
omicidx_etl/transformations/
├── models/
│   ├── raw/
│   │   ├── sra_studies.sql          # Raw data import
│   │   ├── sra_experiments.sql
│   │   └── schema.yml               # Documentation
│   ├── staging/
│   │   ├── stg_sra_studies.sql      # Cleaned/validated
│   │   ├── stg_sra_experiments.sql
│   │   └── schema.yml
│   └── mart/
│       ├── sra_metadata.sql         # Production view
│       └── schema.yml
├── warehouse.py                      # Orchestration engine
└── create_raw.py                     # Original transformation code (kept for reference)
```

## Creating New Models

### 1. Create SQL File

Create a new `.sql` file in the appropriate layer directory:

**Example: `models/staging/stg_geo_samples.sql`**

```sql
-- Staging: Clean GEO sample data

SELECT
    accession,
    title,
    CAST(submission_date AS DATE) AS submission_date,
    organism,

    -- Data quality flag
    CASE
        WHEN title IS NULL THEN FALSE
        ELSE TRUE
    END AS has_complete_metadata,

    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.geo_samples
WHERE accession IS NOT NULL
```

### 2. Document in schema.yml

Add model documentation:

**`models/staging/schema.yml`**

```yaml
models:
  - name: stg_geo_samples
    description: |
      Cleaned and validated GEO sample metadata.
      - Standardized date formats
      - Quality flags added
    materialized: view
    depends_on:
      - raw.geo_samples
    tags:
      - staging
      - geo
    columns:
      - name: accession
        description: Unique GEO sample accession (GSM)
      - name: has_complete_metadata
        description: Quality flag for metadata completeness
```

### 3. Run Your Model

```bash
python -m omicidx_etl.cli warehouse run --models stg_geo_samples
```

## Materialization Strategies

Set `materialized` in `schema.yml`:

### View (default)
```yaml
materialized: view
```
- Fast to refresh (no data copying)
- Always up-to-date with source
- Query time may be slower for complex transformations
- **Best for**: Raw layer, simple staging transformations

### Table
```yaml
materialized: table
```
- Materialized as physical table
- Faster query time
- Takes space and time to refresh
- **Best for**: Complex staging transformations, final marts

### External Table
```yaml
materialized: external_table
```
- Use for COPY operations that write parquet files
- **Best for**: Exporting data to R2/S3

## Metadata Tracking

The warehouse automatically tracks execution metadata in the `meta` schema:

### Model Runs
```sql
SELECT * FROM meta.model_runs
WHERE model_name = 'sra_metadata'
ORDER BY started_at DESC
LIMIT 10;
```

Tracks:
- Execution time
- Rows affected
- Success/error status
- SQL hash (detects changes)

### Model Lineage
```sql
SELECT * FROM meta.model_lineage
WHERE model_name = 'sra_metadata';
```

Shows dependencies between models.

### Model Documentation
```sql
SELECT * FROM meta.model_docs
WHERE model_name = 'sra_metadata';
```

Stores descriptions, column documentation, tags.

## Configuration

### Warehouse Config

Customize in code or via CLI options:

```python
from omicidx_etl.transformations.warehouse import WarehouseConfig

config = WarehouseConfig(
    db_path='omicidx_warehouse.duckdb',
    models_dir='omicidx_etl/transformations/models',
    threads=16,
    memory_limit='8GB',
    temp_directory='/tmp/duckdb'  # Important for large operations
)
```

### Model Config

In `schema.yml`:

```yaml
models:
  - name: my_model
    description: Model description
    materialized: view  # or 'table', 'external_table'
    depends_on:
      - raw.source_table
      - staging.other_model
    tags:
      - production
      - export
```

## Exporting Data

### To Parquet Files

Create an export model with `external_table` materialization:

**`models/mart/export_sra_metadata.sql`**

```sql
COPY (
    SELECT * FROM mart.sra_metadata
)
TO '/data/exports/sra_metadata.parquet' (
    FORMAT parquet,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 100000
);
```

### To Remote Storage (R2/S3)

DuckDB can write directly to S3:

```sql
-- Configure S3 credentials first
SET s3_access_key_id='...';
SET s3_secret_access_key='...';
SET s3_endpoint='...';

COPY (SELECT * FROM mart.sra_metadata)
TO 's3://bucket/sra_metadata.parquet';
```

### Create Remote Views

For querying data in R2 without downloading:

```python
from omicidx_etl.transformations.create_raw import create_remote_views, RAW_TABLES

# Creates a lightweight DuckDB file with views to remote parquet files
create_remote_views(
    RAW_TABLES,
    'https://store.cancerdatasci.org/omicidx/raw',
    'omicidx_remote.duckdb'
)
```

Users can then query directly:

```python
import duckdb

conn = duckdb.connect('omicidx_remote.duckdb')
result = conn.execute("SELECT * FROM sra_studies WHERE organism = 'Homo sapiens' LIMIT 10")
```

## Integration with Existing ETL

Your existing ETL pipeline generates parquet files. The warehouse consumes them:

```
ETL Pipeline → Parquet Files → Warehouse Raw Layer → Staging → Mart → Exports
```

**Workflow:**

1. **Extract**: Run your existing ETL commands
   ```bash
   python -m omicidx_etl.cli sra extract
   ```

2. **Load**: Raw layer points to parquet files (instant)
   ```bash
   # Raw views automatically read from parquet
   ```

3. **Transform**: Run warehouse transformations
   ```bash
   python -m omicidx_etl.cli warehouse run
   ```

4. **Export**: Materialize mart views to parquet
   ```bash
   # Your mart models write to parquet for R2 upload
   ```

## Advanced Usage

### Programmatic Access

```python
from omicidx_etl.transformations.warehouse import (
    WarehouseConfig,
    WarehouseConnection,
    run_warehouse
)

# Run warehouse programmatically
config = WarehouseConfig()
results = run_warehouse(config)

# Or use connection directly
with WarehouseConnection(config) as conn:
    result = conn.execute("SELECT COUNT(*) FROM mart.sra_metadata")
    print(result.fetchone())
```

### Custom Transformations

For complex Python logic, use the transformation framework:

```python
from omicidx_etl.transformations.warehouse import execute_model, ModelConfig

model = ModelConfig(
    name='custom_transformation',
    layer='staging',
    sql_path=Path('custom.sql'),
    materialization='table'
)

with WarehouseConnection(config) as conn:
    result = execute_model(conn, model)
```

### Incremental Updates

For incremental updates (future enhancement):

```sql
-- In your model SQL
SELECT *
FROM read_parquet('/data/source/*.parquet')
WHERE last_modified > (
    SELECT MAX(last_modified)
    FROM staging.stg_table
)
```

## Performance Tips

1. **Use views for raw layer**: No data copying, instant refresh
2. **Materialize complex staging**: If transformations are expensive
3. **Partition large exports**: Use multiple parquet files for >1GB datasets
4. **Set temp_directory**: For large operations, use fast SSD
5. **Tune threads**: Match your CPU cores
6. **Use ZSTD compression**: Good balance of speed and compression ratio

## Troubleshooting

### Model fails with "table not found"

Check dependencies in `schema.yml`:
```yaml
depends_on:
  - raw.source_table  # Must match layer.table_name
```

### Out of memory errors

```python
config = WarehouseConfig(
    memory_limit='16GB',  # Increase limit
    temp_directory='/path/to/fast/storage'  # Use SSD
)
```

### Slow query performance

```sql
-- Add indexes (DuckDB auto-indexes, but can hint):
CREATE INDEX idx_accession ON staging.stg_studies(accession);

-- Or materialize as table instead of view
```

### View history and debug

```bash
# Check recent runs
python -m omicidx_etl.cli warehouse history

# Look for errors
python -m omicidx_etl.cli warehouse history --limit 50 | grep ERROR
```

## Comparison: This Framework vs. Alternatives

| Feature | This Framework | dbt | ClickHouse | PostgreSQL |
|---------|---------------|-----|------------|------------|
| Setup complexity | Low | Medium | High | Medium |
| Operational overhead | None (file-based) | Low | High (server) | Medium (server) |
| SQL-based | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Parquet-native | ✅ Yes | Via DuckDB | No | No |
| Direct R2/S3 access | ✅ Yes | Via DuckDB | Requires import | Requires import |
| Dependency management | ✅ Yes | ✅ Yes | Manual | Manual |
| Lineage tracking | ✅ Yes | ✅ Yes | No | No |
| Testing framework | Basic | Advanced | No | No |
| Docs website | No | ✅ Yes | No | No |
| Migration complexity | N/A | Easy | Hard | Medium |
| Best for | <100GB analytical | Any size | >1TB, real-time | OLTP + OLAP |

## Next Steps

1. **Add more models**: Create staging/mart models for GEO, BioSample, etc.
2. **Add data quality tests**: Implement validation in SQL or Python
3. **Automate exports**: Schedule warehouse runs with cron or Airflow
4. **Add monitoring**: Integrate with monitoring tools
5. **Migrate to dbt**: When complexity justifies it (>30 models, multiple contributors)

## Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/) (for future migration)
- DuckDB Parquet: https://duckdb.org/docs/data/parquet
- DuckDB S3: https://duckdb.org/docs/guides/import/s3_import
