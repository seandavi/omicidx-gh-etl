# Data Warehouse Models

This directory contains SQL models organized in a dbt-compatible structure for the OmicIDX data warehouse.

## Directory Structure

```
models/
├── raw/           # Entry point: direct imports from parquet files
├── staging/       # Cleaned, validated, typed data
└── mart/          # Production-ready views for export and querying
```

## Model Layers

### Raw Layer (`raw/`)

**Purpose**: Direct access to source data with minimal transformation

- Materialized as **views** (no data copying)
- Points directly to parquet files from ETL pipeline
- Fast to "refresh" (just updates view definition)
- No business logic - just consolidation of source files

**Example**: `raw/sra_studies.sql`
```sql
SELECT * FROM read_parquet('/data/sra/*study*.parquet', union_by_name := true)
```

### Staging Layer (`staging/`)

**Purpose**: Clean, validate, and standardize data

- Type casting and standardization
- Data quality flags
- Deduplication
- NULL handling
- Audit timestamps
- No joins yet (single-table focus)

**Example**: `staging/stg_sra_studies.sql`
```sql
SELECT
    accession,
    CAST(publish_date AS DATE) AS publish_date,
    CASE WHEN title IS NOT NULL THEN TRUE ELSE FALSE END AS has_title,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw.sra_studies
```

### Mart Layer (`mart/`)

**Purpose**: Business logic and production-ready datasets

- Joins across tables
- Aggregate calculations
- Derived metrics
- Heavy documentation
- Optimized for consumption (API, exports, analysis)

**Example**: `mart/sra_metadata.sql`
```sql
SELECT
    s.study_accession,
    s.title,
    e.platform,
    e.library_strategy,
    COUNT(DISTINCT e.experiment_accession) AS experiment_count
FROM staging.stg_sra_studies s
LEFT JOIN staging.stg_sra_experiments e ON s.study_accession = e.study_accession
GROUP BY s.study_accession, s.title, e.platform, e.library_strategy
```

## Creating a New Model

### 1. Write SQL

Create a `.sql` file in the appropriate layer:

```bash
touch models/staging/stg_my_model.sql
```

### 2. Add Documentation

Add to the layer's `schema.yml`:

```yaml
models:
  - name: stg_my_model
    description: What this model does
    materialized: view  # or 'table'
    depends_on:
      - raw.source_table
    columns:
      - name: accession
        description: Primary identifier
```

### 3. Run It

```bash
python -m omicidx_etl.cli warehouse run --models stg_my_model
```

## Documentation Format

Each layer has a `schema.yml` file following dbt conventions:

```yaml
version: 2

models:
  - name: model_name
    description: |
      Multi-line description
      of what this model does
    materialized: view  # view, table, or external_table
    depends_on:
      - layer.upstream_model
    tags:
      - tag1
      - tag2
    columns:
      - name: column_name
        description: What this column represents
```

## Materialization Strategies

Set in `schema.yml`:

- **`view`** (default): No data copying, always fresh, slower queries
- **`table`**: Physical table, faster queries, uses space
- **`external_table`**: For COPY operations (exporting parquet)

## Naming Conventions

- **Raw**: `table_name` (e.g., `sra_studies`)
- **Staging**: `stg_table_name` (e.g., `stg_sra_studies`)
- **Mart**: `descriptive_name` (e.g., `sra_metadata`, `biosample_geo_links`)

## Dependencies

Declare in `schema.yml`:

```yaml
depends_on:
  - raw.sra_studies
  - staging.stg_sra_experiments
```

This ensures models run in the correct order.

## Example Models Provided

### Raw Layer
- `sra_studies.sql` - SRA study metadata
- `sra_experiments.sql` - Experiment details
- `sra_samples.sql` - Sample information
- `sra_runs.sql` - Run metadata
- `ncbi_biosamples.sql` - BioSample records

### Staging Layer
- `stg_sra_studies.sql` - Cleaned studies
- `stg_sra_experiments.sql` - Cleaned experiments

### Mart Layer
- `sra_metadata.sql` - Combined SRA metadata view

## Path Configuration

Raw models reference parquet files. **Update paths** to match your environment:

```sql
-- Change this:
SELECT * FROM read_parquet('/data/davsean/omicidx_root/sra/*study*.parquet', ...)

-- To your path:
SELECT * FROM read_parquet('/your/data/path/sra/*study*.parquet', ...)
```

Or use environment variables (DuckDB supports them):

```sql
SELECT * FROM read_parquet('${OMICIDX_ROOT}/sra/*study*.parquet', ...)
```

## Testing Your Models

Run specific models to test:

```bash
# Test one model
python -m omicidx_etl.cli warehouse run --models stg_sra_studies

# Dry run (show what would execute)
python -m omicidx_etl.cli warehouse plan

# List all discovered models
python -m omicidx_etl.cli warehouse list-models
```

Query results directly:

```bash
duckdb omicidx_warehouse.duckdb
```

```sql
-- Check row counts
SELECT COUNT(*) FROM staging.stg_sra_studies;

-- Verify data quality flags
SELECT has_complete_metadata, COUNT(*)
FROM staging.stg_sra_studies
GROUP BY has_complete_metadata;
```

## Best Practices

1. **Single responsibility**: Each model should do one thing well
2. **No hardcoded filters**: Keep models generic, filter in queries
3. **Document everything**: Future you will thank present you
4. **Quality flags over filtering**: Add `has_*` flags instead of WHERE clauses
5. **Consistent naming**: Follow the conventions above
6. **Incremental logic**: Put "what's new" logic in staging, not raw

## Migration to dbt

These models are **already dbt-compatible**. When ready to migrate:

1. Install dbt-duckdb: `pip install dbt-duckdb`
2. Create `dbt_project.yml`:
   ```yaml
   name: omicidx
   models:
     omicidx:
       raw:
         materialized: view
       staging:
         materialized: view
       mart:
         materialized: view
   ```
3. Your SQL and `schema.yml` files work as-is!

## Questions?

See [WAREHOUSE.md](../../../WAREHOUSE.md) for complete documentation.
