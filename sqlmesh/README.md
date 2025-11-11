# SQLMesh Data Warehouse for OmicIDX

SQLMesh-based data warehouse for OmicIDX genomics metadata, supporting incremental processing of NCBI SRA, GEO, BioSample, BioProject, and EBI BioSample data.

## Quick Start

### Prerequisites

- Python 3.11+
- `uv` package manager
- Source data files (parquet/NDJSON) at your configured data root

### Environment Setup

Set the data root path via environment variable:

```bash
export OMICIDX_DATA_ROOT=/data/davsean/omicidx_root
```

Or create a `.env` file in the sqlmesh directory:

```bash
OMICIDX_DATA_ROOT=/data/davsean/omicidx_root
```

### Common Commands

```bash
# View model information
uv run sqlmesh info

# View execution plan (dry run)
uv run sqlmesh plan --dry-run

# Apply changes to production
uv run sqlmesh plan --auto-apply

# Run all tests
uv run sqlmesh test

# Render a model to see compiled SQL
uv run sqlmesh render bronze.stg_geo_samples

# Run specific models
uv run sqlmesh plan --select bronze.stg_sra_experiments --auto-apply
```

## Architecture

### Current Layer Structure

```
raw/               # Source views over parquet/NDJSON files (internal)
├── src_geo_samples.sql
├── src_geo_series.sql
├── src_geo_platforms.sql
├── src_sra_accessions.sql
├── src_sra_experiments.sql
├── src_sra_runs.sql
├── src_sra_samples.sql
├── src_sra_studies.sql
├── src_ebi_biosample.sql
├── src_ncbi_biosample.sql
└── src_ncbi_bioproject.sql

bronze/            # Incremental staging tables with light transformations
├── stg_geo_samples.sql
├── stg_geo_series.sql
├── stg_geo_platforms.sql
├── stg_sra_accessions.sql
├── stg_sra_experiments.sql
├── stg_sra_runs.sql
├── stg_sra_samples.sql
├── stg_sra_studies.sql
├── stg_ebi_biosample.sql
├── stg_ncbi_biosample.sql
└── stg_ncbi_bioproject.sql

geometadb/         # GEOmetadb compatibility views
├── gsm.sql        # GEO Samples
├── gse.sql        # GEO Series
├── gpl.sql        # GEO Platforms
├── gse_gpl.sql    # Series-Platform junction
├── gse_gsm.sql    # Series-Sample junction
└── geo_supplemental_files.sql
```

**Status**: 28 models, 6 tests

### Future Layers

**Silver** (planned): Business logic, cross-dataset joins, derived metrics
- `silver.sra_complete` - SRA with all related entities joined
- `silver.geo_complete` - GEO with all relationships
- `silver.cross_reference` - Links across SRA/GEO/BioSample/BioProject

**Gold/Mart** (planned): Analytics-ready, publication datasets
- `mart.sra_metadata` - Denormalized SRA for public use
- `mart.organism_summary` - Aggregate statistics by organism

## Model Details

### Raw Layer (`raw.*`)

**Purpose**: Direct views over source files, no transformations

**Pattern**:
```sql
MODEL (
    name raw.src_geo_samples,
    kind VIEW
);

SELECT column1, column2, ...
FROM read_ndjson_auto(@data_root || '/geo/gsm*.ndjson.gz', union_by_name=true)
```

**Data Sources**:
- GEO: `/geo/gsm*.ndjson.gz`, `/geo/gse*.ndjson.gz`, `/geo/gpl*.ndjson.gz`
- SRA: `/sra/*Full-{entity}-*.parquet`, `/sra/sra_accessions.parquet`
- NCBI: `/biosample/biosample-*.parquet`, `/biosample/bioproject-*.parquet`
- EBI: `/ebi_biosample/biosamples-*.parquet`

### Bronze Layer (`bronze.*`)

**Purpose**: Incremental staging with light cleaning and standardization

**Pattern**:
```sql
MODEL (
    name bronze.stg_geo_samples,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (last_update_date)
    ),
    cron '@daily',
    grain accession
);

SELECT * FROM raw.src_geo_samples
WHERE last_update_date BETWEEN @start_ds AND @end_ds
```

**Key Features**:
- Incremental processing using `INCREMENTAL_BY_TIME_RANGE`
- Runs daily at midnight UTC (configurable)
- Grain defined on unique identifier (typically `accession`)
- Standardized column names (`snake_case`)

**SRA Special Pattern**: SRA detail files don't have update dates, so they join with `src_sra_accessions` to inherit timestamps:

```sql
SELECT
    e.*,
    CAST(a.Updated AS DATE) AS updated_date,
    a.Updated AS updated_timestamp,
    a.Status AS status,
    a.BioSample AS biosample,
    a.BioProject AS bioproject
FROM raw.src_sra_experiments e
INNER JOIN raw.src_sra_accessions a ON e.accession = a.Accession
WHERE a.Type = 'EXPERIMENT'
    AND CAST(a.Updated AS DATE) BETWEEN @start_ds AND @end_ds
```

### GEOmetadb Layer (`geometadb.*`)

**Purpose**: Backward compatibility with original GEOmetadb SQLite schema

**Features**:
- Views over `bronze.stg_geo_*` tables
- Flattens nested structures (channels, contact info)
- Generates NCBI web links
- Unnests arrays for junction tables

## Configuration

### config.yaml

```yaml
default_gateway: db

model_defaults:
  dialect: duckdb
  start: 2001-01-01  # Backfill start date
  cron: '@daily'     # Default schedule

variables:
  data_root: /data/davsean/omicidx_root  # Can be overridden by OMICIDX_DATA_ROOT

linter:
  enabled: true
  rules:
    - invalidselectstarexpansion
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OMICIDX_DATA_ROOT` | `/data/davsean/omicidx_root` | Base path for all source data files |

**Note**: The `db` gateway uses `{{ env_var('OMICIDX_DATA_ROOT') }}` for dynamic data path configuration.

### Using Variables in Models

Reference variables with `@variable_name`:

```sql
SELECT * FROM read_parquet(@data_root || '/sra/sra_accessions.parquet')
```

## Testing

Tests are defined in `tests/*.yaml` using input/output fixtures:

```yaml
test_stg_geo_samples:
  model: bronze.stg_geo_samples
  inputs:
    raw.src_geo_samples:
      columns:
        accession: VARCHAR
        title: VARCHAR
        last_update_date: DATE
      rows:
        - accession: GSM1
          title: Sample 1
          last_update_date: 2005-05-28
  vars:
    start_ds: 2005-05-28
    end_ds: 2005-05-28
  outputs:
    query:
      rows:
        - accession: GSM1
          title: Sample 1
          last_update_date: 2005-05-28
```

**Run tests**:
```bash
uv run sqlmesh test
```

**Current tests**: 6 covering all bronze models
- `test_stg_geo_samples.yaml`
- `test_stg_geo_platforms.yaml`
- `test_stg_geo_series.yaml`
- `test_stg_ebi_biosample.yaml`
- `test_stg_ncbi_biosample.yaml`
- `test_stg_ncbi_bioproject.yaml`

## Naming Conventions

### Schemas
- `raw.*` - Source views (internal only)
- `bronze.*` - Staging tables
- `geometadb.*` - Compatibility views
- `silver.*` - Business logic (future)
- `mart.*` / `gold.*` - Analytics-ready (future)

### Models
- `src_*` - Raw source views
- `stg_*` - Staging/bronze tables
- No prefix - Domain-specific models (geometadb, marts)

### Columns
- **All columns**: `snake_case` (enforced in bronze layer)
- **Date columns**: `*_date` suffix (e.g., `last_update_date`, `submission_date`)
- **Timestamp columns**: `*_timestamp` suffix (e.g., `updated_timestamp`)
- **Reference columns**: `*_ref` suffix when disambiguating (e.g., `experiment_ref`)

## Data Sources

| Source | Entity Count | Location | File Pattern |
|--------|--------------|----------|--------------|
| **NCBI SRA** | ~30M experiments | `/sra/` | `*Full-{entity}-*.parquet` |
| **NCBI SRA Accessions** | ~40M records | `/sra/` | `sra_accessions.parquet` |
| **GEO Samples** | ~7M samples | `/geo/` | `gsm*.ndjson.gz` |
| **GEO Series** | ~260K series | `/geo/` | `gse*.ndjson.gz` |
| **GEO Platforms** | ~30K platforms | `/geo/` | `gpl*.ndjson.gz` |
| **NCBI BioSample** | ~40M samples | `/biosample/` | `biosample-*.parquet` |
| **NCBI BioProject** | ~800K projects | `/biosample/` | `bioproject-*.parquet` |
| **EBI BioSample** | ~10M samples | `/ebi_biosample/` | `biosamples-*.parquet` |

## Linting

SQLMesh's built-in linter enforces SQL quality:

**Enabled Rules**:
- `invalidselectstarexpansion` - Requires explicit column listing in source views

**Disabled Rules**:
- `ambiguousorinvalidcolumn` - Incompatible with dynamic file sources

## Migrating Data

To relocate data to a new path:

1. **Update environment variable**:
   ```bash
   export OMICIDX_DATA_ROOT=/new/data/path
   ```

2. **Verify configuration**:
   ```bash
   uv run sqlmesh info
   ```

3. **Apply changes**:
   ```bash
   uv run sqlmesh plan --auto-apply
   ```

All models using `@data_root` will automatically reference the new location.

## Best Practices

### Model Development

1. **Always define grain**: Specify unique identifier(s) in MODEL definition
2. **Use incremental processing**: Prefer `INCREMENTAL_BY_TIME_RANGE` for large datasets
3. **Explicit columns in raw**: List all columns in raw views (no `SELECT *`)
4. **snake_case everywhere**: Standardize column names in bronze layer
5. **Add timestamps**: Include `updated_date` and `updated_timestamp` where applicable

### Testing

1. **Test bronze models**: Focus on incremental logic and transformations
2. **Use realistic data**: Test fixtures should match actual data patterns
3. **Test edge cases**: NULL values, empty arrays, missing fields
4. **Keep tests small**: Use 1-3 rows per test for clarity

### Performance

1. **Partition on date columns**: Use `INCREMENTAL_BY_TIME_RANGE` for time-series data
2. **Join on indexed columns**: SRA models join on `accession` (grain field)
3. **Materialize views**: Consider `kind FULL` for frequently-queried views
4. **Monitor model runs**: Check execution time and data volume

## Troubleshooting

### DuckDB Lock Error
```
Conflicting lock is held in /home/user/.duckdb/cli/1.4.1/duckdb
```
**Solution**: Close other DuckDB sessions or wait for locks to release

### Model Not Found
```
Model 'bronze.stg_*' not found
```
**Solution**: Check model name matches file name and schema matches directory

### Invalid Time Range
```
Time column 'last_update_date' not found
```
**Solution**: Verify column exists in source and matches `time_column` definition

### File Not Found
```
IO Error: No files found that match the pattern
```
**Solution**: Verify `OMICIDX_DATA_ROOT` is set and files exist at expected paths

## Future Development

### Planned Features

1. **Silver Layer**
   - Cross-dataset joins (SRA ↔ BioSample ↔ GEO ↔ PubMed)
   - Data quality metrics
   - Derived fields and enrichments

2. **Gold/Mart Layer**
   - Publication-ready denormalized tables
   - Organism-specific datasets
   - Time-series aggregations
   - Technology trend analysis

3. **Additional Data Sources**
   - PubMed metadata integration
   - iCite citation metrics
   - dbGaP study metadata

4. **Enhancements**
   - Add tags for model organization
   - Column-level documentation
   - Audit columns (`_loaded_at`, `_batch_id`)
   - Data quality dashboards

## Resources

- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [OmicIDX Project](https://github.com/seandavi/omicidx-gh-etl)

## Contributing

When adding new models:

1. Follow the layer conventions (raw → bronze → silver → gold)
2. Add comprehensive tests
3. Document complex transformations
4. Use consistent naming conventions
5. Update this README with new models and patterns 