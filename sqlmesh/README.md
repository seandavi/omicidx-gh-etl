# SQLMesh Configuration for OmicIDX

This directory contains SQLMesh models and configuration for the OmicIDX data warehouse.

## Quick Start

### 1. Configure Data Paths

SQLMesh uses environment variables for configurable paths. Create a `.env` file:

```bash
cp .env.example .env
# Edit .env to set your data root path
```

Or set the environment variable directly:

```bash
export OMICIDX_DATA_ROOT=/data/davsean/omicidx_root
```

### 2. Run SQLMesh Commands

```bash
# View execution plan
uv run sqlmesh plan

# Apply changes
uv run sqlmesh plan --auto-apply

# Run tests
uv run sqlmesh test

# Render a model to see compiled SQL
uv run sqlmesh render bronze.src_geo_samples
```

## Configuration

### Variables

The `config.yaml` defines variables that can be used in models:

```yaml
variables:
  data_root: ${OMICIDX_DATA_ROOT:-/data/davsean/omicidx_root}
```

In models, reference with `@variable_name`:

```sql
SELECT * FROM read_ndjson_auto(@data_root || '/geo/gsm*.ndjson.gz')
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OMICIDX_DATA_ROOT` | `/data/davsean/omicidx_root` | Base path for all source data files |

## Model Structure

```
models/
├── bronze/
│   ├── src_geo_samples.sql      # VIEW over source NDJSON files
│   └── stg_geo_samples.sql      # Incremental staging table
└── ...
```

### Bronze Layer Models

**Source Models (src_*)**: VIEWs that read directly from parquet/NDJSON files
- `bronze.src_geo_samples` - GEO samples from `geo/gsm*.ndjson.gz`
- `bronze.src_geo_series` - GEO series (future)
- `bronze.src_geo_platforms` - GEO platforms (future)

**Staging Models (stg_*)**: Incremental tables that materialize changes
- Use `INCREMENTAL_BY_TIME_RANGE` with `last_update_date`
- Only process rows updated since last run
- Add data quality flags and transformations

## Testing

Tests are defined in YAML files in `tests/`:

```yaml
test_stg_geo_samples:
  model: bronze.stg_geo_samples
  inputs:
    bronze.src_geo_samples:
      columns: {...}
      rows: [...]
  vars:
    start_ds: 2005-05-28
    end_ds: 2005-05-28
  outputs:
    query:
      rows: [...]
```

Run tests:
```bash
uv run sqlmesh test
```

## Linting

The project uses SQLMesh's built-in linter with custom rules (see `config.yaml`):

- `invalidselectstarexpansion` - Ensures `SELECT *` can be expanded
- `ambiguousorinvalidcolumn` - Disabled for dynamic file sources

## Gateways

Two DuckDB gateways are configured:

1. **db** (default) - Standard DuckDB database
2. **ducklake** - DuckLake with ACID support and time-travel

## Relocating Data

To move data to a new location:

1. Update the environment variable:
   ```bash
   export OMICIDX_DATA_ROOT=/new/path
   ```

2. Or update `.env` file:
   ```
   OMICIDX_DATA_ROOT=/new/path
   ```

3. Re-run plan:
   ```bash
   uv run sqlmesh plan
   ```

All models using `@data_root` will automatically use the new path.

## TODOS

- [ ] Implement src and stg models for sra (parquet files at /data/davsean/omicidx_root/sra/)
  - [ ] runs
  - [ ] experiments
  - [ ] samples
  - [ ] studies
  - [ ] sra_accessiona is from `https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab` but note that this file is very large and may require special handling.

- [ ] Implement src and stg models for biosample and bioproject (parquet files at /data/davsean/omicidx_root/biosample/)
  - [ ] biosample
  - [ ] bioproject 