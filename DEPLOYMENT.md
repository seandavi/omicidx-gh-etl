# Deployment Guide

This guide covers deploying the OmicIDX data warehouse to Cloudflare R2 (or S3-compatible storage).

## Table of Contents

1. [Overview](#overview)
2. [Configuration](#configuration)
3. [Deployment Commands](#deployment-commands)
4. [Full Deployment Workflow](#full-deployment-workflow)
5. [Upload Tools](#upload-tools)
6. [Remote Database Usage](#remote-database-usage)
7. [Troubleshooting](#troubleshooting)

## Overview

The deployment system provides:

- **Catalog Generation**: Automatic catalog.json generation from schema.yml files
- **Remote Views Database**: Lightweight DuckDB file with views pointing to parquet files
- **R2/S3 Upload**: Upload data, catalog, and database to cloud storage
- **Zero Duplication**: Uses the same export system as local development

### What Gets Deployed

1. **Data files** (`data/`): Parquet files from exported models
2. **Catalog** (`catalog.json`): Metadata about all tables, columns, and files
3. **Database** (`omicidx.duckdb`): Remote views database for querying

## Configuration

### 1. Add Deployment Section to warehouse.yml

```yaml
deployment:
  # R2 endpoint (get from Cloudflare dashboard)
  endpoint_url: https://<account_id>.r2.cloudflarestorage.com

  # API credentials (generate in Cloudflare R2 dashboard)
  access_key_id: your-access-key-id
  secret_access_key: your-secret-access-key

  # Bucket configuration
  bucket_name: your-bucket-name
  region: auto  # For R2, always use 'auto'

  # File organization in bucket
  data_prefix: data           # Where parquet files go
  catalog_path: catalog.json  # Catalog location
  database_path: omicidx.duckdb

  # Public URL - IMPORTANT: Use Cloudflare Worker URL, not R2 direct URL
  # This URL will be used in catalog and remote database views
  # Example: If you have a Cloudflare Worker at https://store.yourdomain.com
  # that mirrors your R2 bucket, use that URL here
  public_url: https://store.yourdomain.com
```

**Why `public_url` Matters**: The remote views database uses this URL to access parquet files. If you have a Cloudflare Worker that mirrors your R2 bucket (providing public HTTPS access), specify that URL here. The views will then use URLs like `https://store.yourdomain.com/data/table.parquet` instead of R2 storage URLs.

**Example with Cloudflare Worker**:
```yaml
deployment:
  # Upload happens to R2 using these credentials
  endpoint_url: https://xxxxx.r2.cloudflarestorage.com
  bucket_name: u24-cancer-genomics

  # But views use the public Worker URL
  public_url: https://store.cancerdatasci.org
  data_prefix: omicidx/data
```

This creates views like:
```sql
CREATE VIEW mart.sra_metadata AS
SELECT * FROM read_parquet('https://store.cancerdatasci.org/omicidx/data/marts/sra_metadata.parquet');
```

Users can then query the remote data without needing R2 credentials:
```bash
duckdb omicidx.duckdb -c "SELECT COUNT(*) FROM mart.sra_metadata"
# Reads from https://store.cancerdatasci.org/ (public Worker URL)
```



### 2. Environment Variable Overrides

All deployment settings can be overridden with environment variables:

```bash
export OMICIDX_R2_ENDPOINT_URL="https://..."
export OMICIDX_R2_ACCESS_KEY_ID="..."
export OMICIDX_R2_SECRET_ACCESS_KEY="..."
export OMICIDX_R2_BUCKET_NAME="your-bucket"
export OMICIDX_R2_REGION="auto"
export OMICIDX_R2_DATA_PREFIX="data"
export OMICIDX_R2_CATALOG_PATH="catalog.json"
export OMICIDX_R2_DATABASE_PATH="omicidx.duckdb"
export OMICIDX_PUBLIC_URL="https://data.yourdomain.com"
```

**Security Best Practice**: Store credentials in environment variables, not in warehouse.yml:

```yaml
# warehouse.yml - no credentials
deployment:
  bucket_name: your-bucket-name
  region: auto
  data_prefix: data
  catalog_path: catalog.json
  database_path: omicidx.duckdb
  public_url: https://data.yourdomain.com
```

```bash
# .env file or shell profile
export OMICIDX_R2_ENDPOINT_URL="..."
export OMICIDX_R2_ACCESS_KEY_ID="..."
export OMICIDX_R2_SECRET_ACCESS_KEY="..."
```

### 3. Verify Configuration

```bash
uv run oidx warehouse show-config
```

Output:
```
Warehouse Configuration:
  Database:      omicidx_warehouse.duckdb
  Models Dir:    omicidx_etl/transformations/models
  Export Dir:    /data/exports
  Threads:       16
  Memory Limit:  32GB

Deployment Configuration:
  Bucket:        your-bucket-name
  Endpoint:      https://...r2.cloudflarestorage.com
  Region:        auto
  Data Prefix:   data
  Catalog Path:  catalog.json
  Database Path: omicidx.duckdb
  Public URL:    https://data.yourdomain.com
```

## Deployment Commands

### Generate Catalog

Creates `catalog.json` with metadata about all tables and exported files:

```bash
uv run oidx warehouse deploy catalog
```

**What it does:**
- Reads all `schema.yml` files from models directory
- Extracts table names, descriptions, columns
- Adds file information (size, format, compression) for exported models
- Generates JSON catalog with URLs (if public_url is configured)

**Output:**
```
============================================================
Catalog generated successfully!
  Tables:     10
  Output:     /data/exports/catalog.json
============================================================
```

**Catalog Structure:**
```json
{
  "version": "1.0",
  "generated_at": "2025-10-30T12:00:00Z",
  "public_url": "https://data.yourdomain.com",
  "tables": {
    "mart.sra_metadata": {
      "name": "sra_metadata",
      "layer": "mart",
      "full_name": "mart.sra_metadata",
      "description": "Production-ready SRA metadata...",
      "columns": [...],
      "materialization": "export_view",
      "export": {
        "path": "marts/sra_metadata.parquet",
        "format": "parquet",
        "compression": "zstd",
        "file_size_bytes": 408299635,
        "file_size_mb": 389.38,
        "url": "https://data.yourdomain.com/data/marts/sra_metadata.parquet"
      }
    }
  }
}
```

### Create Remote Database

Creates a lightweight DuckDB file with views pointing to parquet files:

```bash
uv run oidx warehouse deploy database
```

**What it does:**
- Generates catalog (if not already exists)
- Creates new DuckDB database file
- Creates schemas (raw, staging, mart)
- Creates views for each exported table
- Stores catalog metadata in `_catalog` table

**Output:**
```
============================================================
Remote views database created!
  Database:   /data/exports/omicidx.duckdb
  Views:      1
============================================================
```

**Testing the database:**
```bash
# Query remote database locally
duckdb /data/exports/omicidx.duckdb

# List views
> SHOW TABLES;

# Query a view (reads from parquet file)
> SELECT COUNT(*) FROM mart.sra_metadata;
│ row_count │
├───────────┤
│  49392569 │

# View catalog metadata
> SELECT * FROM _catalog;
```

### Upload Files

Upload catalog, database, and data files to R2:

```bash
# Dry run - see what would be uploaded
uv run oidx warehouse deploy upload --dry-run

# Upload everything
uv run oidx warehouse deploy upload

# Upload selectively
uv run oidx warehouse deploy upload --no-data      # Skip data files
uv run oidx warehouse deploy upload --no-catalog   # Skip catalog
uv run oidx warehouse deploy upload --no-database  # Skip database
```

**Dry run output:**
```
================================================================================
Files to upload to your-bucket-name:
================================================================================

  [data    ] data/marts/sra_metadata.parquet                 (389.38 MB)
  [catalog ] catalog.json                                    (0.02 MB)
  [database] omicidx.duckdb                                  (0.76 MB)

Total: 3 files (390.17 MB)
```

**Upload output:**
```
Uploading files...
  ✓ data/marts/sra_metadata.parquet
  ✓ catalog.json
  ✓ omicidx.duckdb

============================================================
Upload complete:
  Success: 3
  Errors:  0
============================================================
```

### Full Deployment Pipeline

Run all steps in sequence:

```bash
# Full deployment with upload
uv run oidx warehouse deploy all

# Generate catalog + database only (no upload)
uv run oidx warehouse deploy all --skip-upload
```

**Output:**
```
============================================================
Full Deployment Pipeline
============================================================

Step 1/3: Generating catalog...
  ✓ Catalog generated: 10 tables

Step 2/3: Creating remote views database...
  ✓ Database created: 1 views

Step 3/3: Uploading to R2...
  ✓ Upload complete

============================================================
Deployment complete!
============================================================
```

## Full Deployment Workflow

### Typical Workflow

```bash
# 1. Run warehouse transformations with exports
uv run oidx warehouse run

# 2. Test deployment locally (no upload)
uv run oidx warehouse deploy all --skip-upload

# 3. Test remote database locally
duckdb /data/exports/omicidx.duckdb -c "SELECT COUNT(*) FROM mart.sra_metadata"

# 4. Dry run upload to see what will be uploaded
uv run oidx warehouse deploy upload --dry-run

# 5. Deploy to R2
uv run oidx warehouse deploy all
```

### Incremental Updates

After updating models:

```bash
# 1. Re-run specific model to regenerate export
uv run oidx warehouse run --models sra_metadata

# 2. Regenerate catalog (picks up new file size)
uv run oidx warehouse deploy catalog

# 3. Recreate remote database (updates view definitions)
uv run oidx warehouse deploy database

# 4. Upload only the changed files
uv run oidx warehouse deploy upload
```

### Automated Deployment

Example GitHub Actions workflow:

```yaml
name: Deploy Warehouse

on:
  schedule:
    - cron: '0 0 * * *'  # Daily at midnight
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install uv
        run: pip install uv

      - name: Run warehouse transformations
        run: uv run oidx warehouse run

      - name: Deploy to R2
        env:
          OMICIDX_R2_ENDPOINT_URL: ${{ secrets.R2_ENDPOINT_URL }}
          OMICIDX_R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
          OMICIDX_R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
        run: uv run oidx warehouse deploy all
```

## Upload Tools

The deployment system supports two upload tools:

### 1. rclone (Recommended)

**Installation:**
```bash
# macOS
brew install rclone

# Ubuntu/Debian
sudo apt install rclone

# Other platforms: https://rclone.org/downloads/
```

**Configuration:**
```bash
# Configure rclone remote named 'r2'
rclone config

# Or create config file directly: ~/.config/rclone/rclone.conf
[r2]
type = s3
provider = Cloudflare
access_key_id = your-access-key
secret_access_key = your-secret-key
endpoint = https://your-account.r2.cloudflarestorage.com
```

### 2. aws-cli

**Installation:**
```bash
pip install awscli
```

**Configuration:**
The deployment system will use credentials from `warehouse.yml` or environment variables, so no separate AWS CLI configuration is needed.

## Remote Database Usage

Once deployed, users can query your data directly:

### 1. Download Database Only

```bash
# Download the lightweight database file (~1MB)
wget https://your-bucket.r2.dev/omicidx.duckdb

# Query it - data stays remote
duckdb omicidx.duckdb
```

### 2. Query Remote Data

The database contains views that read directly from R2:

```sql
-- Count rows (DuckDB reads from R2)
SELECT COUNT(*) FROM mart.sra_metadata;

-- Filter and aggregate
SELECT
  platform,
  COUNT(*) as experiment_count
FROM mart.sra_metadata
GROUP BY platform
ORDER BY experiment_count DESC
LIMIT 10;

-- View catalog metadata
SELECT
  json_extract_string(catalog, '$.version') as version,
  json_extract_string(catalog, '$.generated_at') as generated_at
FROM _catalog;
```

### 3. Python/R Usage

**Python:**
```python
import duckdb

# Connect to remote database
conn = duckdb.connect('omicidx.duckdb', read_only=True)

# Query remote data
df = conn.execute("""
    SELECT * FROM mart.sra_metadata
    WHERE platform = 'ILLUMINA'
    LIMIT 1000
""").df()

conn.close()
```

**R:**
```r
library(duckdb)

# Connect to remote database
con <- dbConnect(duckdb(), "omicidx.duckdb", read_only=TRUE)

# Query remote data
df <- dbGetQuery(con, "
  SELECT * FROM mart.sra_metadata
  WHERE platform = 'ILLUMINA'
  LIMIT 1000
")

dbDisconnect(con)
```

## Troubleshooting

### Deployment not configured

**Error:**
```
Error: Deployment not configured. Add 'deployment:' section to warehouse.yml
```

**Solution:**
Add `deployment:` section to `warehouse.yml` with at least `bucket_name` configured.

### Upload fails with "command not found"

**Error:**
```
Neither rclone nor aws-cli found. Please install one of them.
```

**Solution:**
Install either rclone or aws-cli (see [Upload Tools](#upload-tools)).

### Remote views fail to query

**Error:**
```
IO Error: No files found that match the pattern "data/marts/sra_metadata.parquet"
```

**Solution:**
The remote database is using local paths instead of R2 URLs. Ensure:
1. `public_url` is configured in `warehouse.yml`
2. Regenerate database: `uv run oidx warehouse deploy database`
3. Database views should now use full URLs: `https://data.yourdomain.com/data/marts/sra_metadata.parquet`

### Upload fails with authentication error

**Error:**
```
Upload failed: The AWS Access Key Id you provided does not exist in our records
```

**Solution:**
1. Verify credentials in Cloudflare R2 dashboard
2. Check `warehouse.yml` or environment variables
3. Ensure credentials are for the correct account
4. Regenerate API tokens if needed

### File already exists in bucket

By default, uploads overwrite existing files. To keep versions, use date-based paths in your export configuration:

```yaml
# In schema.yml
export:
  path: "marts/sra_metadata_{{ date }}.parquet"  # TODO: Implement templating
```

### Large file upload timeout

For very large files, increase timeout or use rclone which handles large files better:

```bash
# Install rclone
brew install rclone

# Configure remote
rclone config

# Uploads will now use rclone automatically
uv run oidx warehouse deploy upload
```

## Next Steps

- See [EXPORT_DEPLOYMENT.md](EXPORT_DEPLOYMENT.md) for export materialization details
- See [README.md](README.md) for warehouse setup and usage
- See [warehouse.yml.example](warehouse.yml.example) for configuration options
