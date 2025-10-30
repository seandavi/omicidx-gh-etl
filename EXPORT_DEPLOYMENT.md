# Export & Deployment System

## Overview

The OmicIDX warehouse now supports automatic parquet exports with **zero code duplication**. Change your SQL once, and exports update automatically.

## Export Materialization Types

### `export_view`
Creates a view in the warehouse AND exports it to parquet.

```yaml
# schema.yml
models:
  - name: sra_metadata
    materialized: export_view
    export:
      path: "marts/sra_metadata.parquet"
      compression: zstd
```

**Use when**: You want a lightweight view for internal queries + parquet for external distribution.

### `export_table`
Creates a materialized table AND exports it to parquet.

```yaml
# schema.yml
models:
  - name: large_aggregation
    materialized: export_table
    export:
      path: "marts/large_agg.parquet"
```

**Use when**: Query is expensive and you want to cache results internally + export.

## Export Configuration Options

```yaml
export:
  enabled: true                    # Required
  path: "marts/my_model.parquet"   # Optional (defaults to layer/name.parquet)
  format: parquet                  # Optional (default: parquet)
  compression: zstd                # Optional (default: zstd)
  row_group_size: 100000           # Optional (default: 100000)
  partition_by: [column1, column2] # Optional (default: null)
```

### Partitioning Example

```yaml
export:
  enabled: true
  path: "marts/sra_by_type"
  partition_by: [Type]  # Creates sra_by_type/Type=RUN/, Type=STUDY/, etc.
```

## Usage

### Running Exports

```bash
# Run single model - automatically exports
oidx warehouse run --models sra_metadata

# Run all export models
oidx warehouse run  # All models with export_view/export_table materialize

# Dry run to see what would export
oidx warehouse plan
```

### Export Output

```
exports/
├── raw/
│   └── sra_accessions.parquet
├── staging/
│   ├── stg_sra_studies.parquet
│   └── stg_sra_experiments.parquet
└── marts/
    └── sra_metadata.parquet
```

## Workflow: Development to Deployment

### 1. Development (Local)

```bash
# Add/update model
vim models/mart/my_new_view.sql

# Configure export
vim models/mart/schema.yml
```

```yaml
models:
  - name: my_new_view
    materialized: export_view
    export:
      path: "marts/my_new_view.parquet"
```

```bash
# Run warehouse (creates view + exports)
oidx warehouse run --models my_new_view

# Output:
# - mart.my_new_view view in warehouse.duckdb
# - exports/marts/my_new_view.parquet file
```

### 2. Deployment (TODO - Next Phase)

```bash
# Upload exports to R2
rclone sync exports/ r2:omicidx/data/

# Generate catalog from schema.yml
oidx deploy generate-catalog

# Create remote views database
oidx deploy create-remote-db --r2-url https://pub-xxx.r2.dev/data

# Upload database + metadata
rclone copy omicidx_readonly.duckdb r2:omicidx/databases/
rclone copy catalog.json r2:omicidx/metadata/
```

### 3. End-User Consumption

**Option A: Download lightweight DB**
```python
import duckdb

# Download tiny DB file with views (~1MB)
conn = duckdb.connect('omicidx_readonly.duckdb')

# Queries read directly from R2 parquet
df = conn.execute("""
    SELECT * FROM sra_metadata
    WHERE organism = 'Homo sapiens'
    LIMIT 100
""").df()
```

**Option B: Direct parquet access**
```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT * FROM read_parquet('https://pub-xxx.r2.dev/data/marts/sra_metadata.parquet')
    WHERE organism = 'Homo sapiens'
""").df()
```

## Maintenance Comparison

### ❌ Old Way (Separate Export Models)

```
models/
├── mart/sra_metadata.sql           # Warehouse internal
└── export/export_sra_metadata.sql  # DUPLICATE LOGIC
```

**Problems**:
- Change SQL → must update 2 files
- Easy to forget exports
- 2x files to maintain

### ✅ New Way (Export Materialization)

```
models/
└── mart/sra_metadata.sql           # Single source of truth
```

```yaml
# schema.yml
models:
  - name: sra_metadata
    materialized: export_view  # ← Just change this!
    export:
      path: "marts/sra_metadata.parquet"
```

**Benefits**:
- Change SQL once
- Exports automatic
- 1 file to maintain

## Implementation Details

### Export Process

1. **Model execution** (`oidx warehouse run`)
2. **Create view/table** in warehouse (for internal queries)
3. **Export to parquet** (if export_view/export_table)
4. **Track metadata** (execution time, export path)

### Code Flow

```python
# warehouse.py
def execute_model(conn, model, config):
    # 1. Create view/table
    if model.materialization == 'export_view':
        conn.execute(f"CREATE OR REPLACE VIEW {model.layer}.{model.name} AS {sql}")

        # 2. Export it
        export_path = _export_model(conn, model, config)

    return {'status': 'success', 'export_path': export_path}
```

### Export Helper

```python
def _export_model(conn, model, config):
    export_path = Path(config.export_dir) / model.export.path

    conn.execute(f"""
        COPY {model.layer}.{model.name}
        TO '{export_path}' (
            FORMAT {model.export.format},
            COMPRESSION {model.export.compression},
            ROW_GROUP_SIZE {model.export.row_group_size}
        )
    """)

    return export_path
```

## Verified Working

### Example: `sra_metadata` export

```bash
oidx warehouse run --models sra_metadata
```

**Results**:
- ✅ Created `mart.sra_metadata` view
- ✅ Exported to `exports/marts/sra_metadata.parquet`
- ✅ File size: 390MB
- ✅ Row count: 49,392,569
- ✅ Execution time: 6.9 seconds

## Next Steps

### Phase 2: Deployment CLI (TODO)

```bash
oidx deploy upload --bucket omicidx      # Upload to R2
oidx deploy catalog                       # Generate catalog.json
oidx deploy create-remote-db              # Create views DB
oidx deploy publish                       # All-in-one command
```

### Phase 3: Catalog & Metadata (TODO)

Generate `catalog.json` from schema.yml:

```json
{
  "name": "OmicIDX",
  "version": "2025-10-30",
  "tables": [
    {
      "name": "sra_metadata",
      "layer": "mart",
      "path": "data/marts/sra_metadata.parquet",
      "r2_url": "https://pub-xxx.r2.dev/data/marts/sra_metadata.parquet",
      "rows": 49392569,
      "size_bytes": 408944640,
      "schema": {...},
      "updated": "2025-10-30T11:32:33Z"
    }
  ]
}
```

### Phase 4: Remote Views Database (TODO)

Auto-generate lightweight DuckDB file:

```python
# deploy/create_remote_db.py
def create_remote_views_db(catalog, output='omicidx_readonly.duckdb'):
    conn = duckdb.connect(output)

    for table in catalog['tables']:
        conn.execute(f"""
            CREATE VIEW {table['name']} AS
            SELECT * FROM read_parquet('{table['r2_url']}')
        """)
```

## Best Practices

### When to Export

**Export these**:
- ✅ Mart views (production-ready analytics)
- ✅ Staging tables (cleaned, validated data)
- ✅ Large aggregations (pre-computed for performance)

**Don't export these**:
- ❌ Raw views (users can read your parquet directly)
- ❌ Intermediate transformations (internal only)
- ❌ Temporary working tables

### Naming Conventions

```yaml
# Good export paths
export:
  path: "marts/sra_metadata.parquet"           # Clear, descriptive
  path: "staging/cleaned_studies.parquet"      # Shows layer + purpose
  path: "marts/by_type/studies.parquet"        # Organized by category

# Avoid
export:
  path: "output.parquet"                       # Not descriptive
  path: "temp/test.parquet"                    # Implies temporary
```

### Partitioning Guidelines

**Partition when**:
- File > 1GB
- Clear partition key (Type, date, organism)
- Users often filter by that key

```yaml
export:
  path: "marts/sra_by_type"
  partition_by: [Type]  # Creates RUN/, STUDY/, etc subdirs
```

**Don't partition when**:
- File < 500MB
- No clear partition key
- Users query across all partitions

## Configuration Reference

### WarehouseConfig

```python
config = WarehouseConfig(
    db_path='omicidx_warehouse.duckdb',
    models_dir='omicidx_etl/transformations/models',
    export_dir='exports',  # Where exports are written
    threads=16,
    memory_limit='32GB'
)
```

### ExportConfig

```python
@dataclass
class ExportConfig:
    enabled: bool = False
    path: Optional[str] = None
    format: str = 'parquet'
    compression: str = 'zstd'  # or 'snappy', 'gzip', 'none'
    partition_by: Optional[List[str]] = None
    row_group_size: int = 100000
```

## Troubleshooting

### Export not happening

Check materialization type:
```yaml
materialized: export_view  # ← Must be export_view or export_table
```

### Wrong export path

Check export configuration:
```yaml
export:
  enabled: true
  path: "correct/path/here.parquet"  # ← Relative to export_dir
```

### Partition errors

Ensure partition columns exist:
```yaml
export:
  partition_by: [Type]  # ← Column must exist in SELECT
```

### File already exists

Exports overwrite by default. To keep versions:
```yaml
export:
  path: "marts/sra_metadata_{{ date }}.parquet"  # TODO: Add templating
```

## Summary

**What we built**:
- ✅ Zero-duplication export system
- ✅ Two new materialization types (export_view, export_table)
- ✅ Automatic exports during warehouse runs
- ✅ Configurable per-model via schema.yml

**What's next**:
- 🔄 Deployment CLI commands
- 🔄 Catalog generator
- 🔄 Remote views database creator
- 🔄 Documentation generator

**Key benefit**: **Change SQL once, exports update automatically!**
