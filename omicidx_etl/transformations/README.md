# Transformations

This directory contains modular transformation functions for OmicIDX data sources.

## Architecture

Transformations are organized by data source into separate modules:

```
transformations/
├── __init__.py
├── sra.py          # SRA transformations
├── biosample.py    # Biosample/Bioproject transformations
├── geo.py          # GEO transformations
└── README.md       # This file
```

Each module contains focused functions that:
- Accept a DuckDB connection and directory paths as parameters
- Perform a specific transformation task
- Return results (row counts, status, etc.)
- Handle errors gracefully with logging

## Current Transformations

### SRA (sra.py)

**Entity Consolidations:**
- `consolidate_entities()` - Consolidates chunked parquet files into:
  - `studies.parquet`
  - `experiments.parquet`
  - `samples.parquet`
  - `runs.parquet`

**Summaries:**
- `create_study_summary()` - Creates `sra_study_summary.parquet` with experiment/run counts per study

### Biosample (biosample.py)

**Entity Consolidations:**
- `consolidate_biosamples()` - NCBI biosamples → `biosamples.parquet`
- `consolidate_bioprojects()` - NCBI bioprojects → `bioprojects.parquet`
- `consolidate_ebi_biosamples()` - EBI biosamples (NDJSON → parquet) → `ebi_biosamples.parquet`

### GEO (geo.py)

**Entity Consolidations:**
- `consolidate_gse()` - GEO series (GSE) → `geo_series.parquet`
- `consolidate_gsm()` - GEO samples (GSM) → `geo_samples.parquet`
- `consolidate_gpl()` - GEO platforms (GPL) → `geo_platforms.parquet`

## Adding New Transformations

### Step 1: Add Function to Appropriate Module

Add your transformation function to the relevant module (`sra.py`, `biosample.py`, `geo.py`):

```python
# In omicidx_etl/transformations/sra.py

def create_experiment_summary(con, output_dir: Path) -> int:
    """
    Create experiment-level summary with run statistics.

    Args:
        con: DuckDB connection
        output_dir: Directory for output files

    Returns:
        Number of experiments in summary
    """
    logger.info("Creating experiment summary")

    output_path = output_dir / "experiment_summary.parquet"

    try:
        con.execute(f"""
            COPY (
                SELECT
                    experiment_accession,
                    COUNT(DISTINCT run_accession) as run_count,
                    SUM(total_bases) as total_bases,
                    AVG(total_bases) as avg_bases_per_run,
                    MIN(published) as first_published,
                    MAX(published) as last_published
                FROM read_parquet('{output_dir}/runs.parquet')
                GROUP BY experiment_accession
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        row_count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created experiment_summary.parquet with {row_count:,} experiments")
        return row_count

    except Exception as e:
        logger.error(f"Failed to create experiment summary: {e}")
        return 0
```

### Step 2: Call from Main Orchestrator

Add the function call to `omicidx_etl/transform.py` in the appropriate stage:

```python
# In run_all_transformations()

# Stage 1: SRA Transformations
try:
    sra_results = sra.consolidate_entities(con, extract_dir, output_dir)
    all_results["sra_consolidation"] = sra_results

    if sra_results and any(sra_results.values()):
        # Existing summary
        sra_summary_count = sra.create_study_summary(con, output_dir)
        all_results["sra_study_summary"] = sra_summary_count

        # NEW: Add your transformation here
        experiment_summary_count = sra.create_experiment_summary(con, output_dir)
        all_results["sra_experiment_summary"] = experiment_summary_count

except Exception as e:
    logger.error(f"SRA transformations failed: {e}")
    all_results["sra_consolidation"] = {"error": str(e)}
```

### Step 3: Update Summary Output (Optional)

If you want your new transformation to appear in the summary output, add it to the logging section:

```python
# In run_all_transformations(), in the summary section

# SRA
logger.info("\nSRA:")
if "sra_consolidation" in all_results:
    sra_results = all_results["sra_consolidation"]
    if isinstance(sra_results, dict) and "error" not in sra_results:
        for entity, count in sra_results.items():
            if count:
                logger.info(f"  {entity}: {count:,} rows")

if "sra_study_summary" in all_results:
    logger.info(f"  study_summary: {all_results['sra_study_summary']:,} rows")

# NEW: Add your summary here
if "sra_experiment_summary" in all_results:
    logger.info(f"  experiment_summary: {all_results['sra_experiment_summary']:,} rows")
```

## Example: Cross-Dataset Join

For transformations that join multiple datasets, create a new module or add to an existing one:

```python
# In omicidx_etl/transformations/sra.py (or create cross_dataset.py)

def join_sra_with_biosample(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Join SRA samples with biosample metadata.

    Creates enriched_samples.parquet with organism, tissue type, etc.
    """
    logger.info("Joining SRA with biosample data")

    samples_path = output_dir / "samples.parquet"
    biosample_path = output_dir / "biosamples.parquet"
    output_path = output_dir / "enriched_samples.parquet"

    # Check required files exist
    if not all([samples_path.exists(), biosample_path.exists()]):
        logger.warning("Missing required files for join")
        return 0

    try:
        con.execute(f"""
            COPY (
                SELECT
                    s.*,
                    b.organism,
                    b.tissue_type,
                    b.cell_type,
                    b.strain
                FROM read_parquet('{samples_path}') s
                LEFT JOIN read_parquet('{biosample_path}') b
                    ON s.biosample_accession = b.accession
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        row_count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created enriched_samples.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to join SRA with biosample: {e}")
        return 0
```

## Guidelines

### Function Signature

All transformation functions should follow this pattern:

```python
def transformation_name(con, extract_dir: Path, output_dir: Path, **kwargs) -> ReturnType:
    """
    Brief description of what this transformation does.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted/raw data
        output_dir: Directory for transformed output files
        **kwargs: Any additional parameters

    Returns:
        Row count, status, or dictionary with results
    """
```

### Error Handling

Always handle errors gracefully and log them:

```python
try:
    # Transformation logic here
    logger.info(f"✓ Created {output_file}")
    return row_count

except Exception as e:
    logger.error(f"Failed to create {output_file}: {e}")
    return 0  # or None, or {"error": str(e)}
```

### Check for Required Files

Before running transformations that depend on other files, check they exist:

```python
required_files = [
    output_dir / "studies.parquet",
    output_dir / "experiments.parquet"
]

if not all(f.exists() for f in required_files):
    logger.warning("Missing required files for transformation")
    return 0
```

### Use ZSTD Compression

Always use ZSTD compression for parquet output:

```python
con.execute(f"""
    COPY (...) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

### Log Progress

Use informative logging at each step:

```python
logger.info("Creating summary table...")
# ... transformation ...
logger.info(f"✓ Created summary with {row_count:,} rows")
```

## Testing Transformations

Test individual transformation functions from a Python REPL:

```python
from pathlib import Path
from omicidx_etl.db import duckdb_connection
from omicidx_etl.transformations import sra

extract_dir = Path("/data/davsean/omicidx_root")
output_dir = Path("/tmp/test_output")
output_dir.mkdir(exist_ok=True)

with duckdb_connection() as con:
    results = sra.consolidate_entities(con, extract_dir, output_dir)
    print(results)
```

Or test via CLI:

```bash
# Test specific source
uv run oidx transform consolidate --source sra --output-dir /tmp/test

# Test all transformations
uv run oidx transform run --output-dir /tmp/test
```

## Common Patterns

### Simple Consolidation

Combine chunked files into one:

```python
pattern = str(extract_dir / "source/*entity*.parquet")
output_path = output_dir / "consolidated.parquet"

con.execute(f"""
    COPY (SELECT * FROM read_parquet('{pattern}'))
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

### Aggregation

Create summary tables:

```python
con.execute(f"""
    COPY (
        SELECT
            group_column,
            COUNT(*) as count,
            SUM(value) as total
        FROM read_parquet('{input_path}')
        GROUP BY group_column
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

### Join

Combine multiple datasets:

```python
con.execute(f"""
    COPY (
        SELECT a.*, b.extra_field
        FROM read_parquet('{table_a_path}') a
        LEFT JOIN read_parquet('{table_b_path}') b
            ON a.key = b.key
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

### Filtering

Select subset of data:

```python
con.execute(f"""
    COPY (
        SELECT *
        FROM read_parquet('{input_path}')
        WHERE condition = true
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

## File Naming Convention

- Consolidated entities: plural lowercase (e.g., `studies.parquet`, `biosamples.parquet`)
- Summaries: `{entity}_summary.parquet` (e.g., `study_summary.parquet`)
- Enriched data: `enriched_{entity}.parquet` (e.g., `enriched_samples.parquet`)
- Cross-dataset joins: `{source1}_{source2}_joined.parquet`

## Operational Concerns

### Schema Variations with union_by_name

When consolidating chunked files that may have schema variations (different columns across chunks), use `union_by_name=True` in DuckDB's `read_parquet()` function:

```python
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet('{pattern}', union_by_name=true)
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
```

**Why this is needed:**
- SRA data in particular can have schema evolution across time periods
- Different extraction batches may have different columns
- `union_by_name=true` handles these variations by:
  - Aligning columns by name across files
  - Filling missing columns with NULL values
  - Allowing consolidation to succeed despite schema differences

**When to use:**
- SRA entity consolidations (studies, experiments, samples, runs)
- Any dataset where schema may change over time
- When consolidating files from different time periods or extraction runs

### DuckDB Temp Directory Management

DuckDB can use significant temporary disk space during large operations. The default `/tmp` directory may fill up, causing failures.

**Solution: Custom temp directory**

The transformation orchestrator (`transform.py`) creates a custom temp directory in the data directory:

```python
# Create temp directory for DuckDB (avoids /tmp space issues)
temp_dir = extract_dir / "duckdb_temp"
temp_dir.mkdir(parents=True, exist_ok=True)

try:
    with duckdb_connection(temp_directory=str(temp_dir)) as con:
        # Run transformations
        pass
finally:
    # Clean up temp directory
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
```

**Key points:**
- Custom temp directory should be on a filesystem with sufficient space
- The data directory typically has more space than `/tmp`
- Always clean up temp directory after completion (use try/finally)
- Monitor disk space if working with very large datasets

**DuckDB configuration:**
The `duckdb_connection()` context manager accepts a `temp_directory` parameter:

```python
from omicidx_etl.db import duckdb_connection

temp_dir = Path("/data/tmp")  # Use directory with sufficient space
temp_dir.mkdir(parents=True, exist_ok=True)

with duckdb_connection(temp_directory=str(temp_dir)) as con:
    # Your transformation logic
    pass
```

### Memory Settings

DuckDB is configured with these memory limits (in `db.py`):

```sql
SET memory_limit='16GB';
SET max_temp_directory_size='100GB';
```

For very large transformations, you may need to:
- Increase `memory_limit` if you have more RAM available
- Increase `max_temp_directory_size` and ensure sufficient disk space
- Process data in smaller batches if hitting limits

## Questions?

If you're unsure where a transformation should go:
- **Single data source** → Add to that source's module (sra.py, biosample.py, geo.py)
- **Multiple data sources** → Consider creating `cross_dataset.py` module
- **General utilities** → Consider creating `utils.py` module

For more complex transformations or questions, refer to existing functions in this directory as examples.
