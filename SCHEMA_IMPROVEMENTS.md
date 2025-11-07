# SRA Extract Schema Improvements

## Overview

The [omicidx_etl/sra/extract.py](omicidx_etl/sra/extract.py) file has been refactored to ensure consistent Parquet schemas and eliminate null vs empty list inconsistencies.

## Problems Solved

### 1. **Inconsistent List Field Handling**
**Before**: Some records had `null` for list fields, others had `[]`, and some had missing keys entirely.

**After**: All list fields are consistently `[]` (empty list) when there's no data.

### 2. **Schema Drift Across Chunks**
**Before**: When writing large files in chunks, schema could vary between chunks causing merge issues.

**After**: Hard-coded PyArrow schemas enforce consistency across all chunks.

### 3. **Type Mismatches**
**Before**: Numeric and string fields could be missing or have inconsistent types.

**After**: All fields follow the schema defined in `omicidx.sra.pydantic_models`.

## Key Changes

### 1. Hard-Coded PyArrow Schemas (`get_pyarrow_schemas()`)

Added explicit PyArrow schema definitions for all 4 SRA record types:
- **Run** (15 fields): accession, experiment_accession, files, reads, qualities, etc.
- **Study** (15 fields): accession, title, abstract, identifiers, pubmed_ids, etc.
- **Sample** (11 fields): accession, organism, taxon_id, identifiers, etc.
- **Experiment** (26 fields): accession, platform, library_*, instrument_model, etc.

These schemas are based on the Pydantic models in:
```
.venv/lib/python3.11/site-packages/omicidx/sra/pydantic_models.py
```

### 2. Record Normalization (`normalize_record()`)

New function that ensures:
- **List fields**: Always `[]` (empty list), never `None` or missing
- **Struct fields**: `None` if missing
- **Numeric fields**: `None` if missing (proper nullable handling)
- **String fields**: `None` if missing

Example transformation:
```python
# Before
{
    "accession": "SRP123456",
    "identifiers": None,      # ← Problem
    "attributes": None,       # ← Problem
}

# After normalization
{
    "accession": "SRP123456",
    "identifiers": [],        # ← Consistent
    "attributes": [],         # ← Consistent
}
```

### 3. Schema Enforcement in `_write_parquet_file()`

The parquet writing function now:
1. Infers record type from URL (`infer_record_type_from_url()`)
2. Normalizes each record before appending to batch
3. Writes with explicit schema: `pa.Table.from_pylist(records, schema=target_schema)`

### 4. Code Simplification

- Added comprehensive docstrings with type hints
- Simplified file handling with modern Python patterns
- Improved error messages and logging
- Added top-level module documentation

## Testing

Run the test suite to verify schema enforcement:

```bash
.venv/bin/python test_schema_enforcement.py
```

Expected output:
```
✓ All schemas loaded successfully
✓ Record normalization working
✓ URL inference working
✓ All tests passed!
```

## Usage Example

```python
from omicidx_etl.sra import extract

# Extract SRA data with schema enforcement
results = extract.extract_sra(
    output_dir=Path("./sra_data"),
    max_workers=4,
    output_format="parquet"
)

# All output parquet files will have consistent schemas:
# - No null vs [] inconsistencies
# - All chunks have the same schema
# - Type safety enforced
```

## Schema Reference

### Run Schema (15 fields)
```
Scalar: accession, alias, experiment_accession, title, total_spots, total_bases, size, avg_length
Lists: identifiers, attributes, files, reads, base_counts, qualities
Struct: tax_analysis
```

### Study Schema (15 fields)
```
Scalar: accession, study_accession, alias, title, description, abstract, study_type, center_name, broker_name, BioProject, GEO
Lists: identifiers, attributes, xrefs, pubmed_ids
```

### Sample Schema (11 fields)
```
Scalar: accession, alias, title, organism, description, taxon_id, geo, BioSample
Lists: identifiers, attributes, xrefs
```

### Experiment Schema (26 fields)
```
Scalar: accession, experiment_accession, alias, title, description, design, center_name, study_accession, sample_accession, platform, instrument_model, library_name, library_construction_protocol, library_layout, library_layout_orientation, library_layout_length, library_layout_sdev, library_strategy, library_source, library_selection, spot_length, nreads
Lists: identifiers, attributes, xrefs, reads
```

## Benefits

1. **Reliable Data Pipeline**: No more schema conflicts when merging parquet files
2. **Easier SQL Queries**: Consistent schemas mean predictable null handling in DuckDB/SQL
3. **Better Compression**: Consistent types allow better dictionary encoding
4. **Type Safety**: Explicit schemas catch data issues early
5. **Maintainability**: Clear documentation of expected data structure

## Migration Notes

If you have existing parquet files with inconsistent schemas:

1. **Re-extract**: The safest approach is to re-run extraction with the new code
2. **Schema Validation**: Use `pa.Table.validate()` to check existing files
3. **Normalization Script**: Create a migration script using `normalize_record()` to fix old data

## Related Files

- [omicidx_etl/sra/extract.py](omicidx_etl/sra/extract.py) - Main extraction code
- [test_schema_enforcement.py](test_schema_enforcement.py) - Test suite
- `.venv/.../omicidx/sra/pydantic_models.py` - Source of truth for schema definitions
