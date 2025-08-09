# SRA ETL Module

This module provides simplified SRA (Sequence Read Archive) data extraction without Prefect dependencies.

## Quick Start

### CLI Usage

```bash
# Extract SRA data to Parquet files (default)
omicidx-etl sra extract /path/to/output

# Extract to NDJSON format with 8 workers
omicidx-etl sra extract /path/to/output --format ndjson --workers 8

# Extract and upload to R2 in one step
omicidx-etl sra extract /path/to/output --upload

# Extract without SRA accessions file
omicidx-etl sra extract /path/to/output --no-accessions

# Extract only the SRA accessions file
omicidx-etl sra accessions /path/to/output

# Show statistics about extracted files
omicidx-etl sra stats /path/to/output

# Upload existing files to R2
omicidx-etl sra upload /path/to/output

# Clean up old files
omicidx-etl sra clean /path/to/output
```

### Python API

```python
from pathlib import Path
from omicidx_etl.sra.extract import extract_sra, get_file_stats

# Extract SRA data
output_dir = Path("./sra_data")
results = extract_sra(
    output_dir=output_dir,
    max_workers=4,
    output_format="parquet",  # or "ndjson"
    include_accessions=True   # default: True
)

# Extract only accessions
from omicidx_etl.sra.extract import extract_sra_accessions
success = extract_sra_accessions(output_dir)

# Get statistics
stats = get_file_stats(output_dir)
print(f"Extracted {stats['sra']['file_count']} files")
```

## Features

### Output Formats
- **Parquet**: Default format, optimized for DuckDB analytics
- **NDJSON**: Gzipped newline-delimited JSON for legacy compatibility

### Processing
- **Parallel Processing**: Configurable worker threads for faster extraction
- **Memory Efficient**: Uses temporary files and streaming processing
- **URL Discovery**: Automatically finds current and incremental SRA mirror files
- **SRA Accessions**: Included by default, uses DuckDB for efficient TSV→Parquet conversion

### R2 Storage Integration
- **Clean Upload**: Removes old files before uploading new ones
- **UPath Integration**: Uses modern Python path interface for cloud storage
- **Zero Egress**: Optimized for Cloudflare R2 to avoid transfer costs

## Configuration

### Environment Variables for R2 Upload

```bash
export R2_ACCESS_KEY_ID="your_access_key"
export R2_SECRET_ACCESS_KEY="your_secret_key"  
export R2_ENDPOINT_URL="https://account-id.r2.cloudflarestorage.com"
```

### Required Dependencies

```bash
# For Parquet output
pip install pyarrow

# For R2 upload
pip install universal-pathlib[s3] s3fs
```

## Migration from Legacy ETL

### Before (Prefect-based)
```python
from omicidx_etl.sra.etl import sra_get_urls

# Complex Prefect flow with tasks, caching, etc.
sra_get_urls()
```

### After (Simplified)
```python
from omicidx_etl.sra.extract import extract_sra

# Simple function call
results = extract_sra(output_dir)
```

### Key Differences
- ✅ **No Prefect**: Eliminates infrastructure overhead
- ✅ **Direct Parquet**: Skip NDJSON intermediate step
- ✅ **Local Optimized**: Designed for high-memory local servers
- ✅ **Flexible Output**: Choose format based on downstream needs
- ✅ **Simple CLI**: Easy automation and scripting

## File Organization

Output files follow this pattern:
```
output_dir/
├── 20250101_experiment_set.parquet
├── 20250101_run_set.parquet
├── 20250101_sample_set.parquet
├── 20250101_study_set.parquet
├── 20250102_experiment_set.parquet
├── sra_accessions.parquet
    ...
```

Where `20250101` represents the mirror date directory from NCBI.

## Performance

Optimized for local servers with high RAM:
- **Default Workers**: 4 parallel processes
- **Memory Usage**: Processes one file at a time per worker
- **Local Server**: Designed for 512GB RAM, 64-core systems
- **Batch Processing**: Efficient handling of large SRA XML files

## Troubleshooting

### Common Issues

1. **Missing PyArrow**: Install with `pip install pyarrow`
2. **R2 Upload Fails**: Check environment variables and credentials
3. **Memory Issues**: Reduce `max_workers` parameter
4. **Network Timeouts**: SRA files are large, ensure stable connection

### Logging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Future Enhancements

- [ ] Resume interrupted downloads
- [ ] Incremental processing (skip unchanged files)
- [ ] Integration with DuckDB for direct loading
- [ ] Compression ratio optimization
- [ ] Progress reporting for large extractions
