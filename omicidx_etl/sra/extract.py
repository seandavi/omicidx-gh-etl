"""
Simplified SRA extraction without Prefect dependencies.

Schema Management:
------------------
This module ensures consistent Parquet schema across all SRA records by:

1. **Hard-coded PyArrow Schemas**: Based on Pydantic models from omicidx.sra.pydantic_models,
   we define explicit schemas for each record type (run, study, sample, experiment).

2. **Record Normalization**: The normalize_record() function ensures:
   - List fields are always [] (empty list), never None or missing
   - Numeric fields have consistent types (int64, float64)
   - Optional string fields are None when missing (not missing keys)

3. **Schema Enforcement**: During parquet writing, records are normalized and validated
   against the target schema to prevent inconsistencies like:
   - Some records having null while others have empty lists
   - Missing fields causing schema inference issues
   - Type mismatches between chunks

Key Functions:
--------------
- get_pyarrow_schemas(): Defines PyArrow schemas for each SRA record type
- normalize_record(): Normalizes a record dict to match the target schema
- infer_record_type_from_url(): Determines record type from URL
- _write_parquet_file(): Writes records with schema enforcement
"""

import tempfile
import gzip
import orjson
import re
import os
from pathlib import Path
from typing import Any
from omicidx.sra.parser import sra_object_generator
from upath import UPath
from ..db import duckdb_connection
import httpx
import click
from loguru import logger

# Output format constants
OUTPUT_FORMAT_PARQUET = "parquet"
OUTPUT_FORMAT_NDJSON = "ndjson"

# Block size for chunking parquet files (configurable via environment variable)
SRA_BLOCK_SIZE = int(os.environ.get("SRA_BLOCK_SIZE", "1000000"))  # Default 1M records per chunk

# PyArrow schema definitions for consistent parquet output
# These schemas are based on the Pydantic models in omicidx.sra.pydantic_models
def get_pyarrow_schemas():
    """Get PyArrow schema definitions for each SRA record type.

    These schemas ensure consistent field types and handle null vs empty list issues.
    """
    try:
        import pyarrow as pa
    except ImportError:
        return {}

    # Common nested types
    identifier_type = pa.struct([
        ("namespace", pa.string()),
        ("id", pa.string()),
        ("uuid", pa.string())  # Only in some identifiers
    ])

    attribute_type = pa.struct([
        ("tag", pa.string()),
        ("value", pa.string())
    ])

    xref_type = pa.struct([
        ("db", pa.string()),
        ("id", pa.string())
    ])

    file_alternative_type = pa.struct([
        ("url", pa.string()),
        ("free_egress", pa.string()),
        ("access_type", pa.string()),
        ("org", pa.string())
    ])

    file_type = pa.struct([
        ("cluster", pa.string()),
        ("filename", pa.string()),
        ("url", pa.string()),
        ("size", pa.int64()),
        ("date", pa.string()),  # Will be parsed as string initially
        ("md5", pa.string()),
        ("sratoolkit", pa.string()),
        ("alternatives", pa.list_(file_alternative_type))
    ])

    run_read_type = pa.struct([
        ("index", pa.int64()),
        ("count", pa.int64()),
        ("mean_length", pa.float64()),
        ("sd_length", pa.float64())
    ])

    base_count_type = pa.struct([
        ("base", pa.string()),
        ("count", pa.int64())
    ])

    quality_type = pa.struct([
        ("quality", pa.int32()),
        ("count", pa.int64())
    ])

    tax_count_entry_type = pa.struct([
        ("rank", pa.string()),
        ("name", pa.string()),
        ("parent", pa.int32()),
        ("total_count", pa.int64()),
        ("self_count", pa.int64()),
        ("tax_id", pa.int32())
    ])

    tax_analysis_type = pa.struct([
        ("nspot_analyze", pa.int64()),
        ("total_spots", pa.int64()),
        ("mapped_spots", pa.int64()),
        ("tax_counts", pa.list_(tax_count_entry_type))
    ])

    experiment_read_type = pa.struct([
        ("base_coord", pa.int64()),
        ("read_class", pa.string()),
        ("read_index", pa.int64()),
        ("read_type", pa.string())
    ])

    # Schema for SRA Run records
    run_schema = pa.schema([
        ("accession", pa.string()),
        ("alias", pa.string()),
        ("experiment_accession", pa.string()),
        ("title", pa.string()),
        ("total_spots", pa.int64()),
        ("total_bases", pa.int64()),
        ("size", pa.int64()),
        ("avg_length", pa.float64()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("files", pa.list_(file_type)),
        ("reads", pa.list_(run_read_type)),
        ("base_counts", pa.list_(base_count_type)),
        ("qualities", pa.list_(quality_type)),
        ("tax_analysis", tax_analysis_type)
    ])

    # Schema for SRA Study records
    study_schema = pa.schema([
        ("accession", pa.string()),
        ("study_accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("abstract", pa.string()),
        ("study_type", pa.string()),
        ("center_name", pa.string()),
        ("broker_name", pa.string()),
        ("BioProject", pa.string()),
        ("GEO", pa.string()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type)),
        ("pubmed_ids", pa.list_(pa.string()))
    ])

    # Schema for SRA Sample records
    sample_schema = pa.schema([
        ("accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("organism", pa.string()),
        ("description", pa.string()),
        ("taxon_id", pa.int32()),
        ("geo", pa.string()),
        ("BioSample", pa.string()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type))
    ])

    # Schema for SRA Experiment records
    experiment_schema = pa.schema([
        ("accession", pa.string()),
        ("experiment_accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("design", pa.string()),
        ("center_name", pa.string()),
        ("study_accession", pa.string()),
        ("sample_accession", pa.string()),
        ("platform", pa.string()),
        ("instrument_model", pa.string()),
        ("library_name", pa.string()),
        ("library_construction_protocol", pa.string()),
        ("library_layout", pa.string()),
        ("library_layout_orientation", pa.string()),
        ("library_layout_length", pa.string()),
        ("library_layout_sdev", pa.string()),
        ("library_strategy", pa.string()),
        ("library_source", pa.string()),
        ("library_selection", pa.string()),
        ("spot_length", pa.int64()),
        ("nreads", pa.int64()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type)),
        ("reads", pa.list_(experiment_read_type))
    ])

    return {
        "run": run_schema,
        "study": study_schema,
        "sample": sample_schema,
        "experiment": experiment_schema
    }

# Initialize schemas at module level
PYARROW_SCHEMAS = get_pyarrow_schemas()


def infer_record_type_from_url(url: str) -> str:
    """Infer the SRA record type from the URL.

    Args:
        url: The SRA XML URL

    Returns:
        Record type: 'run', 'study', 'sample', or 'experiment'
    """
    url_lower = url.lower()
    if "run" in url_lower or "meta_run" in url_lower:
        return "run"
    elif "study" in url_lower or "meta_study" in url_lower:
        return "study"
    elif "sample" in url_lower or "meta_sample" in url_lower:
        return "sample"
    elif "experiment" in url_lower or "meta_experiment" in url_lower:
        return "experiment"
    else:
        logger.warning(f"Could not infer record type from URL: {url}, defaulting to 'run'")
        return "run"


def normalize_record(record: dict[str, Any], record_type: str) -> dict[str, Any]:
    """Normalize a record to ensure consistent schema.

    This function ensures:
    - All list fields are present and contain empty lists [] instead of None
    - All numeric fields have consistent types
    - Missing optional fields are set to None

    Args:
        record: The raw record dict from sra_object_generator
        record_type: The type of record (run, study, sample, experiment)

    Returns:
        Normalized record with consistent schema
    """
    normalized = {}

    # Get the schema for this record type
    schema = PYARROW_SCHEMAS.get(record_type)
    if not schema:
        logger.warning(f"No schema found for record type: {record_type}")
        return record

    # Process each field in the schema
    for field in schema:
        field_name = field.name
        field_value = record.get(field_name)

        # Handle list fields - ensure they're always lists, never None
        if str(field.type).startswith("list"):
            if field_value is None or not isinstance(field_value, list):
                normalized[field_name] = []
            else:
                normalized[field_name] = field_value

        # Handle struct fields - keep as None if missing
        elif str(field.type).startswith("struct"):
            normalized[field_name] = field_value

        # Handle numeric fields - keep as None if missing
        elif "int" in str(field.type) or "float" in str(field.type):
            normalized[field_name] = field_value

        # Handle string fields - keep as None if missing
        else:
            normalized[field_name] = field_value

    return normalized


def get_sra_urls() -> list[str]:
    """Get list of SRA XML URLs from the most recent full mirror and incrementals."""
    u = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring")
    pathlist = sorted(list(u.glob("**/")), reverse=True)
    
    # Find the most recent full mirror
    index = 0
    for path in pathlist:
        index += 1
        match = re.search(r"_Full$", str(path.parent))
        if match is not None:
            recent_dirs = pathlist[:index]
            break
    else:
        recent_dirs = pathlist
    
    all_urls = []
    for parent in recent_dirs:
        p = parent.parent
        urls = list(p.glob("**/*xml.gz"))
        logger.info(f"Found URLs in {p}: {len(urls)}")
        
        for url in urls:
            # Skip meta analysis files
            if url.name == "meta_analysis_set.xml.gz":
                continue
            all_urls.append(str(url))
    
    logger.info(f"Total SRA URLs found: {len(all_urls)}")
    return all_urls


def cleanup_old_files(output_dir: Path, url_prefixes: set[str], pattern: str = "*.parquet") -> int:
    """Remove old output files that don't match current URL prefixes.

    Args:
        output_dir: Directory containing output files
        url_prefixes: Set of valid URL prefixes to keep
        pattern: File pattern to match (default: *.parquet)

    Returns:
        Number of files removed
    """
    removed_count = 0

    for file_path in output_dir.glob(pattern):
        filename = file_path.stem

        # Check if this file matches any of the current URL prefixes
        # Handle both chunked (-0001) and non-chunked files
        matches_current_prefix = any(
            re.match(rf"{re.escape(prefix)}-\d{{4}}$", filename)
            for prefix in url_prefixes
        )

        if not matches_current_prefix:
            file_path.unlink()
            logger.info(f"Removed old file: {file_path}")
            removed_count += 1

    if removed_count > 0:
        logger.info(f"Cleaned up {removed_count} old files from {output_dir}")

    return removed_count


def _generate_file_prefix(url: str) -> str:
    """Generate file prefix from SRA URL (without extension or chunk number).

    Example:
        Input: https://...NCBI_SRA_Mirroring_20250101_Full/meta_study_set.xml.gz
        Output: 20250101_Full-study
    """
    url_path = UPath(url)
    path_part = url_path.parts[-2].replace("NCBI_SRA_Mirroring_", "")
    xml_name = url_path.parts[-1].replace("_set.xml.gz", "").replace("meta_", "")
    return f"{path_part}-{xml_name}"


def _generate_output_filename(
    url: str,
    chunk_number: int | None = None,
    output_format: str = OUTPUT_FORMAT_PARQUET
) -> str:
    """Generate output filename from SRA URL with optional chunk number.

    Args:
        url: The SRA XML URL
        chunk_number: Optional chunk number for large files
        output_format: Output format (parquet or ndjson)

    Returns:
        Filename string (e.g., "20250101_Full-study-0001.parquet")
    """
    prefix = _generate_file_prefix(url)
    extension = output_format if output_format == OUTPUT_FORMAT_PARQUET else "ndjson.gz"

    if chunk_number is not None:
        return f"{prefix}-{chunk_number:04d}.{extension}"
    return f"{prefix}.{extension}"


def _get_semaphore_filename(prefix: str) -> str:
    """Generate semaphore filename for a given prefix."""
    return f"{prefix}.completed"


def _is_prefix_completed(output_dir: Path, prefix: str) -> bool:
    """Check if a prefix has already been completed by looking for semaphore file."""
    return (output_dir / _get_semaphore_filename(prefix)).exists()


def _mark_prefix_completed(output_dir: Path, prefix: str, metadata: dict | None = None) -> None:
    """Create semaphore file to mark prefix as completed.

    Args:
        output_dir: Output directory
        prefix: File prefix
        metadata: Optional metadata dict to store in semaphore file
    """
    import json
    import time

    semaphore_file = output_dir / _get_semaphore_filename(prefix)
    semaphore_data = {
        "prefix": prefix,
        "completed_at": time.time(),
        "completed_at_iso": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "block_size": SRA_BLOCK_SIZE,
        "metadata": metadata or {}
    }

    try:
        semaphore_file.write_text(json.dumps(semaphore_data, indent=2))
        logger.info(f"Created semaphore file: {semaphore_file}")
    except Exception as e:
        logger.warning(f"Failed to create semaphore file {semaphore_file}: {e}")


def _cleanup_orphaned_semaphore_files(output_dir: Path, current_prefixes: set[str]) -> None:
    """Remove semaphore files for prefixes that are no longer in the current URL list."""
    removed_count = 0
    
    for semaphore_file in output_dir.glob("*.completed"):
        # Extract prefix from semaphore filename
        prefix = semaphore_file.stem
        
        if prefix not in current_prefixes:
            semaphore_file.unlink()
            logger.info(f"Removed orphaned semaphore file: {semaphore_file}")
            removed_count += 1
    
    if removed_count > 0:
        logger.info(f"Cleaned up {removed_count} orphaned semaphore files from {output_dir}")


def _download_and_process_sra_file(
    url: str, 
    output_dir: Path, 
    output_format: str = OUTPUT_FORMAT_PARQUET
) -> tuple[list[str], int]:
    """Download and process a single SRA XML file, potentially creating multiple chunked files.
    
    Returns early if the prefix has already been completed (semaphore file exists).
    """
    prefix = _generate_file_prefix(url)
    
    # Check if this prefix has already been completed
    if _is_prefix_completed(output_dir, prefix):
        logger.info(f"Skipping {url} - already completed (semaphore file exists)")
        
        # Read metadata from semaphore file to get accurate results
        semaphore_file = output_dir / _get_semaphore_filename(prefix)
        try:
            import json
            with open(semaphore_file, 'r') as f:
                semaphore_data = json.load(f)
                metadata = semaphore_data.get('metadata', {})
                return metadata.get('filenames', []), metadata.get('record_count', 0)
        except Exception as e:
            logger.warning(f"Failed to read semaphore metadata from {semaphore_file}: {e}")
            # Fallback to file counting
            pattern = f"{prefix}*.{output_format}"
            existing_files = list(output_dir.glob(pattern))
            existing_filenames = [f.name for f in existing_files]
            return existing_filenames, 0  # Unknown record count
    
    logger.info(f"Processing {url}")
    
    record_count = 0
    output_filenames = []
    
    try:
        with tempfile.NamedTemporaryFile() as tmpfile:
            # Download
            logger.info(f"Downloading {url}")
            with httpx.stream("GET", url) as remote_file:
                for chunk in remote_file.iter_bytes():
                    tmpfile.write(chunk)
            tmpfile.flush()
            
            output_filenames, record_count = _write_parquet_file(tmpfile.name, url, output_dir)
        
        # Mark prefix as completed after successful processing
        completion_metadata = {
            "url": url,
            "record_count": record_count,
            "file_count": len(output_filenames),
            "filenames": output_filenames,
            "output_format": output_format
        }
        _mark_prefix_completed(output_dir, prefix, completion_metadata)
        
        logger.info(f"Processed {record_count} records from {url} into {len(output_filenames)} files")
        return output_filenames, record_count
        
    except Exception as e:
        logger.error(f"Failed to process {url}: {e}")
        # Don't create semaphore file on failure
        raise


def _write_ndjson_file(xml_file_path: str, output_path: Path) -> int:
    """Write SRA records to NDJSON file."""
    record_count = 0
    
    with gzip.open(xml_file_path, "rb") as xml_file:
        with gzip.open(output_path, "wb") as output_file:
            for obj in sra_object_generator(xml_file):
                output_file.write(orjson.dumps(obj.data) + b"\n")
                record_count += 1
    
    return record_count


def _write_parquet_file(xml_file_path: str, url: str, output_dir: Path) -> tuple[list[str], int]:
    """Write SRA records to chunked Parquet files with schema enforcement.

    This function:
    1. Infers the record type from the URL
    2. Normalizes each record to ensure consistent schema
    3. Writes records in chunks with the appropriate PyArrow schema
    4. Ensures all records have consistent field types (no null vs empty list issues)
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("PyArrow not available. Install with: pip install pyarrow")
        raise

    # Infer record type from URL
    record_type = infer_record_type_from_url(url)
    target_schema = PYARROW_SCHEMAS.get(record_type)

    if not target_schema:
        logger.warning(f"No schema defined for record type: {record_type}, falling back to untyped")
        target_schema = None
    else:
        logger.info(f"Using schema for record type: {record_type}")

    records = []
    record_count = 0
    chunk_number = 1
    output_filenames = []

    logger.info(f"Writing parquet files with block size: {SRA_BLOCK_SIZE}")

    def write_chunk():
        """Write current batch of records to a parquet file with schema enforcement."""
        nonlocal chunk_number, records, output_filenames

        if not records:
            return

        output_filename = _generate_output_filename(url, chunk_number)
        output_path = output_dir / output_filename

        # Create table with explicit schema if available
        if target_schema:
            try:
                table = pa.Table.from_pylist(records, schema=target_schema)
            except Exception as e:
                logger.warning(f"Failed to apply schema, falling back to inferred: {e}")
                table = pa.Table.from_pylist(records)
        else:
            table = pa.Table.from_pylist(records)

        pq.write_table(
            table,
            output_path,
            compression='zstd',
            use_dictionary=True
        )

        output_filenames.append(output_filename)
        logger.info(f"Wrote chunk {chunk_number}: {output_filename} ({len(records)} records)")

        # Reset for next chunk
        records = []
        chunk_number += 1

    with gzip.open(xml_file_path, "rb") as xml_file:
        for obj in sra_object_generator(xml_file):
            record_count += 1

            # Normalize record to ensure consistent schema
            if target_schema:
                normalized_record = normalize_record(obj.data, record_type)
                records.append(normalized_record)
            else:
                records.append(obj.data)

            # Write chunk when we reach the block size
            if len(records) >= SRA_BLOCK_SIZE:
                write_chunk()

    # Write any remaining records
    if records:
        write_chunk()

    logger.info(f"Created {len(output_filenames)} parquet files with {record_count} total records")
    return output_filenames, record_count




def extract_sra(
    output_dir: Path, 
    max_workers: int = 4,
    output_format: str = OUTPUT_FORMAT_PARQUET,
    include_accessions: bool = True
) -> dict[str, int]:
    """Extract all SRA data files with chunking support and semaphore file tracking."""
    output_dir = Path(output_dir)  # Ensure it's a Path object
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all URLs
    urls = get_sra_urls()
    
    if not urls:
        logger.warning("No SRA URLs found")
        return {}
    
    # Generate prefixes for current URLs to use for cleanup
    current_prefixes = {_generate_file_prefix(url) for url in urls}
    logger.info(f"Generated {len(current_prefixes)} file prefixes for cleanup")
    
    # Clean up old files and semaphore files that don't match current URL prefixes
    cleanup_old_files(output_dir, current_prefixes, f"*.{output_format}")
    _cleanup_orphaned_semaphore_files(output_dir, current_prefixes)
    
    # Count already completed vs remaining work
    completed_prefixes = set()
    for prefix in current_prefixes:
        if _is_prefix_completed(output_dir, prefix):
            completed_prefixes.add(prefix)
    
    remaining_prefixes = current_prefixes - completed_prefixes
    logger.info(f"Found {len(completed_prefixes)} already completed prefixes, {len(remaining_prefixes)} remaining")
    
    # Process files
    results = {}
    total_records = 0
    total_files = 0
    
    for url in urls:
        try:
            output_filenames, record_count = _download_and_process_sra_file(url, output_dir, output_format)
            
            # Store results using the URL prefix as key
            prefix = _generate_file_prefix(url)
            results[prefix] = {
                'record_count': record_count,
                'file_count': len(output_filenames),
                'filenames': output_filenames
            }
            
            total_records += record_count
            total_files += len(output_filenames)
            
        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")
            continue
    
    completed_count = len([p for p in current_prefixes if _is_prefix_completed(output_dir, p)])
    logger.info(f"SRA extraction completed: {total_files} files, {total_records} total records from {len(results)} sources")
    logger.info(f"Semaphore tracking: {completed_count}/{len(current_prefixes)} prefixes completed")
    return results


def get_file_stats(output_dir: Path, output_format: str = OUTPUT_FORMAT_PARQUET) -> dict:
    """Get statistics about extracted SRA files, including chunked files and semaphore files."""
    stats = {}
    extension = "*.parquet" if output_format == OUTPUT_FORMAT_PARQUET else "*.ndjson.gz"
    
    files = sorted(output_dir.glob(extension))
    total_size = sum(f.stat().st_size for f in files)
    
    # Group chunked files by prefix
    grouped_files = {}
    for f in files:
        filename = f.stem
        
        # Check if this is a chunked file (ends with -NNNN)
        if re.match(r".*-\d{4}$", filename):
            # Extract prefix (everything before the last -NNNN)
            prefix = "-".join(filename.split("-")[:-1])
        else:
            prefix = filename
            
        if prefix not in grouped_files:
            grouped_files[prefix] = []
        grouped_files[prefix].append(f.name)
    
    # Get semaphore file information
    semaphore_files = list(output_dir.glob("*.completed"))
    completed_prefixes = {f.stem for f in semaphore_files}
    
    stats["sra"] = {
        "file_count": len(files),
        "total_size_mb": total_size / (1024 * 1024),
        "grouped_files": grouped_files,
        "prefix_count": len(grouped_files),
        "completed_prefixes": sorted(completed_prefixes),
        "semaphore_count": len(semaphore_files),
        "files": [f.name for f in files]
    }
    
    return stats


def list_semaphore_files(output_dir: Path) -> dict[str, dict]:
    """List all semaphore files and their metadata."""
    semaphore_info = {}
    
    for semaphore_file in output_dir.glob("*.completed"):
        prefix = semaphore_file.stem
        
        try:
            with open(semaphore_file, 'r') as f:
                import json
                metadata = json.load(f)
            semaphore_info[prefix] = metadata
        except Exception as e:
            logger.warning(f"Failed to read semaphore file {semaphore_file}: {e}")
            semaphore_info[prefix] = {"error": str(e)}
    
    return semaphore_info


def remove_semaphore_file(output_dir: Path, prefix: str) -> bool:
    """Remove semaphore file for a specific prefix to force re-processing."""
    semaphore_file = output_dir / _get_semaphore_filename(prefix)
    
    if semaphore_file.exists():
        try:
            semaphore_file.unlink()
            logger.info(f"Removed semaphore file: {semaphore_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to remove semaphore file {semaphore_file}: {e}")
            return False
    else:
        logger.warning(f"Semaphore file does not exist: {semaphore_file}")
        return False


def extract_and_upload(
    output_dir: Path,
    upload: bool = True,
    max_workers: int = 4,
    output_format: str = OUTPUT_FORMAT_PARQUET,
    include_accessions: bool = True
) -> dict[str, int]:
    """Extract all SRA files and optionally upload to R2."""
    results = extract_sra(output_dir, max_workers, output_format, include_accessions)

    return results


# CLI Commands

@click.group()
def sra():
    """SRA extraction commands."""
    pass


@sra.command()
@click.argument('output_dir', type=click.Path(path_type=Path))
@click.option('--format', 'output_format',
              type=click.Choice([OUTPUT_FORMAT_PARQUET, OUTPUT_FORMAT_NDJSON]),
              default=OUTPUT_FORMAT_PARQUET,
              help='Output format (default: parquet)')
@click.option('--workers', default=4, help='Number of parallel workers (default: 4)')
@click.option('--include-accessions/--no-accessions', default=True,
              help='Include SRA accessions extraction (default: yes)')
def extract(output_dir: Path, output_format: str, workers: int, include_accessions: bool):
    """Extract SRA data to local files."""
    logger.info(f"Starting SRA extraction to {output_dir}")
    logger.info(f"Format: {output_format}, Workers: {workers}, Accessions: {include_accessions}")

    results = extract_sra(
        output_dir, max_workers=workers,
        output_format=output_format, include_accessions=include_accessions
    )

    return results


@sra.command()
@click.argument('output_dir', type=click.Path(exists=True, path_type=Path))
@click.option('--format', 'output_format',
              type=click.Choice([OUTPUT_FORMAT_PARQUET, OUTPUT_FORMAT_NDJSON]),
              default=OUTPUT_FORMAT_PARQUET,
              help='File format to analyze (default: parquet)')
def stats(output_dir: Path, output_format: str):
    """Show statistics about extracted files."""
    stats_data = get_file_stats(output_dir, output_format)

    for entity, info in stats_data.items():
        click.echo(f"\n{entity.upper()} Files:")
        click.echo(f"  Count: {info['file_count']}")
        click.echo(f"  Total Size: {info['total_size_mb']:.2f} MB")

        if info['files']:
            click.echo("  Files:")
            for filename in info['files'][:10]:  # Show first 10 files
                click.echo(f"    - {filename}")
            if len(info['files']) > 10:
                click.echo(f"    ... and {len(info['files']) - 10} more")
