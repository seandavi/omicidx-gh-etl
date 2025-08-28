"""
Simplified SRA extraction without Prefect dependencies.
"""

import tempfile
import gzip
import orjson
import re
import os
from pathlib import Path
import logging
from omicidx.sra.parser import sra_object_generator
from upath import UPath
from ..db import duckdb_connection
import httpx

logger = logging.getLogger(__name__)

# Output format constants
OUTPUT_FORMAT_PARQUET = "parquet"
OUTPUT_FORMAT_NDJSON = "ndjson"

# Block size for chunking parquet files (configurable via environment variable)
SRA_BLOCK_SIZE = int(os.environ.get("SRA_BLOCK_SIZE", "1000000"))  # Default 1M records per chunk


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


def cleanup_old_files(output_dir: Path, url_prefixes: set[str], pattern: str = "*.parquet"):
    """Remove old output files that don't match current URL prefixes."""
    removed_count = 0
    
    for file_path in output_dir.glob(pattern):
        # Extract the prefix from the filename (before the first chunk number)
        filename = file_path.stem
        
        # Check if this file matches any of the current URL prefixes
        matches_current_prefix = False
        for prefix in url_prefixes:
            # Handle both chunked (-0001) and non-chunked files
            if re.match(rf"{re.escape(prefix)}-\d{{4}}$", filename):  # Chunked pattern
                matches_current_prefix = True
                break

        if not matches_current_prefix:
            file_path.unlink()
            logger.info(f"Removed old file: {file_path}")
            removed_count += 1
    
    if removed_count > 0:
        logger.info(f"Cleaned up {removed_count} old files from {output_dir}")


def _generate_file_prefix(url: str) -> str:
    """Generate file prefix from SRA URL (without extension or chunk number)."""
    url_path = UPath(url)
    path_part = url_path.parts[-2]  # Date directory
    xml_name = url_path.parts[-1]   # XML filename
    
    path_part = path_part.replace("NCBI_SRA_Mirroring_","")
    base_name = xml_name.replace("_set.xml.gz","").replace("meta_","")

    return f"{path_part}-{base_name}"


def _generate_output_filename(url: str, chunk_number: int | None = None, output_format: str = OUTPUT_FORMAT_PARQUET) -> str:
    """Generate output filename from SRA URL with optional chunk number."""
    prefix = _generate_file_prefix(url)
    
    if chunk_number is not None:
        return f"{prefix}-{chunk_number:04d}.{output_format}"
    else:
        return f"{prefix}.{output_format}"


def _get_semaphore_filename(prefix: str) -> str:
    """Generate semaphore filename for a given prefix."""
    return f"{prefix}.completed"


def _is_prefix_completed(output_dir: Path, prefix: str) -> bool:
    """Check if a prefix has already been completed by looking for semaphore file."""
    semaphore_file = output_dir / _get_semaphore_filename(prefix)
    return semaphore_file.exists()


def _mark_prefix_completed(output_dir: Path, prefix: str, metadata: dict | None = None) -> None:
    """Create semaphore file to mark prefix as completed."""
    semaphore_file = output_dir / _get_semaphore_filename(prefix)
    
    # Create metadata for the semaphore file
    import time
    semaphore_data = {
        "prefix": prefix,
        "completed_at": time.time(),
        "completed_at_iso": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "block_size": SRA_BLOCK_SIZE,
        "metadata": metadata or {}
    }
    
    try:
        with open(semaphore_file, 'w') as f:
            import json
            json.dump(semaphore_data, f, indent=2)
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
    """Write SRA records to chunked Parquet files."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("PyArrow not available. Install with: pip install pyarrow")
        raise
    
    records = []
    record_count = 0
    chunk_number = 1
    output_filenames = []
    
    logger.info(f"Writing parquet files with block size: {SRA_BLOCK_SIZE}")
    
    def write_chunk():
        """Write current batch of records to a parquet file."""
        nonlocal chunk_number, records, output_filenames
        
        if not records:
            return
            
        output_filename = _generate_output_filename(url, chunk_number)
        output_path = output_dir / output_filename
        
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
