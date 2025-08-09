"""
Simplified SRA extraction without Prefect dependencies.
"""

import tempfile
import gzip
import orjson
import re
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from omicidx.sra.parser import sra_object_generator
from upath import UPath

logger = logging.getLogger(__name__)

# Output format constants
OUTPUT_FORMAT_PARQUET = "parquet"
OUTPUT_FORMAT_NDJSON = "ndjson"

# Batch sizes optimized for 512GB RAM
SRA_BATCH_SIZE = 1_000_000  # Conservative batch size for SRA records


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


def cleanup_old_files(output_dir: Path, pattern: str = "*.ndjson.gz"):
    """Remove old output files matching pattern."""
    for file_path in output_dir.glob(pattern):
        file_path.unlink()
        logger.info(f"Removed old file: {file_path}")


def _generate_output_filename(url: str, output_format: str = OUTPUT_FORMAT_PARQUET) -> str:
    """Generate output filename from SRA URL."""
    url_path = UPath(url)
    path_part = url_path.parts[-2]  # Date directory
    xml_name = url_path.parts[-1]   # XML filename
    
    if output_format == OUTPUT_FORMAT_PARQUET:
        base_name = xml_name.replace(".xml.gz", ".parquet")
    else:
        base_name = xml_name.replace(".xml.gz", ".ndjson.gz")
    
    return f"{path_part}_{base_name}"


def _download_and_process_sra_file(
    url: str, 
    output_dir: Path, 
    output_format: str = OUTPUT_FORMAT_PARQUET
) -> tuple[str, int]:
    """Download and process a single SRA XML file."""
    output_filename = _generate_output_filename(url, output_format)
    output_path = output_dir / output_filename
    
    logger.info(f"Processing {url} -> {output_path}")
    
    record_count = 0
    
    with tempfile.NamedTemporaryFile() as tmpfile:
        # Download
        logger.info(f"Downloading {url}")
        with UPath(url).open("rb") as remote_file:
            tmpfile.write(remote_file.read())
        tmpfile.flush()
        
        # Process and write output
        if output_format == OUTPUT_FORMAT_PARQUET:
            record_count = _write_parquet_file(tmpfile.name, output_path)
        else:
            record_count = _write_ndjson_file(tmpfile.name, output_path)
    
    logger.info(f"Processed {record_count} records from {url}")
    return output_filename, record_count


def _write_ndjson_file(xml_file_path: str, output_path: Path) -> int:
    """Write SRA records to NDJSON file."""
    record_count = 0
    
    with gzip.open(xml_file_path, "rb") as xml_file:
        with gzip.open(output_path, "wb") as output_file:
            for obj in sra_object_generator(xml_file):
                output_file.write(orjson.dumps(obj.data) + b"\n")
                record_count += 1
    
    return record_count


def _write_parquet_file(xml_file_path: str, output_path: Path) -> int:
    """Write SRA records to Parquet file."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("PyArrow not available. Install with: pip install pyarrow")
        raise
    
    records = []
    record_count = 0
    
    with gzip.open(xml_file_path, "rb") as xml_file:
        for obj in sra_object_generator(xml_file):
            records.append(obj.data)
            record_count += 1
    
    if records:
        # Convert to PyArrow table and write
        table = pa.Table.from_pylist(records)
        pq.write_table(
            table, 
            output_path,
            compression='zstd',
            use_dictionary=True
        )
    
    return record_count


def extract_sra(
    output_dir: Path, 
    max_workers: int = 4,
    output_format: str = OUTPUT_FORMAT_PARQUET
) -> dict[str, int]:
    """Extract all SRA data files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean up old files
    pattern = "*.parquet" if output_format == OUTPUT_FORMAT_PARQUET else "*.ndjson.gz"
    cleanup_old_files(output_dir, pattern)
    
    # Get all URLs
    urls = get_sra_urls()
    
    if not urls:
        logger.warning("No SRA URLs found")
        return {}
    
    # Process files in parallel
    results = {}
    total_records = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(_download_and_process_sra_file, url, output_dir, output_format): url 
            for url in urls
        }
        
        # Collect results
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                filename, record_count = future.result()
                results[filename] = record_count
                total_records += record_count
                logger.info(f"Completed {filename}: {record_count} records")
            except Exception as e:
                logger.error(f"Failed to process {url}: {e}")
                results[url] = 0
    
    logger.info(f"SRA extraction completed: {len(results)} files, {total_records} total records")
    return results
