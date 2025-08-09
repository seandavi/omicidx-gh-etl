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
from ..db import duckdb_connection

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


def extract_sra_accessions(output_dir: Path) -> bool:
    """Extract SRA accessions TSV to Parquet using DuckDB."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sra_accessions.parquet"
    
    logger.info("Converting SRA accessions TSV to Parquet")
    
    try:
        with duckdb_connection() as con:
            sql = f"""
            copy 
                (
                    select 
                        * 
                    from 
                        read_csv_auto(
                            'https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab', 
                            delim='\t',
                            nullstr='-'
                        )) 
                to 
                    '{output_path}' 
                    (
                        format parquet, 
                        compression zstd
                    )
            """
            con.execute(sql)
        
        logger.info(f"Created SRA accessions file: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to extract SRA accessions: {e}")
        return False


def extract_sra(
    output_dir: Path, 
    max_workers: int = 4,
    output_format: str = OUTPUT_FORMAT_PARQUET,
    include_accessions: bool = True
) -> dict[str, int]:
    """Extract all SRA data files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean up old files
    pattern = "*.parquet" if output_format == OUTPUT_FORMAT_PARQUET else "*.ndjson.gz"
    cleanup_old_files(output_dir, pattern)
    
    # Extract SRA accessions first if requested
    if include_accessions:
        logger.info("Extracting SRA accessions...")
        extract_sra_accessions(output_dir)
    
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


def get_file_stats(output_dir: Path, output_format: str = OUTPUT_FORMAT_PARQUET) -> dict:
    """Get statistics about extracted SRA files."""
    stats = {}
    extension = "*.parquet" if output_format == OUTPUT_FORMAT_PARQUET else "*.ndjson.gz"
    
    files = sorted(output_dir.glob(extension))
    total_size = sum(f.stat().st_size for f in files)
    
    stats["sra"] = {
        "file_count": len(files),
        "total_size_mb": total_size / (1024 * 1024),
        "files": [f.name for f in files]
    }
    
    return stats


def upload_to_r2(local_files: list[Path], bucket: str = "biodatalake"):
    """Upload SRA files to R2 storage using UPath with s3fs backend."""
    try:
        from upath import UPath
        import s3fs
        import os
    except ImportError as e:
        logger.error(f"Required packages not available: {e}")
        logger.error("Install with: pip install universal-pathlib[s3] s3fs")
        return
    
    # Check for R2 credentials
    required_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_ENDPOINT_URL']
    if not all(var in os.environ for var in required_vars):
        logger.warning(f"R2 credentials not found. Required: {required_vars}")
        return
    
    try:
        # Configure s3fs for R2
        fs = s3fs.S3FileSystem(
            key=os.environ['R2_ACCESS_KEY_ID'],
            secret=os.environ['R2_SECRET_ACCESS_KEY'],
            endpoint_url=os.environ['R2_ENDPOINT_URL'],
            use_ssl=True
        )
        
        # R2 directory path using UPath with configured filesystem
        r2_base = UPath(f"s3://{bucket}/sra/", fs=fs)
        
        # Clean up existing files in R2 directory
        if r2_base.exists():
            logger.info(f"Cleaning up R2 directory: {r2_base}")
            existing_files = list(r2_base.glob("*"))
            
            if existing_files:
                logger.info(f"Deleting {len(existing_files)} existing files from {r2_base}")
                for file_path in existing_files:
                    file_path.unlink()
            else:
                logger.info(f"No existing files found in {r2_base}")
        else:
            logger.info(f"R2 directory {r2_base} does not exist, creating new")
            r2_base.mkdir(parents=True, exist_ok=True)
        
        # Upload new files
        for local_file in local_files:
            r2_file = r2_base / local_file.name
            
            logger.info(f"Uploading {local_file} to R2: {r2_file}")
            r2_file.write_bytes(local_file.read_bytes())
            
    except Exception as e:
        logger.error(f"Failed to upload to R2: {e}")
        logger.info("Verify R2 credentials and endpoint configuration")


def extract_and_upload(
    output_dir: Path, 
    upload: bool = True,
    max_workers: int = 4,
    output_format: str = OUTPUT_FORMAT_PARQUET,
    include_accessions: bool = True
) -> dict[str, int]:
    """Extract all SRA files and optionally upload to R2."""
    results = extract_sra(output_dir, max_workers, output_format, include_accessions)
    
    if upload and results:
        # Get list of generated files
        extension = "*.parquet" if output_format == OUTPUT_FORMAT_PARQUET else "*.ndjson.gz"
        local_files = list(output_dir.glob(extension))
        
        if local_files:
            upload_to_r2(local_files)
    
    return results
