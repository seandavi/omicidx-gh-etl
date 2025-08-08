"""
Simplified biosample/bioproject extraction without Prefect dependencies.
"""

import tempfile
import gzip
import orjson
from pathlib import Path
import urllib.request
import logging
from omicidx.biosample import BioSampleParser, BioProjectParser

logger = logging.getLogger(__name__)

# Configuration
BIO_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIO_PROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"

# Batch sizes optimized for your 512GB RAM
BIOSAMPLE_BATCH_SIZE = 2_000_000  # Much larger than current 1M
BIOPROJECT_BATCH_SIZE = 500_000   # Much larger than current 100k


def cleanup_old_files(output_dir: Path, entity: str):
    """Remove old output files for an entity."""
    for file_path in output_dir.glob(f"{entity}*.ndjson.gz"):
        file_path.unlink()
        logger.info(f"Removed old file: {file_path}")


def extract_biosample(output_dir: Path) -> list[Path]:
    """Extract biosample data to NDJSON files."""
    return _extract_entity(
        url=BIO_SAMPLE_URL,
        entity="biosample", 
        output_dir=output_dir,
        batch_size=BIOSAMPLE_BATCH_SIZE,
        parser_class=BioSampleParser,
        use_gzip_input=True
    )


def extract_bioproject(output_dir: Path) -> list[Path]:
    """Extract bioproject data to NDJSON files."""
    return _extract_entity(
        url=BIO_PROJECT_URL,
        entity="bioproject",
        output_dir=output_dir, 
        batch_size=BIOPROJECT_BATCH_SIZE,
        parser_class=BioProjectParser,
        use_gzip_input=False
    )


def _extract_entity(
    url: str, 
    entity: str, 
    output_dir: Path, 
    batch_size: int,
    parser_class,
    use_gzip_input: bool
) -> list[Path]:
    """Extract a single entity type."""
    output_dir.mkdir(parents=True, exist_ok=True)
    cleanup_old_files(output_dir, entity)
    
    logger.info(f"Downloading {url}")
    
    output_files = []
    
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        
        obj_counter = 0
        file_counter = 0
        current_output = None
        
        def _finalize_current_file():
            nonlocal current_output, file_counter, obj_counter, output_files
            if current_output:
                current_output.close()
                file_counter += 1
                obj_counter = 0
        
        # Open input file
        open_func = gzip.open if use_gzip_input else open
        mode = "rb"
        
        with open_func(tmpfile.name, mode) as input_file:
            for obj in parser_class(input_file, validate_with_schema=False):
                
                # Start new output file if needed
                if obj_counter % batch_size == 0:
                    if current_output:
                        _finalize_current_file()
                    
                    output_path = output_dir / f"{entity}-{file_counter:06}.ndjson.gz"
                    current_output = gzip.open(output_path, "wb")
                    output_files.append(output_path)
                    logger.info(f"Writing to {output_path}")
                
                # Write object (current_output is guaranteed to exist here)
                if current_output:
                    current_output.write(orjson.dumps(obj) + b"\n")
                    obj_counter += 1
        
        # Finalize last file
        if current_output:
            _finalize_current_file()
    
    logger.info(f"Completed {entity} extraction: {len(output_files)} files")
    return output_files


def extract_all(output_dir: Path) -> dict[str, list[Path]]:
    """Extract both biosample and bioproject."""
    results = {}
    
    for entity_func, entity_name in [
        (extract_bioproject, "bioproject"),
        (extract_biosample, "biosample")
    ]:
        try:
            logger.info(f"Starting {entity_name} extraction")
            results[entity_name] = entity_func(output_dir)
        except Exception as e:
            logger.error(f"Failed to extract {entity_name}: {e}")
            results[entity_name] = []
    
    return results


def get_file_stats(output_dir: Path) -> dict:
    """Get statistics about extracted files."""
    stats = {}
    
    for entity in ["biosample", "bioproject"]:
        files = sorted(output_dir.glob(f"{entity}-*.ndjson.gz"))
        total_size = sum(f.stat().st_size for f in files)
        
        stats[entity] = {
            "file_count": len(files),
            "total_size_mb": total_size / (1024 * 1024),
            "files": [f.name for f in files]
        }
    
    return stats


# R2 upload functionality (optional)
def upload_to_r2(local_files: list[Path], entity: str, bucket: str = "biodatalake"):
    """Upload files to R2 storage."""
    import os
    import boto3
    
    # Check for R2 credentials
    if not all(key in os.environ for key in ['R2_ACCESS_KEY', 'R2_SECRET_KEY', 'R2_ENDPOINT']):
        logger.warning("R2 credentials not found, skipping upload")
        return
    
    r2_client = boto3.client(
        's3',
        endpoint_url=os.environ['R2_ENDPOINT'],
        aws_access_key_id=os.environ['R2_ACCESS_KEY'],
        aws_secret_access_key=os.environ['R2_SECRET_KEY'],
    )
    
    for local_file in local_files:
        r2_key = f"biosample/{entity}/{local_file.name}"
        
        logger.info(f"Uploading {local_file} to R2: {r2_key}")
        r2_client.upload_file(str(local_file), bucket, r2_key)


def extract_and_upload(output_dir: Path, upload: bool = True):
    """Extract all entities and optionally upload to R2."""
    results = extract_all(output_dir)
    
    if upload:
        for entity, files in results.items():
            if files:
                upload_to_r2(files, entity)
    
    return results
