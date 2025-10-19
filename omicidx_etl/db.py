from .config import settings
import duckdb
import contextlib
from pathlib import Path
from typing import Optional


def duckdb_setup_sql(temp_directory: Optional[str] = None):
    """
    Generate DuckDB setup SQL with optional custom temp directory.

    Args:
        temp_directory: Custom temp directory path. If None, uses /tmp/db_temp
    """
    if temp_directory is None:
        temp_directory = '/tmp/db_temp'

    return f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET memory_limit='16GB';
    SET preserve_insertion_order=false;
    SET temp_directory='{temp_directory}';
    SET max_temp_directory_size='100GB';
    CREATE SECRET (
        TYPE r2,
        KEY_ID '{settings.R2_ACCESS_KEY_ID}',
        SECRET '{settings.R2_SECRET_ACCESS_KEY}',
        ACCOUNT_ID '{settings.R2_ACCOUNT_ID}'
    );
    CREATE SECRET osn (
        TYPE s3,
        KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
        SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
        ENDPOINT '{settings.AWS_ENDPOINT_URL or "https://s3.amazonaws.com"}'
    )
    """

@contextlib.contextmanager
def duckdb_connection(temp_directory: Optional[str] = None):
    """
    Create a DuckDB connection with optional custom temp directory.

    Args:
        temp_directory: Custom temp directory path. Defaults to /tmp/db_temp.
                       For large operations, use a directory on a filesystem
                       with sufficient space.

    Example:
        # Use custom temp directory
        with duckdb_connection(temp_directory='/data/tmp') as con:
            con.execute("SELECT * FROM large_table")
    """
    with duckdb.connect() as con:
        con.execute(duckdb_setup_sql(temp_directory))
        yield con