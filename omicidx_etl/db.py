from .config import settings
import duckdb
import contextlib


def duckdb_setup_sql():
    return f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET memory_limit='16GB';
    SET preserve_insertion_order=false;
    SET temp_directory='/tmp/db_temp';
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
def duckdb_connection():
    with duckdb.connect() as con:
        con.execute(duckdb_setup_sql())
        yield con