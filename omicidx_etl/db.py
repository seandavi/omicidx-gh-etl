import clickhouse_connect as ch
from .etl.config import settings
import duckdb
import contextlib

def get_client():
    return ch.get_client(
        host=settings.CLICKHOUSE_HOST,
        username=settings.CLICKHOUSE_USERNAME,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    )
    
def duckdb_setup_sql():
    return """
    INSTALL httpfs;
    LOAD httpfs;
    SET memory_limit='16GB';
    SET preserve_insertion_order=false;
    SET temp_directory='/tmp/db_temp';
    SET max_temp_directory_size='100GB';
    CREATE SECRET (
        TYPE r2,
        KEY_ID '{{settings.R2_ACCESS_KEY_ID}}',
        SECRET '{{settings.R2_SECRET_ACCESS_KEY}}',
        ACCOUNT_ID '{{settings.R2_ACCOUNT_ID}}'
    );
    """

@contextlib.contextmanager
def duckdb_connection():
    with duckdb.connect() as con:
        con.execute(duckdb_setup_sql())
        yield con