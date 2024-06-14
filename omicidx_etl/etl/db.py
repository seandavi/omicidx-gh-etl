import clickhouse_connect as ch
from .config import settings


def get_client():
    return ch.get_client(
        host=settings.CLICKHOUSE_HOST,
        username=settings.CLICKHOUSE_USERNAME,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    )
