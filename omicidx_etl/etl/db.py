import clickhouse_connect as ch
from .config import settings


def get_client():
    return ch.get_client(
        host=settings.ch_host, username=settings.ch_user, password=settings.ch_password
    )
