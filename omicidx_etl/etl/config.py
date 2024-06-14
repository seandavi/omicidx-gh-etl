from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # The base path for the GCS bucket
    storage_base_path: str = "gs://omicidx-json/"

    # Clickhouse
    CLICKHOUSE_HOST: str = ""
    CLICKHOUSE_USERNAME: str = ""
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"


settings = Settings()


if __name__ == "__main__":
    settings = Settings()

    print(settings.model_dump())
