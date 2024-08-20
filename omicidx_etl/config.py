"""config settings for omicidx_etl"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """settings for omicidx_etl"""

    model_config = SettingsConfigDict()

    PUBLISH_DIRECTORY: str


settings = Settings()  # type: ignore

if __name__ == "__main__":
    print(settings.model_dump())
