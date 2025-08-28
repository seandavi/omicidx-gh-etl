"""config settings for omicidx_etl"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from dotenv import load_dotenv
from upath import UPath

load_dotenv()  # Load environment variables from .env file

class Settings(BaseSettings):
    """settings for omicidx_etl"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="allow",
    )

    PUBLISH_DIRECTORY: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_ENDPOINT_URL: Optional[str] = None
    R2_ACCESS_KEY_ID: str
    R2_SECRET_ACCESS_KEY: str
    R2_ACCOUNT_ID: str

    @property
    def publish_directory(self) -> UPath:
        return UPath(self.PUBLISH_DIRECTORY)

settings = Settings()  # type: ignore

if __name__ == "__main__":
    print(settings.model_dump())
