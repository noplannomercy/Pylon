from pydantic_settings import BaseSettings

class Config(BaseSettings):
    forge_url: str = "http://localhost:8003"
    lightrag_url: str = "http://localhost:9621"
    database_url: str = ""
    bitbucket_webhook_secret: str = ""
    forge_api_key: str = ""
    lightrag_api_key: str = ""
    port: int = 8001
    self_url: str = "http://localhost:8001"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
