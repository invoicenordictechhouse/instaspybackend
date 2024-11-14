from typing import Dict, Any
from pydantic import BaseModel


class EnvironmentConfig(BaseModel):
    dataset_id: str
    raw_table_id: str
    advertisers_tracking: str
    logging: Dict[str, Any]


class Config(BaseModel):
    project_id: str
    environments: Dict[str, EnvironmentConfig]
