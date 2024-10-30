import os

ENV = os.getenv("ENV", "local")

if ENV == "local":
    from dotenv import load_dotenv

    load_dotenv("local.env")

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = (
    os.getenv("DATASET_ID_DEV") if ENV != "prod" else os.getenv("DATASET_ID_PROD")
)
RAW_TABLE_ID = (
    os.getenv("RAW_TABLE_ID_DEV") if ENV != "prod" else os.getenv("RAW_TABLE_ID_PROD")
)
ADVERTISERS_TRACKING_TABLE_ID = (
    os.getenv("ADVERTISERS_TRACKING_TABLE_ID_DEV")
    if ENV != "prod"
    else os.getenv("ADVERTISERS_TRACKING_TABLE_ID_PROD")
)
