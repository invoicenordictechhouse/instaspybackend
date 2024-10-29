import os

ENV = os.getenv("ENV", "local")

if ENV == "local":
    from dotenv import load_dotenv

    load_dotenv("local.env")

PROJECT_ID = (
    os.getenv("PROJECT_ID_DEV") if ENV != "prod" else os.getenv("PROJECT_ID_PROD")
)
DATASET_ID = (
    os.getenv("DATASET_ID_DEV") if ENV != "prod" else os.getenv("DATASET_ID_PROD")
)
RAW_TABLE_ID = (
    os.getenv("RAW_TABLE_ID_DEV") if ENV != "prod" else os.getenv("RAW_TABLE_ID_PROD")
)
ADVERTISERS_TRACKING = (
    os.getenv("ADVERTISERS_TRACKING_DEV")
    if ENV != "prod"
    else os.getenv("ADVERTISERS_TRACKING_PROD")
)
