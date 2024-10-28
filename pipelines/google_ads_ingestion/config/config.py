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
STAGING_TABLE_ID = (
    os.getenv("STAGING_TABLE_ID_DEV")
    if ENV != "prod"
    else os.getenv("STAGING_TABLE_ID_PROD")
)
CLEAN_TABLE_ID = (
    os.getenv("CLEAN_TABLE_ID_DEV")
    if ENV != "prod"
    else os.getenv("CLEAN_TABLE_ID_PROD")
)
ADVERTISER_IDS_TABLE = (
    os.getenv("ADVERTISER_IDS_TABLE_DEV")
    if ENV != "prod"
    else os.getenv("ADVERTISER_IDS_TABLE_PROD")
)
