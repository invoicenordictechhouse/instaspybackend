from config import PROJECT_ID
from google.cloud import bigquery

bigquery_client = bigquery.Client(project=PROJECT_ID)
