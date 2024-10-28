from google.cloud import bigquery
from config import PROJECT_ID

bigquery_client = bigquery.Client(project=PROJECT_ID)
