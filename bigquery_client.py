from google.cloud import bigquery
import os
from dotenv import load_dotenv
 
load_dotenv()
 
client = bigquery.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
 
def execute_query(query: str):
    try:
        query_job = client.query(query)
        results = [dict(row) for row in query_job]
        return results
    except Exception as e:
        raise Exception(f"BigQuery query failed: {e}")
