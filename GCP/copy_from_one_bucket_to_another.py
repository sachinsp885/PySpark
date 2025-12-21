from google.cloud import bigquery, storage
import ast

# Initialize clients
bigquery_client = bigquery.Client()
storage_client = storage.Client()

env = "dv"
table_name ='logs'

# Bucket details
source_project_id = f"gcp-file-transfer-{env}"
destin_project_id = f"gcp-file-transfer-{env}"

source_bucket_name = f"gcp-file-source-{env}"
destin_bucket_name = f"gcp-file-destination-{env}"

# BigQuery query
query = f"SELECT input_files FROM {table_name} LIMIT 1"
query_job = bigquery_client.query(query)
results = list(query_job.result())

if not results:
    print("No files found")
else:
    for row in results:
        input_files = ast.literal_eval(row.input_files) if row.input_files else []

        if input_files:
            source_client = storage.Client(project=source_project_id)
            destin_client = storage.Client(project=destin_project_id)

            source_bucket = source_client.bucket(source_bucket_name)
            destin_bucket = destin_client.bucket(destin_bucket_name)

            for file_name in input_files:
                source_blob_path = f"path/{file_name}"
                source_blob = source_bucket.blob(source_blob_path)

                try:
                    copied_blob = source_bucket.copy_blob(
                        source_blob,
                        destin_bucket,
                        file_name
                    )
                    print(f"Copied {source_blob_path} to {copied_blob.name}")
                except Exception as e:
                    print(f"Failed to copy {file_name}: {e}")
