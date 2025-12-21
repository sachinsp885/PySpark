import boto3
import ast
import time

env = "dv"

# AWS clients
s3_client = boto3.client("s3")
athena_client = boto3.client("athena")

# S3 bucket details
source_bucket_name = f"gcp-file-source-{env}"
destin_bucket_name = f"gcp-file-destination-{env}"

# Athena details
database = "logs_db"
table = "logs"
output_location = "s3://athena-query-results-bucket/"

query = f"SELECT input_files FROM {table} LIMIT 1"

# Execute Athena query
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": database},
    ResultConfiguration={"OutputLocation": output_location}
)

query_execution_id = response["QueryExecutionId"]

# Wait for query to complete
while True:
    status = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id
    )["QueryExecution"]["Status"]["State"]

    if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        break
    time.sleep(1)

if status != "SUCCEEDED":
    print("Athena query failed")
else:
    result = athena_client.get_query_results(
        QueryExecutionId=query_execution_id
    )

    rows = result["ResultSet"]["Rows"][1:]  # Skip header

    if not rows:
        print("No files found")
    else:
        for row in rows:
            input_files = row["Data"][0].get("VarCharValue")

            input_files = ast.literal_eval(input_files) if input_files else []

            if input_files:
                for file_name in input_files:
                    source_key = f"path/{file_name}"
                    destination_key = file_name

                    try:
                        s3_client.copy_object(
                            Bucket=destin_bucket_name,
                            CopySource={
                                "Bucket": source_bucket_name,
                                "Key": source_key
                            },
                            Key=destination_key
                        )
                        print(f"Copied {source_key} to {destination_key}")
                    except Exception as e:
                        print(f"Failed to copy {file_name}: {e}")
