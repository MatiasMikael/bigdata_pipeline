import os
import logging
from google.cloud import bigquery

# Configure logging to save logs in the 5_logs folder
log_dir = "5_logs"
os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists
log_file = os.path.join(log_dir, "data_upload.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def upload_to_bigquery(credentials, input_file, dataset_id, table_id):
    try:
        # Set credentials and initialize BigQuery client
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(credentials)
        client = bigquery.Client()

        # Define table reference and job configuration
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"  # Overwrite existing table
        )

        # Upload data to BigQuery
        logging.info(f"Uploading {input_file} to {table_ref}...")
        with open(input_file, "rb") as source_file:
            load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Upload completed to {table_ref}.")
        print(f"Upload completed to {table_ref}.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_to_bigquery(
        credentials="credentials/bigdata-pipeline-447012-1d64943f363e.json",
        input_file="3_results/bigquery_ready_data.csv",
        dataset_id="processed_data",
        table_id="bigquery_ready_data"
    )