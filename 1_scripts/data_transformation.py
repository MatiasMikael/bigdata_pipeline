import os
import pandas as pd
import logging

# Configure logging to save logs in the 5_logs folder
log_dir = 'C:/Users/Matias/Desktop/bigdata_pipeline/5_logs'
os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists
log_file = os.path.join(log_dir, 'data_transformation.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def clean_address_newlines(df):
    # Cleans newlines from the 'address' column to ensure all data fits in a single row per entry.
    logging.info("Cleaning newlines from the address column.")
    df['address'] = df['address'].str.replace(r'\n', ' ', regex=True)
    return df

def transform_data(input_file, output_file):
    # Reads the input CSV with UTF-8 encoding, cleans and formats it, then saves it to a new file.
    try:
        # Log start of the transformation
        logging.info(f"Starting data transformation for file: {input_file}")

        # Load the data from the input file with UTF-8 encoding
        logging.info("Loading data...")
        df = pd.read_csv(input_file, encoding='utf-8')

        # Clean the 'address' column by removing newlines
        logging.info("Cleaning address column...")
        df = clean_address_newlines(df)

        # Save the cleaned and formatted data to the output file with UTF-8 encoding
        logging.info(f"Saving cleaned data to: {output_file}")
        df.to_csv(output_file, index=False, encoding='utf-8')

        logging.info("Data transformation completed successfully.")

    except Exception as e:
        # Log any errors that occur during the process
        logging.error(f"An error occurred during data transformation: {e}", exc_info=True)

if __name__ == "__main__":
    # Define the paths for the input and output files
    input_file = 'C:/Users/Matias/Desktop/bigdata_pipeline/3_results/cleaned_big_data_fi.csv'
    output_file = 'C:/Users/Matias/Desktop/bigdata_pipeline/3_results/bigquery_ready_data.csv'

    # Print the current working directory for debugging purposes
    current_dir = os.getcwd()
    logging.info(f"Current working directory: {current_dir}")

    # Run the transformation function with the defined file paths
    transform_data(input_file, output_file)