import os
import logging
import pandas as pd

# Configure logging
log_folder = os.path.join(os.getcwd(), '5_logs')
os.makedirs(log_folder, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_folder, 'data_processing.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Top 10 biggest cities in Finland
top_cities = [
    "Helsinki", "Espoo", "Tampere", "Vantaa", "Oulu",
    "Turku", "Jyväskylä", "Lahti", "Kuopio", "Pori"
]

# Data processing function
def process_data(input_file='2_data/big_data_fi.csv', output_file='3_results/cleaned_big_data_fi.csv'):
    """Filter data for top 10 cities based on the city part of the address."""
    print(f"Starting data processing from {input_file}...")
    logging.info(f"Starting data processing from {input_file}.")

    try:
        # Read data
        print("Reading input data...")
        df = pd.read_csv(input_file)

        # Extract the city name (assuming it's the last part after the postal code)
        def extract_city(address):
            try:
                return address.split('\n')[-1].split()[-1]
            except Exception:
                return None

        print("Extracting city information from addresses...")
        df['city'] = df['address'].apply(extract_city)

        # Filter rows where the city is in the top cities list
        print(f"Filtering rows for top cities: {top_cities}...")
        filtered_df = df[df['city'].isin(top_cities)]

        # Drop the temporary city column before saving
        filtered_df = filtered_df.drop(columns=['city'])

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save the cleaned data
        print(f"Saving filtered data to {output_file}...")
        filtered_df.to_csv(output_file, index=False)

        logging.info(f"Filtered data saved to {output_file}. Rows remaining: {len(filtered_df)}")
        print(f"Data processing complete. Rows remaining: {len(filtered_df)}")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        print(f"An error occurred during data processing: {e}")

# Run the script
if __name__ == "__main__":
    print("Script started.")
    logging.info("Script started.")
    process_data()
    print("Script finished.")
    logging.info("Script finished.")