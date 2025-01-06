import os
import logging
from faker import Faker
import pandas as pd

# Configure logging
log_folder = os.path.join(os.getcwd(), '5_logs')
os.makedirs(log_folder, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_folder, 'data_generator.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Faker with Finnish locale
fake = Faker('fi_FI')

# Generate Finnish big data
def generate_big_data(records=100000, output_file='2_data/big_data_fi.csv'):
    """Generate synthetic Finnish big data and save it as a CSV file."""
    print(f"Starting data generation for {records} records...")
    logging.info(f"Starting data generation for {records} records.")
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        data = [
            {
                "id": i,
                "name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "phone": fake.phone_number(),
                "created_at": fake.date_time_this_year()
            }
            for i in range(records)
        ]
        print("Data generation complete. Saving to CSV...")
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
        logging.info(f"Generated {records} Finnish records and saved to {output_file}.")
        print(f"Data successfully saved to {output_file}.")
    except Exception as e:
        logging.error(f"Error generating Finnish data: {e}")
        print(f"An error occurred: {e}")

# Run the script
if __name__ == "__main__":
    print("Script started.")
    logging.info("Script started.")
    generate_big_data()
    print("Script finished.")
    logging.info("Script finished.")