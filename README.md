## bigdata_pipeline

### Overview
This project demonstrates a complete big data pipeline that generates, processes, and transforms synthetic Finnish data before uploading it to Google BigQuery for further analysis. The pipeline consists of multiple steps described below.

### Pipeline Steps

1. **Data Generation**
   - **Script**: `data_generator.py`
   - **Description**: Generates synthetic Finnish big data using the Faker library. Each record includes fields such as `id`, `name`, `email`, `address`, `phone`, and `created_at`.
   - **Output File**: `2_data/big_data_fi.csv`

2. **Data Processing**
   - **Script**: `data_processing.py`
   - **Description**: Filters the data to include only entries from the top 10 largest cities in Finland (e.g., Helsinki, Espoo, Tampere).
   - **Output File**: `3_results/cleaned_big_data_fi.csv`

3. **Data Transformation**
   - **Script**: `data_transformation.py`
   - **Description**: Cleans the `address` column to remove newline characters and prepares the data in a format compatible with BigQuery.
   - **Output File**: `3_results/bigquery_ready_data.csv`

4. **Data Upload**
   - **Script**: `data_upload.py`
   - **Description**: Uploads the transformed data to Google BigQuery.
   - **Dataset**: `processed_data`
   - **Table**: `bigquery_ready_data`

### Tools Used
- Python for scripting and data manipulation
- VS Code as the development environment
- Google Cloud BigQuery for data storage and querying
- SQL for querying the processed data
- Python libraries including Faker, Pandas, and Google Cloud BigQuery
- Logging to track progress and errors
- CSV files for intermediate data storage

### Future Development Suggestions
- Expand the dataset to include other Nordic countries for regional analysis
- Implement real-time data streaming using tools like Apache Kafka
- Enhance data validation with automated quality checks and anomaly detection
- Integrate visualization tools like Tableau or Power BI to analyze query results
- Add machine learning capabilities to generate insights from the processed data

### License
This project is licensed under the MIT License.
