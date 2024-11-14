Introduction
This project demonstrates Extract, Transform, Load data pipeline. The pipeline reads a dataset, performs data validation and transformation, and then loads the processed data into a structured output format. 

Objective
The objective of this data pipeline is to:
Extract: Read and process a dataset from a CSV file.
Transform: Clean and standardize the data, including handling missing values, converting date formats, and aggregating sales by category and time period.
Load: Store the cleaned and transformed data in an SQLite database or export it as a new CSV file for further analysis.

The sample dataset includes:
Sales information - sales
Dates of transactions - date
Product categories - category

Requirements
Language: Python
Libraries:
pandas: For data manipulation and processing.
sqlite3: For loading and saving data to a structured SQLite database.
unittest: For unit testing critical functions within the pipeline.

Tasks
Data Validation:
Check for missing or malformed values in the sales and date columns.
Drop rows with critical missing values, such as empty sales or invalid dates, to maintain data quality.
Ensure columns have correct data types (e.g., sales as numeric and date as datetime).

Data Transformation:
Normalize date formats to ensure consistency.
Aggregate sales data by product category and month.
Create a new column representing the month-year format for easier analysis.

Data Loading:
Save the transformed data to an SQLite database.
Alternatively, output the data as a new CSV file, allowing easy access and analysis.

Project Structure

data_pipeline.py         # Main ETL pipeline code
test_data_pipeline.py    # Unit tests for pipeline functions
sample_data.csv          # Sample dataset (input file)
transformed_data.csv     # Transformed data (output file)
README.md                # Project documentation

Run pipeline using: python data_pipeline.py
Run Test using: python -m unittest test_data_pipeline.py