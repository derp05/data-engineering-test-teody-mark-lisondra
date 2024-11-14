import pandas as pd
import sqlite3
from datetime import datetime

# Step 1: Read Data
def read_data(file_path):
    """Read data from a CSV file into a DataFrame."""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

# Step 2: Data Cleaning and Validation
def clean_data(df):
    """Perform data validation and cleaning on the DataFrame."""
    # Example: Drop rows with missing values in 'sales' and 'date' columns
    df = df.dropna(subset=['sales', 'date'])
    
    # Fill missing values in other columns if needed
    # df['some_column'].fillna(value=default_value, inplace=True)
    
    # Ensure correct data types (e.g., sales as float, date as datetime)
    df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Drop rows with invalid data after type conversion
    df = df.dropna(subset=['sales', 'date'])
    return df

# Step 3: Data Transformation
def transform_data(df):
    """Transform data by standardizing date format and aggregating sales by category and month."""
    # Standardize date format to month-year
    df['month'] = df['date'].astype(str)
    
    # Aggregate sales by category and month
    transformed_df = df.groupby(['category', 'month']).agg({'sales': 'sum'}).reset_index()
    return transformed_df

# Step 4: Data Loading
def load_data_to_sqlite(df, db_path):
    """Load transformed data into an SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql('sales_data', conn, if_exists='replace', index=False)
        conn.close()
        print("Data loaded successfully into SQLite database.")
    except Exception as e:
        print(f"Error loading data into SQLite: {e}")

def load_data_to_csv(df, output_path):
    """Save transformed data to a CSV file."""
    try:
        df.to_csv(output_path, index=False)
        print("Data saved successfully to CSV file.")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

# Main Pipeline
def run_pipeline(input_file, output_db, output_csv):
    """Run the entire data pipeline."""
    # Step 1: Read Data
    df = read_data(input_file)
    if df is None:
        return

    # Step 2: Clean and Validate Data
    df = clean_data(df)

    # Step 3: Transform Data
    transformed_df = transform_data(df)

    # Step 4: Load Data
    load_data_to_sqlite(transformed_df, output_db)
    load_data_to_csv(transformed_df, output_csv)

if __name__ == "__main__":
    # Define file paths
    input_file = 'input_sales_data.csv'
    output_db = 'sales_data.db'
    output_csv = 'transformed_sales_data.csv'
    
    # Run the pipeline
    run_pipeline(input_file, output_db, output_csv)
