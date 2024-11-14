import unittest
import pandas as pd
from data_pipeline import read_data, clean_data, transform_data, load_data_to_csv
import os

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        """Set up sample data for testing."""
        self.sample_data = pd.DataFrame({
            'sales': [100, 200, None, 300],
            'date': ['2024-01-01', '2024-02-02', 'invalid_date', '2024-03-03'],
            'category': ['A', 'B', 'A', 'B']
        })
        self.sample_data['date'] = pd.to_datetime(self.sample_data['date'], errors='coerce')

    def test_read_data(self):
        """Test reading data function."""
        df = read_data('sample_data.csv')
        self.assertIsNotNone(df, "Data should be read successfully from a valid file.")

    def test_clean_data(self):
        """Test data cleaning function."""
        cleaned_data = clean_data(self.sample_data)
        self.assertFalse(cleaned_data['sales'].isnull().any(), "Sales column should have no missing values.")
        self.assertFalse(cleaned_data['date'].isnull().any(), "Date column should have no invalid dates.")

    def test_transform_data(self):
        """Test data transformation function."""
        cleaned_data = clean_data(self.sample_data)
        transformed_data = transform_data(cleaned_data)
        self.assertIn('month', transformed_data.columns, "Data should have a 'month' column after transformation.")
        self.assertTrue(transformed_data['sales'].sum() > 0, "Aggregated sales should be greater than zero.")

    def test_load_data_to_csv(self):
        """Test data loading to CSV."""
        output_path = 'test_output.csv'
        load_data_to_csv(self.sample_data, output_path)
        self.assertTrue(os.path.exists(output_path), "Output CSV file should exist after saving data.")
        os.remove(output_path)

    def tearDown(self):
        """Clean up files after tests."""
        if os.path.exists('test_output.csv'):
            os.remove('test_output.csv')

if __name__ == '__main__':
    unittest.main()
