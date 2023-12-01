import os
import shutil
import pytest
from transformation_scripts.transpose_data import run_transpose_data
from pyspark.sql import SparkSession

# Fixture to create a Spark session for testing
@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

# Test function for the transpose_data script
def test_transpose_data(spark):
    # Set up the output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Run the data processing logic
    run_transpose_data(spark)

    # Read the result CSV file
    result_df = spark.read.csv("/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/output_data/result.csv", header=True, inferSchema=True)

    # Add your assertions here based on the expected output
    assert "unique_id" in result_df.columns, "Missing 'unique_id' column in the result."
    assert "ex_1_present" in result_df.columns, "Missing 'ex_1_present' column in the result."
    assert "ex_2_present" in result_df.columns, "Missing 'ex_2_present' column in the result."
    # ... add more assertions based on your expected output

    # Clean up the output directory
    shutil.rmtree(output_dir)

# Run the tests when the script is executed directly
if __name__ == "__main__":
    pytest.main([__file__])
