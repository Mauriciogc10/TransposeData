from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transpose(spark):
    # List of CSV files
    csv_files = ["/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/input_data/ex_1.csv", "/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/input_data/ex_2.csv", "/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/input_data/ex_3.csv", "/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/input_data/ex_4.csv"]


    # Read the CSV files into a DataFrame
    dfs = [spark.read.csv(file, header=True, inferSchema=True) for file in csv_files]

    # Union the DataFrames to get distinct unique_ids
    unique_ids = dfs[0].select("unique_id")
    for df in dfs[1:]:
        unique_ids = unique_ids.union(df.select("unique_id"))

    # Create a DataFrame with distinct unique_ids
    result_df = unique_ids.distinct()

    # Loop through each CSV file and check if the unique_id is present
    for csv_file, df in zip(csv_files, dfs):
        # Perform a left join with the existing result DataFrame using aliases
        result_df = result_df.join(df.select(col("unique_id").alias(f"{csv_file.replace('/content/', '').replace('.', '_')}_id")), 
                                col("unique_id") == col(f"{csv_file.replace('/content/', '').replace('.', '_')}_id"), 
                                "left_outer")

    # Replace null values with 0 and non-null values with 1
    for file in csv_files:
        result_df = result_df.withColumn(f"{file.replace('/content/', '').replace('.', '_')}_present",
                                        (col(f"{file.replace('/content/', '').replace('.', '_')}_id").isNotNull()).cast("int"))

    # Drop intermediate columns
    result_df = result_df.drop(*[f"{file.replace('/content/', '').replace('.', '_')}_id" for file in csv_files])

    # Show the final result
    result_df.show(truncate=False)

    # Save the result as a CSV file
    result_df.write.csv("/Users/mauriciogodinezcastro/Desktop/LNN/TransposeData/output_data/result.csv", header=True, mode="overwrite")


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("TransposeData").getOrCreate()

    # Run the data processing logic
    #run_transpose_data(spark)
    transpose(spark)

    # Stop the Spark session
    spark.stop()
