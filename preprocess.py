from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # 1. Initialize a Spark session
    spark = SparkSession.builder \
        .appName("TaxiTripsPreprocessing") \
        .getOrCreate()

    # 2. Read the cleaned dataset into a Spark DataFrame
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

    # 3. Add the new column 'fare_per_minute'
    # fare_per_minute = fare / (trip_seconds / 60.0)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))

    # 4. Register the DataFrame as a SQL view named 'trips'
    df.createOrReplaceTempView("trips")

    # 5. Compute the company-level summary using Spark SQL
    query = """
        SELECT 
            company, 
            COUNT(*) AS trip_count, 
            ROUND(AVG(fare), 2) AS avg_fare, 
            ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute 
        FROM trips 
        GROUP BY company 
        ORDER BY trip_count DESC
    """
    summary_df = spark.sql(query)

    # 6. Save the resulting DataFrame to processed_data/ in JSON format (one record per line)
    # Using mode("overwrite") ensures we don't get errors if we run it multiple times
    summary_df.write.mode("overwrite").json("processed_data/")

    print("Preprocessing complete. Data saved to processed_data/")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()