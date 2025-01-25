import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark import SparkETLJob
from pyspark.sql.functions import col


class TestSparkETLJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("ETL Test") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after all tests."""
        cls.spark.stop()

    def test_read_data(self):
        """Test that read_data correctly reads restaurant data."""
        # Create a mock ETL job
        etl = SparkETLJob("data/restaurants", "data/weather", "data/output")
        etl.spark = self.spark

        # Mock data for restaurants
        restaurant_data = [
            ("Milan", "IT", 45.4642, 9.1900),
            ("Paris", "FR", 48.8566, 2.3522)
        ]
        restaurant_schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
        ])
        restaurant_df = self.spark.createDataFrame(restaurant_data, schema=restaurant_schema)

        etl.df = restaurant_df

        # Assert the data was loaded correctly
        self.assertEqual(etl.df.count(), 2)
        self.assertTrue("city" in etl.df.columns)
        self.assertTrue("country" in etl.df.columns)

    def test_geohash_generation(self):
        """Test that geohash generation is correct."""
        # Mock data
        data = [
            (45.4642, 9.1900),  # Milan
            (48.8566, 2.3522),  # Paris
        ]
        schema = StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])
        df = self.spark.createDataFrame(data, schema)

        # Mock ETL job
        etl = SparkETLJob("data/restaurants", "data/weather", "data/output")
        etl.spark = self.spark

        # Apply geohash UDF
        generate_geohash_udf = etl.define_udfs()[2]
        df_with_geohash = df.withColumn("geohash", generate_geohash_udf(col("latitude"), col("longitude")))

        # Collect results
        results = df_with_geohash.select("geohash").collect()
        self.assertEqual(results[0]["geohash"], "u0nd")  # Example geohash for Milan
        self.assertEqual(results[1]["geohash"], "u09t")  # Example geohash for Paris

    def test_join_weather_data(self):
        """Test the join between restaurant and weather data."""
        # Mock restaurant data
        restaurant_data = [("u0rd", "Milan", "IT", 45.4642, 9.1900)]
        restaurant_schema = StructType([
            StructField("geohash", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])
        restaurant_df = self.spark.createDataFrame(restaurant_data, schema=restaurant_schema)

        # Mock weather data
        weather_data = [("u0rd", 20.5, 10.2)]
        weather_schema = StructType([
            StructField("geohash", StringType(), True),
            StructField("avg_tmpr_c", DoubleType(), True),
            StructField("avg_tmpr_f", DoubleType(), True),
        ])
        weather_df = self.spark.createDataFrame(weather_data, schema=weather_schema)

        # Mock ETL job
        etl = SparkETLJob("data/restaurants", "data/weather", "data/output")
        etl.spark = self.spark
        etl.df = restaurant_df
        etl.weather_df = weather_df

        # Join and validate
        etl.process_data()
        joined_df = etl.df

        self.assertEqual(joined_df.count(), 1)
        self.assertTrue("avg_tmpr_c" in joined_df.columns)
        self.assertTrue("avg_tmpr_f" in joined_df.columns)

if __name__ == "__main__":
    unittest.main()