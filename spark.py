import requests
import geohash2 as geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import DoubleType, StringType
from dotenv import load_dotenv
import os

class SparkETLJob:
    def __init__(self, input_path, weather_path, output_path):
        # Load environment variables
        load_dotenv()
        self.api_key = os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY is not set in the .env file")

        self.spark = SparkSession.builder \
            .appName("Restaurant and Weather Data ETL") \
            .getOrCreate()
        self.input_path = input_path
        self.weather_path = weather_path
        self.output_path = output_path
        self.df = None
        self.weather_df = None

    def read_data(self):
        """Reads all CSV files from the input directory and all weather parquet files recursively."""
        # Read all restaurant files
        self.df = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(self.input_path)

        # Read all weather parquet files recursively
        self.weather_df = self.spark.read.format("parquet").option("recursiveFileLookup", "true").load(self.weather_path)

        # Generate geohash for weather data
        geohash_udf = udf(lambda lat, lng: geohash.encode(lat, lng, precision=4), StringType())
        self.weather_df = self.weather_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

        print("Data read successfully.")

    @staticmethod
    def fetch_coordinates_static(api_key, city, country):
        """Fetches latitude and longitude for a given city and country using OpenCage API."""
        try:
            query = f"{city}, {country}"
            url = f"https://api.opencagedata.com/geocode/v1/json"
            params = {
                "q": query,
                "key": api_key,
                "limit": 1
            }
            response = requests.get(url, params=params)
            data = response.json()
            if data['results']:
                location = data['results'][0]['geometry']
                return location['lat'], location['lng']
        except Exception as e:
            print(f"Error fetching coordinates for city '{city}', country '{country}': {e}")
        return None, None

    def define_udfs(self):
        """Defines UDFs for geocoding and geohashing."""
        api_key = self.api_key

        @udf(DoubleType())
        def get_latitude_udf(city, country):
            lat, _ = SparkETLJob.fetch_coordinates_static(api_key, city, country)
            return lat

        @udf(DoubleType())
        def get_longitude_udf(city, country):
            _, lng = SparkETLJob.fetch_coordinates_static(api_key, city, country)
            return lng

        @udf(StringType())
        def generate_geohash_udf(lat, lng):
            if lat is not None and lng is not None:
                return geohash.encode(lat, lng, precision=4)
            return None

        return get_latitude_udf, get_longitude_udf, generate_geohash_udf

    def process_data(self):
        """Processes the restaurant data: fills missing coordinates, generates geohash, joins with weather data, and deduplicates."""
        get_latitude_udf, get_longitude_udf, generate_geohash_udf = self.define_udfs()

        # Fill missing latitude and longitude
        self.df = self.df.withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude")
        self.df = self.df.withColumn(
            "latitude", when(col("latitude").isNull(), get_latitude_udf(col("city"), col("country"))).otherwise(col("latitude"))
        ).withColumn(
            "longitude", when(col("longitude").isNull(), get_longitude_udf(col("city"), col("country"))).otherwise(col("longitude"))
        )

        # Generate geohash
        self.df = self.df.withColumn("geohash", generate_geohash_udf(col("latitude"), col("longitude")))

        # Join with weather data
        self.df = self.df.join(self.weather_df, "geohash", "left_outer")

        # Deduplicate the data
        self.df = self.df.dropDuplicates()

        print("Data processing completed.")

    def write_data(self):
        """Writes the enriched data to the output path in Parquet format and prints a sample of 100 rows."""
        self.df.write.parquet(self.output_path, mode="overwrite", partitionBy="geohash")
        print(f"Data written successfully to {self.output_path}")

        # Print 100 rows of processed data
        print("Sample of processed data:")
        self.df.show(100, truncate=False)

    def run(self):
        """Runs the ETL job."""
        self.read_data()
        self.process_data()
        self.write_data()
        self.spark.stop()

# Main Function
if __name__ == "__main__":
    INPUT_PATH = "data/restaurants"
    WEATHER_PATH = "data/weather"
    OUTPUT_PATH = "data/output/enriched_restaurants"

    etl_job = SparkETLJob(INPUT_PATH, WEATHER_PATH, OUTPUT_PATH)
    etl_job.run()