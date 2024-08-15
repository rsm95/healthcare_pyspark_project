from pyspark.sql import SparkSession
import requests
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Fetch F1 Drivers Data") \
    .getOrCreate()

# API URL
api_url = "https://ergast.com/api/f1/drivers.json"

# Send GET request
response = requests.get(api_url)

# Check if the request was successful
if response.status_code != 200:
    raise Exception(f"API request failed with status code {response.status_code}")

# Parse JSON response
data = response.json()

print(data)

# Navigate through the JSON structure to get the list of drivers
drivers = data['MRData']['DriverTable']['Drivers']

# Create DataFrame
drivers_df = spark.createDataFrame(drivers)

# Show the DataFrame schema
drivers_df.printSchema()

# Display the DataFrame content
drivers_df.show(truncate=False)

# Stop the Spark session
spark.stop()
