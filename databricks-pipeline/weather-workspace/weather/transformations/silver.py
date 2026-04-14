from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

weather_schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("winddirection", IntegerType(), True),
    StructField("weathercode", IntegerType(), True),
    StructField("timestamp", StringType(), True) 
])

dp.create_streaming_table("weather.silver.stg_weather")

@dp.append_flow(
    target = "weather.silver.stg_weather"
)
def weather_stream():
    df = spark.readStream.table('weather.bronze.weather_raw')
    df_parsed = df.withColumn("parsed_weatherInfo", from_json(col("tempInfo"), weather_schema))
    df_parsed = df_parsed.select(col("parsed_weatherInfo.*"))
    df_parsed = df_parsed.withColumn("ingested_at_utc", current_timestamp())
    return df_parsed