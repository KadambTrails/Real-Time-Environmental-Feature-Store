import dlt
from pyspark.sql.functions import *

# --- 1. THE TRENDS TABLE ---
dlt.create_streaming_table(
    name="weather.gold.weather_feature_gold",
    comment="Gold Layer: AI/ML Features with rolling aggregates",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "city_name"
    }
)

@dlt.append_flow(target="weather.gold.weather_feature_gold")
def weather_feature_gold():
    return (
        dlt.read_stream("weather.silver.stg_weather")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "1 hours") 
        .groupBy(
            window(col("event_time"), "1 hour", "15 minutes"),
            col("city_name")
        )
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("windspeed").alias("avg_windspeed"),
            avg("winddirection").alias("avg_winddirection"),
            avg("weathercode").alias("avg_weathercode")
        )
        .select(
            "city_name",
            "avg_temp",
            "avg_windspeed",
            "avg_winddirection",
            "avg_weathercode",
            col("window.start").alias("timestamp"),
            col("window.end").alias("window_end"),
            current_timestamp().alias("ingested_at_utc")
        )
    )

# --- 2. THE ENRICHED VIEW (The Fix) ---
@dlt.view
def enriched_weather_source():
    return (
        dlt.read_stream("weather.silver.stg_weather")
        .withColumn("weather_description",
            when(col("weathercode") == 0 , "Clear Sky")
            .when(col("weathercode").isin(1,2,3),"Partly Clouded")
            .when(col("weathercode").isin(45,48),"Foggy")
            .when(col("weathercode").isin(51, 53, 55), "Drizzle")
            .when(col("weathercode").isin(61, 63, 65), "Rain")
            .when(col("weathercode").isin(71, 73, 75), "Snow")
            .when(col("weathercode").isin(80, 81, 82), "Rain Showers")
            .when(col("weathercode") >= 95, "Thunderstorm")
            .otherwise("Unknown")
        )
    )

# --- 3. THE SNAPSHOT TABLE ---
dlt.create_streaming_table(
    name="weather.gold.weather_latest_snapshot",
    comment="SCD Type 1: Only the most recent reading per city"
)

dlt.apply_changes(
    target = "weather.gold.weather_latest_snapshot",
    source = "enriched_weather_source", 
    keys = ["city_name"],
    sequence_by = col("timestamp"),
    stored_as_scd_type = 1
)