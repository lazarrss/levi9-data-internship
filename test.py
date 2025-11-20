from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format

def main():
    spark = (
        SparkSession.builder
        .appName("WeatherDataProcessing")
        .getOrCreate()
    )

    input_path = "file:///C:/Users/lazar/Downloads/levi9-hack9-weather-firehose-2-2022-04-01-00-50-43-3d54846b-4dc1-31ed-9657-62b8bee0ea48"

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )
    df_clean = (
        df_raw
        .withColumn(
            "datetime_iso",
            date_format(
                to_timestamp(col("time_date")),  # "2022-04-01 00:50:43"
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
        .withColumn(
            "wind_speed_kmh",
            col("weather_windSpeed") * 3.6
        )
        .select(
            col("name"),
            col("datetime_iso"),
            col("weather_temperature").alias("temperature"),
            col("wind_speed_kmh")
        )
    )

    print("=== TRANSFORMED SAMPLE ===")
    df_clean.show(10, truncate=False)

    output_path = "file:///C:/spark_demo/output/weather_csv"

    (
        df_clean
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
