import os
import tempfile
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format


def main():
    s3_url = os.getenv("WEATHER_S3_URL")
    if not s3_url:
        raise RuntimeError("WEATHER_S3_URL nije setovan u okruženju.")

    print("Downloading CSV from presigned S3 URL (in-memory)...")
    resp = requests.get(s3_url, timeout=20)
    resp.raise_for_status()

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp_file.write(resp.content)
    tmp_file.close()

    print("Temp file:", tmp_file.name)

    spark = (
        SparkSession.builder
        .appName("WeatherDataProcessing")
        .getOrCreate()
    )

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(tmp_file.name)
    )

    print("Ulazne kolone:")
    print(df_raw.columns)

    df_clean = (
        df_raw
        .withColumn(
            "datetime_iso",
            date_format(
                to_timestamp(col("time_date")),
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

    print("=== SAMPLE OUTPUT ===")
    df_clean.show(10, truncate=False)

    output_path = os.getenv("weather_out")

    (
        df_clean
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(output_path)
    )

    print("DONE. Output written to:", output_path)

    spark.stop()


if __name__ == "__main__":
    main()

