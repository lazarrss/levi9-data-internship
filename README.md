# Levi9 Data Engineering Internship

Code from my **Data Engineering internship at Levi9 Technology Services** (Belgrade, November–December 2025).
The internship focused on building data pipelines on **AWS** with **Python** and **PySpark** to process weather and air-pollution sensor data.

## Contents

| Folder / file | Description |
|---|---|
| `data_partitioning/` | **AWS Glue (PySpark) jobs** that read raw weather, pollution and sensor CSV files from S3, parse timestamps into dates, log row/file counts and null checks, and write the data back to S3 **partitioned by date** for efficient querying. |
| `touristestimation/` | **Data enrichment pipeline**: reads partitioned pollution data from S3, extracts the city from each location, calls an external tourist-estimation API (authenticated with a token stored in **AWS Secrets Manager**), caches API responses per date, and writes the enriched CSV files back to S3 with an `estimated_no_people` column. |
| `my_lambda.py`, `test_aws_lambda.py` | A simple **AWS Lambda** handler that reads an object from S3, with a unit test that mocks the S3 client (`unittest.mock`). |
| `pyspark/` | Local PySpark experiments: cleaning and transforming weather data (ISO timestamps, unit conversion, column selection). |
| `requests/`, `examplerequests.py` | REST API client exercises with the `requests` library: sessions, headers, timeouts and error handling. |
| `simpletesting/` | Unit-testing exercises with `unittest`, including edge cases and invalid input handling. |

## Pipeline Overview

```
Raw CSV (S3) ──► Glue PySpark job ──► Date-partitioned data (S3)
                                              │
                                              ▼
                     Tourist estimation API ◄── Enrichment script ──► Enriched data (S3)
                     (token from Secrets Manager)
```

## Tech Stack

Python · PySpark · AWS Glue · Amazon S3 · AWS Lambda · AWS Secrets Manager · boto3 · pandas · requests · unittest

## Configuration

The scripts read all AWS settings from environment variables, so no credentials are stored in the code:

- Glue jobs: `JOB_NAME`, `SOURCE_PATH`, `TARGET_PATH` (job arguments)
- Enrichment: `INPUT_PATH`, `OUTPUT_PATH`, `ENDPOINT`, `SECRETS_NAME`, `REGION_NAME`, optional `AWS_PROFILE`
