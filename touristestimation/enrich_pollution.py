import os
import io
import re
import json
from typing import Dict, Any

import boto3
import requests
import pandas as pd
from botocore.exceptions import ClientError


def get_boto_session() -> boto3.Session:
    profile = os.environ.get("AWS_PROFILE")
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()


def get_tourist_token() -> str:
    secret_name = os.environ.get("SECRETS_NAME")
    region_name = os.environ.get("REGION_NAME")

    if not secret_name:
        raise RuntimeError("Environment variable SECRETS_NAME is not set")
    if not region_name:
        raise RuntimeError("Environment variable REGION_NAME is not set")

    session = get_boto_session()
    client = session.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise RuntimeError(f"Could not retrieve secret '{secret_name}': {e}") from e

    if "SecretString" in response:
        secret_str = response["SecretString"]
        try:
            data = json.loads(secret_str)
            if isinstance(data, dict) and "token" in data:
                return data["token"]
            return secret_str
        except json.JSONDecodeError:
            return secret_str
    elif "SecretBinary" in response:
        return response["SecretBinary"].decode("utf-8")
    else:
        raise RuntimeError(f"Secret '{secret_name}' has no SecretString or SecretBinary")


def get_endpoint() -> str:
    endpoint = os.environ.get("ENDPOINT")
    if not endpoint:
        raise RuntimeError("Environment variable ENDPOINT is not set")
    return endpoint


def fetch_city_estimates(for_date: str) -> Dict[str, Any]:
    endpoint = get_endpoint()
    token = get_tourist_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {"date": for_date}

    resp = requests.get(endpoint, headers=headers, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"API call failed with status {resp.status_code}: {resp.text}"
        ) from e

    return resp.json()


def get_city_to_estimate(for_date: str) -> Dict[str, int]:
    """
    Za zadati datum vraća mapiranje: city_name -> estimated_no_people
    """
    payload = fetch_city_estimates(for_date)
    info = payload.get("info", [])
    mapping: Dict[str, int] = {}
    for city in info:
        name = city.get("name")
        est = city.get("estimated_no_people")
        if name is not None:
            mapping[name] = est
    return mapping


def parse_s3_url(url: str):
    """
    's3://bucket/prefix' ili 's3a://bucket/prefix' -> (bucket, prefix)
    """
    if url.startswith("s3a://"):
        url = "s3://" + url[len("s3a://"):]
    if not url.startswith("s3://"):
        raise ValueError(f"Unsupported S3 URL: {url}")
    without = url[len("s3://"):]
    parts = without.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def enrich_pollution_file(
    s3_client,
    bucket: str,
    key: str,
    city_cache: Dict[str, Dict[str, int]],
    output_bucket: str,
    output_prefix: str,
):
    """
    1 CSV fajl: pročita sa S3 -> doda kolonu estimated_no_people -> upiše na drugi prefix na S3.
    city_cache: {date_str: {city_name: estimate}} da ne zove API više puta za isti datum.
    """
    print(f"Processing s3://{bucket}/{key}")

    # 1) Download CSV u memoriju
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(body))

    # 2) Datum iz key-ja (pollution_partitioned/date=YYYY-MM-DD/...)
    m = re.search(r"date=(\d{4}-\d{2}-\d{2})", key)
    if m:
        date_str = m.group(1)
        df["date"] = date_str
    else:
        # fallback: prvih 10 karaktera iz time_date kolone
        if "time_date" in df.columns:
            df["date"] = df["time_date"].astype(str).str.slice(0, 10)
            date_str = df["date"].iloc[0]
        else:
            raise RuntimeError(f"Could not determine date for key {key}")

    # 3) Grad iz location_name: "Ulica, City, Country" -> "City"
    if "location_name" not in df.columns:
        raise RuntimeError(f"Column 'location_name' not found in file {key}")

    df["city"] = (
        df["location_name"]
        .astype(str)
        .str.split(",")
        .str[1]
        .str.strip()
    )

    # 4) mapping city -> estimate za ovaj datum (API cache)
    if date_str not in city_cache:
        city_cache[date_str] = get_city_to_estimate(date_str)
    mapping = city_cache[date_str]

    # 5) Nova kolona
    df["estimated_no_people"] = df["city"].map(mapping)

    # 6) Snimi enriched CSV nazad na S3
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Izgradi output key sa istom strukturom ali pod drugim prefix-om
    input_path = os.environ.get("INPUT_PATH")
    _, input_prefix = parse_s3_url(input_path)
    _, output_prefix_only = parse_s3_url(output_prefix)

    if not key.startswith(input_prefix):
        # fallback: samo prelepi prefix
        out_key = output_prefix_only.rstrip("/") + "/" + key
    else:
        suffix = key[len(input_prefix):]
        out_key = output_prefix_only.rstrip("/") + "/" + suffix

    print(f"Writing enriched file to s3://{output_bucket}/{out_key}")
    s3_client.put_object(Bucket=output_bucket, Key=out_key, Body=csv_bytes)


def main():
    input_path = os.environ.get("INPUT_PATH")
    output_path = os.environ.get("OUTPUT_PATH")

    if not input_path or not output_path:
        raise RuntimeError("INPUT_PATH and OUTPUT_PATH must be set")

    session = get_boto_session()
    s3 = session.client("s3")

    in_bucket, in_prefix = parse_s3_url(input_path)
    out_bucket, out_prefix = parse_s3_url(output_path)

    print(f"Input bucket/prefix:  {in_bucket} / {in_prefix}")
    print(f"Output bucket/prefix: {out_bucket} / {out_prefix}")

    city_cache: Dict[str, Dict[str, int]] = {}

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=in_bucket, Prefix=in_prefix)

    for page in page_iter:
        contents = page.get("Contents", [])
        for obj in contents:
            # print("sss")
            key = obj["Key"]
            if key.endswith("/") or key.endswith(".parquet"):
                continue

            enrich_pollution_file(
                s3_client=s3,
                bucket=in_bucket,
                key=key,
                city_cache=city_cache,
                output_bucket=out_bucket,
                output_prefix=output_path,
            )

    print("Done.")


if __name__ == "__main__":
    main()
