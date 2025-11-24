import os
import json
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError
import requests


def get_tourist_token() -> str:
    secret_name = os.environ.get("SECRETS_NAME")
    region_name = os.environ.get("REGION_NAME")

    if not secret_name:
        raise RuntimeError("Environment variable SECRETS_NAME is not set")
    if not region_name:
        raise RuntimeError("Environment variable REGION_NAME is not set")

    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
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

    session = requests.Session()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {"date": for_date}

    resp = session.get(endpoint, headers=headers, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"API call failed with status {resp.status_code}: {resp.text}"
        ) from e

    return resp.json()


def get_estimates_for_date(for_date: str) -> List[Dict[str, Any]]:
    payload = fetch_city_estimates(for_date)
    info = payload.get("info", [])
    result: List[Dict[str, Any]] = []

    for city in info:
        result.append({
            "date": payload.get("for_date"),
            "city": city.get("name"),
            "estimated_no_people": city.get("estimated_no_people"),
        })

    return result
