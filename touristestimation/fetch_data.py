import os
import json
from datetime import date
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError
import requests

# $env:AWS_PROFILE = "new"
# $env:ENDPOINT = "https://rq5fbome43vbdgq7xoe7d6wbwa0ngkgr.lambda-url.eu-west-1.on.aws/"
# $env:SECRETS_NAME = "tourist_estimate_token"
# $env:REGION_NAME = "eu-west-1"
# added this in powershell and executed it from there

ENDPOINT = os.environ.get("ENDPOINT")
SECRET_NAME = os.environ.get("SECRETS_NAME")
SECRET_REGION = os.environ.get("REGION_NAME")


def get_tourist_token(secret_name: str = SECRET_NAME, region_name: str = SECRET_REGION) -> str:
    if not secret_name:
        raise RuntimeError("Environment variable SECRETS_NAME is not set")
    if not region_name:
        raise RuntimeError("Environment variable REGION_NAME is not set")

    client = boto3.client("secretsmanager", region_name=region_name)

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


def fetch_city_estimates(for_date: str) -> Dict[str, Any]:
    if not ENDPOINT:
        raise RuntimeError("Environment variable ENDPOINT is not set")

    session = requests.Session()

    token = get_tourist_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {"date": for_date}

    resp = session.get(ENDPOINT, headers=headers, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"API call failed with status {resp.status_code}: {resp.text}"
        ) from e

    return resp.json()


def pretty_print_response(payload: Dict[str, Any]):
    for_date = payload.get("for_date", "N/A")
    info: List[Dict[str, Any]] = payload.get("info", [])

    print(f"Tourist visit estimates for date: {for_date}")
    if not info:
        print("No city estimates returned.")
        return

    for city in info:
        name = city.get("name", "UNKNOWN")
        est = city.get("estimated_no_people", "N/A")
        print(f"  - {name}: {est} people")


def main():
    date_str = date.today().strftime("%Y-%m-%d")

    payload = fetch_city_estimates(date_str)
    pretty_print_response(payload)


if __name__ == "__main__":
    main()
