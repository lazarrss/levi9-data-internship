import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["bucket"]
    key = event["key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    return {
        "statusCode": 200,
        "content": content
    }
