
# VERSION 2 : Works with Event Bridge and is Production READY
import json
import boto3
import os

glue_client = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

def lambda_handler(event, context):
    try:
        # event recieved from event bridge
        print(f"Lambda function | Event recieved from event bridge | {event}") 
        # Extract bucket and key from EventBridge event
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]

        print(f"Lambda function | Received new object: | s3://{bucket}/{key}")

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--source_bucket": bucket,
                "--source_key": key
            }
        )

        print(f"Lambda function | Started Glue job | {response['JobRunId']}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Glue job started successfully",
                "jobRunId": response["JobRunId"]
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e