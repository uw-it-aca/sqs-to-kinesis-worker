import json
import os
import boto3
import time

sqs_client = boto3.client('sqs', region_name='us-west-2')
kinesis_client = boto3.client('firehose', region_name='us-west-2')


def handler(event, context):

    try:
        lambda_end = time.time() + 60

        while time.time() < lambda_end:
            response = pollSQS()
            events = []

            for message in response['Messages']:
                events.append(message['Body'])

            stream_name = getKinesisName()
            pushToKinesis(events, stream_name)

            deleteSQSMessages(response)

    except Exception as ex:
        return {
            'statusCode': 400,
            'body': str(ex),
        }

    return {
        'statusCode': 200,
        'body': 'Lambda execution terminated successfully'
        }


def pollSQS():
    """
    Polls SQS for events, returns a list of strings (the SQS payloads)
    """
    return sqs_client.receive_message(
        QueueUrl=getSQSURL(),
        MaxNumberOfMessages=10
    )


def pushToKinesis(events, stream_name):
    records = []

    for event in events:
        records.append({
            'Data': event
        })

    kinesis_client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records
    )


def getSQSURL():
    return os.getenv("SQS_URL")


def getKinesisName():
    return os.getenv("KINESIS_STREAM_NAME")


def deleteSQSMessages(response):
    entries = []

    for message in response['Messages']:
        entries.append({
            'Id': message['MessageId'],
            'ReceiptHandle': message['ReceiptHandle']
        })

    sqs_client.delete_message_batch(
        QueueUrl=getSQSURL(),
        Entries=entries
    )
