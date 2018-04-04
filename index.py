import json
import os
import boto3

sqs_client = boto3.client('sqs')
kinesis_client = boto3.client('kinesis')


def handler(event, context):

    try:
        events = pollSQS()
        stream_name = getKinesisName()

        for event in events:
            pushToKinesis(event, stream_name)

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

    response = sqs_client.receive_message(
        QueueURL=getSQSURL(),
        maxNumberOfMessages=10
    )

    events = []

    for message in response['Messages']:
        events.append(message['body'])

    return events


def pushToKinesis(event, stream_name):
    kinesis_client.put_record(
        StreamName=stream_name,
        data=json.dumps(event),
        PartitionKey='dummykey'
    )


def getSQSURL():
    return os.getenv("SQS_URL")


def getKinesisName():
    return os.getenv("KINESIS_URL")
