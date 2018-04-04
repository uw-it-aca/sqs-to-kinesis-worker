import json
import os
import boto3

sqs_client = boto3.client('sqs')
kinesis_client = boto3.client('kinesis')


def handler(event, context):

    try:
        response = pollSQS()
        events = []

        for message in response['Messages']:
            events.append(message['Body'])

        stream_name = getKinesisName()

        for event in events:
            pushToKinesis(event, stream_name)

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


def pushToKinesis(event, stream_name):
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(event),
        PartitionKey='dummykey'
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
