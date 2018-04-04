import json
import datetime


def handler(event, context):

    try:
        events = pollSQS()
        kinesis_objects = formatKinesisEvents(events)

        for object in kinesis_objects:
            pushToKinesis(object)

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
    pass


def formatKinesisEvents(events):
    pass


def pushToKinesis(event):
    pass


def getSQSURL():
    pass


def getKinesisURL():
    pass
