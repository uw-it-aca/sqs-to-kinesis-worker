import unittest
import os
from unittest.mock import patch
import index


class TestHandlerCase(unittest.TestCase):

    def test_response(self):
        print("testing response.")
        result = index.handler(None, None)
        print(result)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['headers']['Content-Type'], 'application/json')
        self.assertIn('Hello World', result['body'])


class TestConfigs(unittest.TestCase):

    def test_sqs_url(self):

        values = [["http://sqs", "http://sqs"]]

        for case in values:
            with patch.object(os, 'getenv', return_value=case[0]):
                self.assertEquals(index.getSQSURL(), case[1])


    def test_kinesis_url(self):

        values = [["http://kinesis", "http://kinesis"]]

        for case in values:
            with patch.object(os, 'getenv', return_value=case[0]):
                self.assertEquals(index.getKinesisURL(), case[1])


class TestPushToKinesis(unittest.TestCase):

    def test_push(self):
        pass


class TestPollSQS(unittest.TestCase):

    def test_poll(self):
        pass


if __name__ == '__main__':
    unittest.main()
