import asyncio
import logging

from typing import Union

import boto3
from moto import mock_sqs, mock_dynamodb2

from py_lambda_simulator.http_lambda_simulator import HttpLambdaSimulator, LambdaHttpFunc, LambdaPureHttpFunc
from py_lambda_simulator.sqs_lambda_simulator import SqsLambdaSimulator, LambdaSqsFunc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AwsSimulator:

    def __init__(self):
        self.__sqs_mock = mock_sqs()
        self.__dynamodb_mock = mock_dynamodb2()
        self.__sqs_mock.start()
        self.__dynamodb_mock.start()
        self.__sqs_client = None
        self.__dynamodb_client = None

    def get_sqs_client(self):
        if not self.__sqs_client:
            self.__sqs_client = boto3.client("sqs")

        return self.__sqs_client

    def get_dynamodb_client(self):
        if not self.__dynamodb_client:
            self.__dynamodb_client = boto3.client('dynamodb')

        return self.__dynamodb_client

    def create_dynamodb_table(self, table_name, key_schema, attribute_definition):
        self.get_dynamodb_client().create_table(TableName=table_name,
                                                KeySchema=key_schema,
                                                AttributeDefinitions=attribute_definition,
                                                ProvisionedThroughput={
                                                    'ReadCapacityUnits': 10,
                                                    'WriteCapacityUnits': 10
                                                }
                                                )
        return table_name

    def create_sqs_queue(self, queue_name: str):
        client = self.get_sqs_client()
        create_resp = client.create_queue(QueueName=queue_name)
        queue_url = create_resp['QueueUrl']

        return {'queue_name': queue_name, 'queue_url': queue_url}

    def shutdown(self):
        self.__sqs_mock.stop()
        self.__dynamodb_mock.stop()


class Simulator:

    def __init__(self):
        self.sqs = SqsLambdaSimulator()
        self.http = HttpLambdaSimulator()

    def add_func(self, func: Union[LambdaSqsFunc, LambdaPureHttpFunc, LambdaHttpFunc]):
        if type(func) == LambdaSqsFunc:
            self.sqs.add_func(func)
        elif type(func) == LambdaHttpFunc or type(func) == LambdaPureHttpFunc:
            self.http.add_func(func)

    def remove_func(self, name: str):
        self.sqs.remove_func(name)
        self.http.remove_func(name)

    async def start(self):
        return asyncio.gather(self.sqs.start(), self.http.start())

    async def stop(self):
        self.sqs.stop()
        await self.http.stop()
