import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Any, Dict, Literal, Union

import boto3
from aiohttp import web
from asyncer import asyncify
from moto import mock_sqs, mock_dynamodb2

from src.lambda_events import RequestContext, ApiGatewayProxyEvent, Record, SqsEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LambdaConfig:
    name: str


@dataclass
class LambdaSqsFunc(LambdaConfig):
    queue_name: str
    handler_func: Callable[[SqsEvent, Any], None]


@dataclass
class LambdaPureHttpFunc(LambdaConfig):
    method: Literal['GET', 'POST']
    path: str
    handler_func: Callable[[Any, Any], None]


@dataclass
class LambdaHttpFunc(LambdaConfig):
    method: Literal['GET', 'POST']
    path: str
    handler_func: Callable[[ApiGatewayProxyEvent, Any], Any]


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


class HttpLambdaSimulator:

    def __init__(self):
        self.app = web.Application()
        self.runner = None
        self.funcs: Dict[str, Union[LambdaHttpFunc, LambdaPureHttpFunc]] = {}
        self.is_started = False

    def add_func(self, func: Union[LambdaHttpFunc, LambdaPureHttpFunc]):
        self.funcs[func.name] = func

    def remove_func(self, name: str):
        self.funcs.pop(name)

    async def start(self):
        for func in self.funcs.values():
            logger.info(f"Adding func {func}")

            def add(f):
                async def __lambda_func_http(request):
                    if request.has_body:
                        body = await request.json()
                    else:
                        body = None

                    if type(f) == LambdaHttpFunc:
                        event = ApiGatewayProxyEvent(body=body, resource="resource", path=request.rel_url,
                                                     headers=request.headers,
                                                     requestContext=RequestContext(stage="stage", identity=None,
                                                                                   resourceId="resId",
                                                                                   apiId="apiId",
                                                                                   resourcePath=request.rel_url,
                                                                                   httpMethod=request.method,
                                                                                   requestId="reqId",
                                                                                   accountId="accId"),
                                                     queryStringParameters={},
                                                     pathParameters={}, httpMethod=request.method,
                                                     stageVariables={})
                        lambda_response = f.handler_func(event, {})
                        return web.Response(status=lambda_response["statusCode"],
                                            headers=lambda_response.get("headers"),
                                            body=lambda_response.get("body"))
                    elif type(f) == LambdaPureHttpFunc:
                        f.handler_func({}, {})
                        return web.Response(status=200)

                if func.method == "GET":
                    self.app.add_routes([web.get(f.path, __lambda_func_http)])
                elif func.method == "POST":
                    self.app.add_routes([web.post(f.path, __lambda_func_http)])

            self.app.router._frozen = False
            add(func)
            self.app.router._frozen = True

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, 'localhost', 8080)
        await site.start()

    async def stop(self):
        await self.app.shutdown()
        await self.runner.cleanup()


class SqsLambdaSimulator:

    def __init__(self):
        self.sqs_client = boto3.client('sqs')
        self.funcs: Dict[str, LambdaSqsFunc] = {}
        self.is_started = False

    def add_func(self, func: LambdaSqsFunc):
        if func.name in self.funcs:
            raise Exception(f"Function with name {func.name} already added.")
        self.funcs[func.name] = func

    def remove_func(self, name: str):
        self.funcs.pop(name)

    async def start(self):
        self.is_started = True
        while self.is_started:
            for func in self.funcs.values():
                queue_url = self.sqs_client.get_queue_url(QueueName=func.queue_name)['QueueUrl']
                try:
                    messages = await asyncify(self.sqs_client.receive_message)(QueueUrl=queue_url,
                                                                               MaxNumberOfMessages=1,
                                                                               WaitTimeSeconds=1)
                    if messages and 'Messages' in messages and len(messages['Messages']) > 0:
                        records = [
                            Record(messageId=msg['MessageId'], receiptHandle=msg['ReceiptHandle'], body=msg['Body'],
                                   attributes={}, messageAttributes={},
                                   md5OfBody=msg['MD5OfBody'], eventSource="sqs?", eventSourceARN="sqsArn?",
                                   awsRegion="region?") for msg in messages['Messages']]
                        func.handler_func(SqsEvent(Records=records), {})
                        for msg in messages['Messages']:
                            self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
                except KeyboardInterrupt:
                    break

    def stop(self):
        self.is_started = False


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
