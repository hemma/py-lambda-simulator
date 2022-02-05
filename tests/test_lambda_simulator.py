import asyncio

import boto3
import pytest

from py_lambda_simulator.lambda_simulator import SqsLambdaSimulator, LambdaSqsFunc, HttpLambdaSimulator, \
    LambdaHttpFunc, LambdaPureHttpFunc, AwsSimulator
from py_lambda_simulator.lambda_events import ApiGatewayProxyEvent, SqsEvent

import json


class TestAwsSimulator:

    def test_should_create_dynamodb_table(self):
        aws_simulator = AwsSimulator()
        aws_simulator.create_dynamodb_table('test-table', key_schema=[
            {
                'AttributeName': 'pk',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'sk',
                'KeyType': 'RANGE'  # Sort key
            }
        ], attribute_definition=[
            {
                'AttributeName': 'pk',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S'
            },

        ])

        aws_simulator.get_dynamodb_client().put_item(TableName='test-table',
                                                     Item={'pk': {'S': '1'}, 'sk': {'S': 'value'}})

        item_response = aws_simulator.get_dynamodb_client().get_item(TableName='test-table',
                                                                     Key={'pk': {'S': '1'}, 'sk': {'S': 'value'}})

        assert item_response is not None
        assert item_response['Item'] == {'pk': {'S': '1'}, 'sk': {'S': 'value'}}


class TestHttpLambdaSimulator:

    async def test_should_invoke_lambda_func_on_post(self, aiohttp_client, loop):
        simulator = HttpLambdaSimulator()
        client = await aiohttp_client(simulator.app)

        called = {'has_been_called': False}

        def http_handler(event: ApiGatewayProxyEvent, context):
            called['has_been_called'] = True
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(event.body)
            }

        simulator.add_func(
            LambdaHttpFunc(name='test-http-lambda', method='POST', path='/http', handler_func=http_handler))

        async def async_assert():
            resp = await client.post('/http', json={'key': 'value'})
            assert resp.status == 200
            assert await resp.json() == {'key': 'value'}

            await simulator.stop()

        await asyncio.gather(simulator.start(), async_assert())
        assert called['has_been_called']

    async def test_should_invoke_multiple_lambdas(self, aiohttp_client):
        simulator = HttpLambdaSimulator()
        client = await aiohttp_client(simulator.app)
        called = {'has_been_called': False}

        def http_handler(event: ApiGatewayProxyEvent, context):
            called['has_been_called'] = True
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(event.body)
            }

        def http_handler_2(event: ApiGatewayProxyEvent, context):
            return {
                "statusCode": 200
            }

        simulator.add_func(
            LambdaHttpFunc(name='test-http-lambda', method='POST', path='/http', handler_func=http_handler))
        simulator.add_func(
            LambdaHttpFunc(name='test-http-lambda-2', method='GET', path='/http', handler_func=http_handler_2))

        async def async_assert_post():
            resp = await client.post('/http', json={'key': 'value'})
            assert resp.status == 200
            assert await resp.json() == {'key': 'value'}

            await simulator.stop()

        async def async_assert_get():
            resp = await client.post('/http', json={'key': 'value'})
            assert resp.status == 200
            assert await resp.json() == {'key': 'value'}

        async def async_assert():
            await async_assert_post()
            await async_assert_get()
            await simulator.stop()

        await asyncio.gather(simulator.start(), async_assert())
        assert called['has_been_called']

    async def test_should_invoke_pure_lambda_func_on_post(self, aiohttp_client):
        simulator = HttpLambdaSimulator()
        client = await aiohttp_client(simulator.app)

        called = {'has_been_called': False}

        def handler(event, context):
            called['has_been_called'] = True

        simulator.add_func(
            LambdaPureHttpFunc(name='test-http-lambda', method='GET', path='/http', handler_func=handler))

        async def async_assert():
            resp = await client.get('/http')
            assert resp.status == 200

            await simulator.stop()

        await asyncio.gather(simulator.start(), async_assert())
        assert called['has_been_called']


class TestSqsLambdaSimulator:

    @pytest.mark.asyncio
    async def test_should_invoke_lambda_func_on_message(self):
        aws_simulator = AwsSimulator()
        simulator = SqsLambdaSimulator()
        queue = aws_simulator.create_sqs_queue('queue-name')

        def sqs_handler(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {'test': 123}
            simulator.stop()

        simulator.add_func(LambdaSqsFunc(name='test-sqs-lambda', queue_name='queue-name', handler_func=sqs_handler))

        aws_simulator.get_sqs_client().send_message(QueueUrl=queue['queue_url'], MessageBody=json.dumps({"test": 123}))

        await simulator.start()
        aws_simulator.shutdown()

    @pytest.mark.asyncio
    async def test_should_invoke_multiple_lambdas(self):
        aws_simulator = AwsSimulator()
        simulator = SqsLambdaSimulator()
        queue_1 = aws_simulator.create_sqs_queue('queue-name-1')
        queue_2 = aws_simulator.create_sqs_queue('queue-name-2')

        def sqs_handler_1(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {'test': 123}

        def sqs_handler_2(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {'test': 321}
            simulator.stop()

        simulator.add_func(
            LambdaSqsFunc(name='test-sqs-lambda-1', queue_name='queue-name-1', handler_func=sqs_handler_1))
        simulator.add_func(
            LambdaSqsFunc(name='test-sqs-lambda-2', queue_name='queue-name-2', handler_func=sqs_handler_2))

        aws_simulator.get_sqs_client().send_message(QueueUrl=queue_1['queue_url'],
                                                    MessageBody=json.dumps({"test": 123}))
        aws_simulator.get_sqs_client().send_message(QueueUrl=queue_2['queue_url'],
                                                    MessageBody=json.dumps({"test": 321}))

        await simulator.start()
        aws_simulator.shutdown()

    @pytest.mark.asyncio
    async def test_should_invoke_lambda_from_lambda(self):
        aws_simulator = AwsSimulator()
        simulator = SqsLambdaSimulator()
        queue_1 = aws_simulator.create_sqs_queue('queue-name-1')
        queue_2 = aws_simulator.create_sqs_queue('queue-name-2')

        def sqs_handler_1(event: SqsEvent, context):
            sqs_client = boto3.client('sqs')
            sqs_client.send_message(QueueUrl=queue_2['queue_url'], MessageBody=json.dumps({'test': 'from_sqs_1'}))

        def sqs_handler_2(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {'test': 'from_sqs_1'}
            simulator.stop()

        simulator.add_func(
            LambdaSqsFunc(name='test-sqs-lambda-1', queue_name='queue-name-1', handler_func=sqs_handler_1))
        simulator.add_func(
            LambdaSqsFunc(name='test-sqs-lambda-2', queue_name='queue-name-2', handler_func=sqs_handler_2))

        aws_simulator.get_sqs_client().send_message(QueueUrl=queue_1['queue_url'],
                                                    MessageBody=json.dumps({"test": 123}))

        await simulator.start()
        aws_simulator.shutdown()


class TestLambdaSimulator:

    @pytest.mark.asyncio
    async def test_should_invoke_lambdas(self, aiohttp_client):
        aws_simulator = AwsSimulator()
        sqs_simulator = SqsLambdaSimulator()
        http_simulator = HttpLambdaSimulator()

        client = await aiohttp_client(http_simulator.app)

        queue = aws_simulator.create_sqs_queue('queue-name')

        has_been_called = {'sqs': False, 'http': False}

        def sqs_handler(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {'test': 123}
            sqs_simulator.stop()
            has_been_called['sqs'] = True

        def http_handler(event: ApiGatewayProxyEvent, context):
            has_been_called['http'] = True
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(event.body)
            }

        sqs_simulator.add_func(LambdaSqsFunc(name='test-sqs-lambda', queue_name='queue-name', handler_func=sqs_handler))
        http_simulator.add_func(
            LambdaHttpFunc(name='test-http-lambda', method='POST', path='/http', handler_func=http_handler))

        aws_simulator.get_sqs_client().send_message(QueueUrl=queue['queue_url'], MessageBody=json.dumps({"test": 123}))

        async def call_http_lambda():
            resp = await client.post('/http')
            assert resp.status == 200
            await http_simulator.stop()

        await asyncio.gather(sqs_simulator.start(), http_simulator.start(), call_http_lambda())

        assert has_been_called['sqs']
        assert has_been_called['http']

        aws_simulator.shutdown()
