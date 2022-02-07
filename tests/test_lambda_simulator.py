import asyncio

import boto3
import pytest

from py_lambda_simulator.lambda_simulator import (
    SqsLambdaSimulator,
    LambdaSqsFunc,
    HttpLambdaSimulator,
    LambdaHttpFunc,
    LambdaPureHttpFunc,
    AwsSimulator,
)
from py_lambda_simulator.lambda_events import ApiGatewayProxyEvent, SqsEvent

import json


class TestAwsSimulator:
    def test_should_create_dynamodb_table(self):
        aws_simulator = AwsSimulator()
        aws_simulator.create_dynamodb_table(
            "test-table",
            key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},  # Partition key
                {"AttributeName": "sk", "KeyType": "RANGE"},  # Sort key
            ],
            attribute_definition=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
        )

        aws_simulator.get_dynamodb_client().put_item(
            TableName="test-table", Item={"pk": {"S": "1"}, "sk": {"S": "value"}}
        )

        item_response = aws_simulator.get_dynamodb_client().get_item(
            TableName="test-table", Key={"pk": {"S": "1"}, "sk": {"S": "value"}}
        )

        assert item_response is not None
        assert item_response["Item"] == {"pk": {"S": "1"}, "sk": {"S": "value"}}


class TestLambdaSimulator:
    @pytest.mark.asyncio
    async def test_should_invoke_lambdas(self, aiohttp_client):
        aws_simulator = AwsSimulator()
        sqs_simulator = SqsLambdaSimulator()
        http_simulator = HttpLambdaSimulator()

        client = await aiohttp_client(http_simulator.app)

        queue = aws_simulator.create_sqs_queue("queue-name")

        has_been_called = {"sqs": False, "http": False}

        def sqs_handler(event: SqsEvent, context):
            assert json.loads(event["Records"][0]["body"]) == {"test": 123}
            sqs_simulator.stop()
            has_been_called["sqs"] = True

        def http_handler(event: ApiGatewayProxyEvent, context):
            has_been_called["http"] = True
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(event.body),
            }

        sqs_simulator.add_func(
            LambdaSqsFunc(
                name="test-sqs-lambda",
                queue_name="queue-name",
                handler_func=sqs_handler,
            )
        )
        http_simulator.add_func(
            LambdaHttpFunc(
                name="test-http-lambda",
                method="POST",
                path="/http",
                handler_func=http_handler,
            )
        )

        aws_simulator.get_sqs_client().send_message(
            QueueUrl=queue["queue_url"], MessageBody=json.dumps({"test": 123})
        )

        async def call_http_lambda():
            resp = await client.post("/http")
            assert resp.status == 200
            await http_simulator.stop()

        await asyncio.gather(
            sqs_simulator.start(), http_simulator.start(), call_http_lambda()
        )

        assert has_been_called["sqs"]
        assert has_been_called["http"]

        aws_simulator.shutdown()
