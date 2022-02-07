import json

import boto3
import pytest

from py_lambda_simulator.lambda_events import SqsEvent
from py_lambda_simulator.lambda_simulator import AwsSimulator
from py_lambda_simulator.sqs_lambda_simulator import LambdaSqsFunc, SqsLambdaSimulator


@pytest.mark.asyncio
async def test_should_invoke_lambda_func_on_message():
    aws_simulator = AwsSimulator()
    simulator = SqsLambdaSimulator()
    queue = aws_simulator.create_sqs_queue("queue-name")

    def sqs_handler(event: SqsEvent, context):
        assert json.loads(event["Records"][0]["body"]) == {"test": 123}
        simulator.stop()

    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda", queue_name="queue-name", handler_func=sqs_handler
        )
    )

    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue["queue_url"], MessageBody=json.dumps({"test": 123})
    )

    await simulator.start()
    aws_simulator.shutdown()


@pytest.mark.asyncio
async def test_should_invoke_lambda_func_with_batch_of_msgs():
    aws_simulator = AwsSimulator()
    simulator = SqsLambdaSimulator()
    queue = aws_simulator.create_sqs_queue("queue-name")

    def sqs_handler(event: SqsEvent, context):
        assert len(event["Records"]) == 2
        simulator.stop()

    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda",
            queue_name="queue-name",
            handler_func=sqs_handler,
            max_number_of_messages=2,
        )
    )

    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue["queue_url"], MessageBody=json.dumps({"test": 123})
    )
    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue["queue_url"], MessageBody=json.dumps({"test": 123})
    )

    await simulator.start()
    aws_simulator.shutdown()


@pytest.mark.asyncio
async def test_should_invoke_multiple_lambdas():
    aws_simulator = AwsSimulator()
    simulator = SqsLambdaSimulator()
    queue_1 = aws_simulator.create_sqs_queue("queue-name-1")
    queue_2 = aws_simulator.create_sqs_queue("queue-name-2")

    def sqs_handler_1(event: SqsEvent, context):
        assert json.loads(event["Records"][0]["body"]) == {"test": 123}

    def sqs_handler_2(event: SqsEvent, context):
        assert json.loads(event["Records"][0]["body"]) == {"test": 321}
        simulator.stop()

    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda-1",
            queue_name="queue-name-1",
            handler_func=sqs_handler_1,
        )
    )
    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda-2",
            queue_name="queue-name-2",
            handler_func=sqs_handler_2,
        )
    )

    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue_1["queue_url"], MessageBody=json.dumps({"test": 123})
    )
    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue_2["queue_url"], MessageBody=json.dumps({"test": 321})
    )

    await simulator.start()
    aws_simulator.shutdown()


@pytest.mark.asyncio
async def test_should_invoke_lambda_from_lambda():
    aws_simulator = AwsSimulator()
    simulator = SqsLambdaSimulator()
    queue_1 = aws_simulator.create_sqs_queue("queue-name-1")
    queue_2 = aws_simulator.create_sqs_queue("queue-name-2")

    def sqs_handler_1(event: SqsEvent, context):
        sqs_client = boto3.client("sqs")
        sqs_client.send_message(
            QueueUrl=queue_2["queue_url"],
            MessageBody=json.dumps({"test": "from_sqs_1"}),
        )

    def sqs_handler_2(event: SqsEvent, context):
        assert json.loads(event["Records"][0]["body"]) == {"test": "from_sqs_1"}
        simulator.stop()

    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda-1",
            queue_name="queue-name-1",
            handler_func=sqs_handler_1,
        )
    )
    simulator.add_func(
        LambdaSqsFunc(
            name="test-sqs-lambda-2",
            queue_name="queue-name-2",
            handler_func=sqs_handler_2,
        )
    )

    aws_simulator.get_sqs_client().send_message(
        QueueUrl=queue_1["queue_url"], MessageBody=json.dumps({"test": 123})
    )

    await simulator.start()
    aws_simulator.shutdown()
