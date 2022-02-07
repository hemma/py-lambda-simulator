import logging
from dataclasses import dataclass, asdict
from typing import Dict, Callable, Any

import boto3
from asyncer import asyncify

from py_lambda_simulator.lambda_config import LambdaConfig
from py_lambda_simulator.lambda_events import Record, SqsEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LambdaSqsFunc(LambdaConfig):
    queue_name: str
    handler_func: Callable[[SqsEvent, Any], None]
    max_number_of_messages: int = 1


class SqsLambdaSimulator:
    def __init__(self):
        self.sqs_client = None
        self.funcs: Dict[str, LambdaSqsFunc] = {}
        self.is_started = False

    def __get_sqs_client(self):
        if not self.sqs_client:
            self.sqs_client = boto3.client("sqs")
        return self.sqs_client

    def add_func(self, func: LambdaSqsFunc):
        if func.name in self.funcs:
            raise Exception(f"Function with name {func.name} already added.")
        self.funcs[func.name] = func

    def remove_func(self, name: str):
        self.funcs.pop(name)

    async def start(self):
        self.is_started = True
        while self.is_started:
            for name, func in self.funcs.items():
                queue_url = self.__get_sqs_client().get_queue_url(QueueName=func.queue_name)["QueueUrl"]
                messages = await asyncify(self.__get_sqs_client().receive_message)(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=func.max_number_of_messages,
                    WaitTimeSeconds=1,
                )
                if messages and "Messages" in messages and len(messages["Messages"]) > 0:
                    records = [
                        Record(
                            messageId=msg["MessageId"],
                            receiptHandle=msg["ReceiptHandle"],
                            body=msg["Body"],
                            attributes={},
                            messageAttributes={},
                            md5OfBody=msg["MD5OfBody"],
                            eventSource="sqs?",
                            eventSourceARN="sqsArn?",
                            awsRegion="region?",
                        )
                        for msg in messages["Messages"]
                    ]
                    logger.info(f"Invoking {name}")
                    func.handler_func(SqsEvent(Records=records), {})
                    for msg in messages["Messages"]:
                        self.__get_sqs_client().delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    def stop(self):
        self.is_started = False
