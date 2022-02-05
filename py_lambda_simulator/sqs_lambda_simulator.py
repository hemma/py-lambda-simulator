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
