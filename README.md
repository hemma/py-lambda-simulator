# py-lambda-simulator

py-lambda-simulator is a Python library for running aws-lambda functions locally.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install py-lambda-simulator.

```bash
pip install py-lambda-simulator
```

## Usage

```python
import asyncio
import json
from asyncio import run

from py_lambda_simulator.lambda_simulator import AwsSimulator
from py_lambda_simulator.sqs_lambda_simulator import SqsLambdaSimulator, SqsEvent, LambdaSqsFunc


async def example(simulator, aws_simulator):
    queue = aws_simulator.create_sqs_queue("queue-name")

    def sqs_handler(event: SqsEvent, context):
        print(event["Records"][0]["body"])

    simulator.add_func(LambdaSqsFunc(name="test-sqs-lambda", queue_name="queue-name", handler_func=sqs_handler))

    async def send_msg():
        while True:
            aws_simulator.get_sqs_client().send_message(
                QueueUrl=queue["queue_url"], MessageBody=json.dumps({"test": 123})
            )
            await asyncio.sleep(1)

    await asyncio.gather(simulator.start(), send_msg())


if __name__ == '__main__':
    _simulator = SqsLambdaSimulator()
    _aws_simulator = AwsSimulator()
    run(example(_simulator, _aws_simulator))
```

For more examples see the tests.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)