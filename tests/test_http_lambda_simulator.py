import asyncio
import json

from py_lambda_simulator.lambda_events import ApiGatewayProxyEvent
from py_lambda_simulator.lambda_simulator import (
    HttpLambdaSimulator,
    LambdaHttpFunc,
    LambdaPureHttpFunc,
)


async def test_should_invoke_lambda_func_on_post(aiohttp_client, loop):
    simulator = HttpLambdaSimulator()
    client = await aiohttp_client(simulator.app)

    called = {"has_been_called": False}

    def http_handler(event: ApiGatewayProxyEvent, context):
        called["has_been_called"] = True
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(event.body),
        }

    simulator.add_func(
        LambdaHttpFunc(
            name="test-http-lambda",
            method="POST",
            path="/http",
            handler_func=http_handler,
        )
    )

    async def async_assert():
        resp = await client.post("/http", json={"key": "value"})
        assert resp.status == 200
        assert await resp.json() == {"key": "value"}

        await simulator.stop()

    await asyncio.gather(simulator.start(), async_assert())
    assert called["has_been_called"]


async def test_should_invoke_multiple_lambdas(aiohttp_client):
    simulator = HttpLambdaSimulator()
    client = await aiohttp_client(simulator.app)
    called = {"has_been_called": False}

    def http_handler(event: ApiGatewayProxyEvent, context):
        called["has_been_called"] = True
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(event.body),
        }

    def http_handler_2(event: ApiGatewayProxyEvent, context):
        return {"statusCode": 200}

    simulator.add_func(
        LambdaHttpFunc(
            name="test-http-lambda",
            method="POST",
            path="/http",
            handler_func=http_handler,
        )
    )
    simulator.add_func(
        LambdaHttpFunc(
            name="test-http-lambda-2",
            method="GET",
            path="/http",
            handler_func=http_handler_2,
        )
    )

    async def async_assert_post():
        resp = await client.post("/http", json={"key": "value"})
        assert resp.status == 200
        assert await resp.json() == {"key": "value"}

        await simulator.stop()

    async def async_assert_get():
        resp = await client.post("/http", json={"key": "value"})
        assert resp.status == 200
        assert await resp.json() == {"key": "value"}

    async def async_assert():
        await async_assert_post()
        await async_assert_get()
        await simulator.stop()

    await asyncio.gather(simulator.start(), async_assert())
    assert called["has_been_called"]


async def test_should_invoke_pure_lambda_func_on_post(aiohttp_client):
    simulator = HttpLambdaSimulator()
    client = await aiohttp_client(simulator.app)

    called = {"has_been_called": False}

    def handler(event, context):
        called["has_been_called"] = True

    simulator.add_func(
        LambdaPureHttpFunc(
            name="test-http-lambda",
            method="GET",
            path="/http",
            handler_func=handler,
        )
    )

    async def async_assert():
        resp = await client.get("/http")
        assert resp.status == 200

        await simulator.stop()

    await asyncio.gather(simulator.start(), async_assert())
    assert called["has_been_called"]
