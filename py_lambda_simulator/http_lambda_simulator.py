import logging
from dataclasses import dataclass
from typing import Callable, Any, Dict, Literal, Union

from aiohttp import web

from py_lambda_simulator.lambda_config import LambdaConfig
from py_lambda_simulator.lambda_events import RequestContext, ApiGatewayProxyEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LambdaPureHttpFunc(LambdaConfig):
    method: Literal["GET", "POST"]
    path: str
    handler_func: Callable[[Any, Any], None]


@dataclass
class LambdaHttpFunc(LambdaConfig):
    method: Literal["GET", "POST"]
    path: str
    handler_func: Callable[[ApiGatewayProxyEvent, Any], Any]


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
                    if request.can_read_body:
                        body = await request.json()
                    else:
                        body = None

                    if type(f) == LambdaHttpFunc:
                        event = ApiGatewayProxyEvent(
                            body=body,
                            resource="resource",
                            path=request.rel_url,
                            headers=request.headers,
                            requestContext=RequestContext(
                                stage="stage",
                                identity=None,
                                resourceId="resId",
                                apiId="apiId",
                                resourcePath=request.rel_url,
                                httpMethod=request.method,
                                requestId="reqId",
                                accountId="accId",
                            ),
                            queryStringParameters={},
                            pathParameters={},
                            httpMethod=request.method,
                            stageVariables={},
                        )
                        lambda_response = f.handler_func(event, {})
                        return web.Response(
                            status=lambda_response["statusCode"],
                            headers=lambda_response.get("headers"),
                            body=lambda_response.get("body"),
                        )
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
        site = web.TCPSite(self.runner, "localhost", 8080)
        await site.start()

    async def stop(self):
        await self.app.shutdown()
        await self.runner.cleanup()
