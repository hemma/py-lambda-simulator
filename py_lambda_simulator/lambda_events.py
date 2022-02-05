from dataclasses import dataclass
from typing import Optional, Dict, List, TypedDict


@dataclass
class Identity:
    caller: str
    user: str
    apiKey: str
    userArn: str
    cognitoAuthenticationType: str
    userAgent: str
    cognitoIdentityPoolId: str
    cognitoAuthenticationProvider: str
    sourceIp: str
    accountId: str
    cognitoIdentityId: str


@dataclass
class RequestContext:
    stage: str
    identity: Optional[Identity]
    resourceId: str
    apiId: str
    resourcePath: str
    httpMethod: str
    requestId: str
    accountId: str


@dataclass
class ApiGatewayProxyEvent:
    body: Optional[Dict]
    resource: str
    path: str
    headers: Dict[str, str]
    requestContext: RequestContext
    queryStringParameters: Dict[str, str]
    pathParameters: Dict[str, str]
    httpMethod: str
    stageVariables: Dict[str, str]


class Record(TypedDict):
    messageId: str
    receiptHandle: str
    body: str
    attributes: Dict[str, str]
    messageAttributes: Dict[str, str]
    md5OfBody: str
    eventSource: str
    eventSourceARN: str
    awsRegion: str


class SqsEvent(TypedDict):
    Records: List[Record]
