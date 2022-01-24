from dataclasses import dataclass, field, InitVar

import json
import warnings
from typing import Union, Dict, List

from dataclasses import dataclass


def _check_fields(item: Dict, cls):
    out = {}
    for k, v in item.items():
        normalized_key = _normalize_key(k)
        if normalized_key not in cls.__dataclass_fields__:
            warnings.warn(f"Found field \"{k}\" in input, which is not part of dataclass \"{cls.__name__}\"",
                          RuntimeWarning)
        else:
            out[normalized_key] = v
    return out


def _normalize_key(identifier: str):
    res = identifier.replace("-", "_")
    return res


@dataclass
class GenericDataClass:
    @classmethod
    def from_json(cls, input: Union[str, Dict]):
        if isinstance(input, str):
            input = json.loads(input)
        input = _check_fields(input, cls)
        return cls(**input)


@dataclass
class EventClass(GenericDataClass):
    @classmethod
    def from_event(cls, event):
        return cls.from_json(event)


@dataclass
class Identity(GenericDataClass):
    caller: str
    user: str
    api_key: str = field(init=False)
    user_arn: str = field(init=False)
    cognito_auth_type: str = field(init=False)
    user_agent: str = field(init=False)
    cognito_identity_pool_id: str = field(init=False)
    cognito_auth_provider: str = field(init=False)
    source_ip: str = field(init=False)
    account_id: str = field(init=False)
    cognito_identity_id: str = field(init=False)
    apiKey: InitVar[str] = field(repr=False, default=None)
    userArn: InitVar[str] = field(repr=False, default=None)
    cognitoAuthenticationType: InitVar[str] = field(repr=False, default=None)
    userAgent: InitVar[str] = field(repr=False, default=None)
    cognitoIdentityPoolId: InitVar[str] = field(repr=False, default=None)
    cognitoAuthenticationProvider: InitVar[str] = field(repr=False, default=None)
    sourceIp: InitVar[str] = field(repr=False, default=None)
    accountId: InitVar[str] = field(repr=False, default=None)
    cognitoIdentityId: InitVar[str] = field(repr=False, default=None)

    def __post_init__(self, apiKey: str, userArn: str, cognitoAuthenticationType: str, userAgent: str,
                      cognitoIdentityPoolId: str, cognitoAuthenticationProvider: str, sourceIp: str, accountId: str,
                      cognitoIdentityId: str):
        self.api_key = apiKey
        self.user_arn = userArn
        self.user_agent = userAgent
        self.cognito_auth_type = cognitoAuthenticationType
        self.cognito_auth_provider = cognitoAuthenticationProvider
        self.cognito_identity_id = cognitoIdentityId
        self.cognito_identity_pool_id = cognitoIdentityPoolId
        self.source_ip = sourceIp
        self.account_id = accountId


@dataclass
class RequestContext(GenericDataClass):
    stage: str
    identity: Identity
    resource_id: str = field(init=False)
    api_id: str = field(init=False)
    resource_path: str = field(init=False)
    http_method: str = field(init=False)
    request_id: str = field(init=False)
    account_id: str = field(init=False)
    resourceId: InitVar[str] = field(repr=False, default=None)
    apiId: InitVar[str] = field(repr=False, default=None)
    resourcePath: InitVar[str] = field(repr=False, default=None)
    httpMethod: InitVar[str] = field(repr=False, default=None)
    requestId: InitVar[str] = field(repr=False, default=None)
    accountId: InitVar[str] = field(repr=False, default=None)

    def __post_init__(self, resourceId: str, apiId: str, resourcePath: str, httpMethod: str, requestId: str,
                      accountId: str):
        self.request_id = requestId
        self.resource_id = resourceId
        self.resource_path = resourcePath
        self.account_id = accountId
        self.api_id = apiId
        self.http_method = httpMethod
        self.identity = Identity.from_json(self.identity)


@dataclass
class ApiGwProxyEvent(EventClass):
    body: str
    resource: str
    path: str
    headers: Dict[str, str]
    http_method: str = field(init=False)
    request_context: RequestContext = field(init=False)
    query_string_parameters: Dict[str, str] = field(init=False)
    path_parameters: Dict[str, str] = field(init=False)
    stage_variables: Dict[str, str] = field(init=False)
    requestContext: InitVar[Dict] = field(repr=False, default=None)
    queryStringParameters: InitVar[str] = field(repr=False, default=None)
    pathParameters: InitVar[str] = field(repr=False, default=None)
    httpMethod: InitVar[str] = field(repr=False, default=None)
    stageVariables: InitVar[str] = field(repr=False, default=None)

    def __post_init__(self, requestContext: str, queryStringParameters: str, pathParameters: str, httpMethod: str,
                      stageVariables: str):
        self.request_context = RequestContext.from_json(requestContext)
        self.query_string_parameters = queryStringParameters
        self.path_parameters = pathParameters
        self.http_method = httpMethod
        self.stage_variables = stageVariables

# @dataclass
# class S3Object(GenericDataClass):
#     etag: str = field(init=False, default=None)
#     sequencer: str = field(default=None)
#     key: str = field(default=None)
#     size: float = field(default=None)
#     eTag: InitVar[str] = field(repr=False, default=None)
#
#     def __post_init__(self, eTag: str):
#         self.etag = eTag
#
#
# @dataclass
# class S3Bucket(GenericDataClass):
#     arn: str = field(default=None)
#     name: str = field(default=None)
#     owner_identity: str = field(init=False, default=None)
#     ownerIdentity: InitVar[str] = field(repr=False, default=None)
#
#     def __post_init__(self, ownerIdentity: str):
#         self.owner_identity = ownerIdentity
#
#
# @dataclass
# class S3(GenericDataClass):
#     object: S3Object
#     bucket: S3Bucket
#     configuration_id: str = field(init=False, default=None)
#     s3_schemaversion: str = field(init=False, default=None)
#     configurationId: InitVar[str] = field(repr=False, default=None)
#     s3SchemaVersion: InitVar[str] = field(repr=False, default=None)
#
#     def __post_init__(self, configurationId: str, s3SchemaVersion: str):
#         self.configuration_id = configurationId
#         self.s3_schemaversion = s3SchemaVersion
#         self.object = S3Object.from_json(self.object)
#         self.bucket = S3Bucket.from_json(self.bucket)
#
#
# @dataclass
# class S3Record(GenericDataClass):
#     s3: S3
#     event_version: str = field(init=False)
#     event_source: str = field(init=False)
#     event_time: datetime = field(init=False)
#     event_name: str = field(init=False)
#     response_elements: Dict[str, str] = field(init=False, default=None)
#     aws_region: str = field(init=False)
#     user_identity: Dict[str, str] = field(init=False, default=None)
#     request_params: Dict[str, str] = field(init=False, default=None)
#     eventVersion: InitVar[str] = field(repr=False, default=None)
#     eventTime: InitVar[str] = field(repr=False, default=None)
#     requestParameters: InitVar[Dict[str, str]] = field(repr=False, default=None)
#     responseElements: InitVar[Dict[str, str]] = field(repr=False, default=None)
#     awsRegion: InitVar[str] = field(repr=False, default=None)
#     eventName: InitVar[str] = field(repr=False, default=None)
#     userIdentity: InitVar[Dict[str, str]] = field(repr=False, default=None)
#     eventSource: InitVar[str] = field(repr=False, default=None)
#
#     def __post_init__(self, eventVersion: str, eventTime: str, requestParameters: Dict[str, str],
#                       responseElements: Dict[str, str], awsRegion: str, eventName: str, userIdentity: Dict[str, str],
#                       eventSource: str):
#         self.event_name = eventName
#         self.event_time = arrow.get(eventTime).datetime
#         self.event_source = eventSource
#         self.event_version = eventVersion
#         self.response_elements = responseElements
#         self.aws_region = awsRegion
#         self.request_params = requestParameters
#         self.user_identity = userIdentity
#         self.s3 = S3.from_json(self.s3)
#
#
# @dataclass
# class S3Event(EventClass):
#     records: List[S3Record] = field(init=False)
#     first_record: S3Record = field(init=False)
#     Records: InitVar[List[Dict]] = field(repr=False, default=[])
#
#     def __post_init__(self, Records: List[Dict]):
#         self.records = [S3Record.from_json(item) for item in Records]
#         self.first_record = self.records[0]

# MessageAttribute = namedtuple("MessageAttribute", ['type', 'value'])
#
#
# def _parse_message_attributes(attrs):
#     return {att_name: MessageAttribute(att.get("Type", None),
#                                        att.get("Value", None)) for att_name, att in attrs.items()}
#
#
# @dataclass
# class SnsMessage(GenericDataClass):
#     signature_version: str = field(init=False)
#     timestamp: datetime = field(init=False)
#     signature: str = field(init=False)
#     subject: str = field(init=False)
#     message_id: str = field(init=False)
#     message: str = field(init=False)
#     type: str = field(init=False)
#     topic_arn: str = field(init=False)
#     signing_cert_url: str = field(init=False)
#     unsubscribe_url: str = field(init=False)
#     message_attributes: Dict = field(init=False, default=None)
#     SignatureVersion: InitVar[str] = field(repr=False, default=None)
#     Timestamp: InitVar[str] = field(repr=False, default=None)
#     Signature: InitVar[str] = field(repr=False, default=None)
#     MessageId: InitVar[str] = field(repr=False, default=None)
#     Message: InitVar[str] = field(repr=False, default=None)
#     Subject: InitVar[str] = field(repr=False, default=None)
#     Type: InitVar[str] = field(repr=False, default=None)
#     TopicArn: InitVar[str] = field(repr=False, default=None)
#     UnsubscribeUrl: InitVar[str] = field(repr=False, default=None)
#     SigningCertUrl: InitVar[str] = field(repr=False, default=None)
#     MessageAttributes: InitVar[Dict] = field(repr=False, default=None)
#
#     def __post_init__(self, SignatureVersion: str, Timestamp: str, Signature: str, MessageId: str, Message: str,
#                       Subject: str, Type: str, TopicArn: str, UnsubscribeUrl: str, SigningCertUrl: str,
#                       MessageAttributes: Dict):
#         self.signature_version = SignatureVersion
#         self.signature = Signature
#         self.topic_arn = TopicArn
#         self.type = Type
#         self.unsubscribe_url = UnsubscribeUrl
#         self.timestamp = arrow.get(Timestamp).datetime
#         self.message = Message
#         self.message_id = MessageId
#         self.subject = Subject
#         self.signing_cert_url = SigningCertUrl
#         self.message_attributes = _parse_message_attributes(
#             MessageAttributes) if MessageAttributes is not None else MessageAttributes
#
#
# @dataclass
# class SnsRecord(GenericDataClass):
#     event_source: str = field(init=False)
#     sns: SnsMessage = field(init=False)
#     event_version: str = field(init=False)
#     event_subscription_arn: str = field(init=False)
#     EventVersion: InitVar[str] = field(repr=False, default=None)
#     EventSubscriptionArn: InitVar[str] = field(repr=False, default=None)
#     Sns: InitVar[Dict] = field(repr=False, default={})
#     EventSource: InitVar[str] = field(repr=False, default=None)
#
#     def __post_init__(self, EventVersion: str, EventSubscriptionArn: str, Sns: Dict, EventSource: str):
#         self.event_source = EventSource
#         self.event_version = EventVersion
#         self.event_subscription_arn = EventSubscriptionArn
#         self.sns = SnsMessage.from_json(Sns)
#
#
# @dataclass
# class SnsEvent(EventClass):
#     records: List[SnsRecord] = field(init=False)
#     first_record: SnsRecord = field(init=False)
#     Records: InitVar[List] = field(repr=False, default=[])
#
#     def __post_init__(self, Records: List):
#         self.records = [SnsRecord.from_json(record) for record in Records]
#         self.first_record = self.records[0]
