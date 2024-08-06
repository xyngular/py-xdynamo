from xmodel.remote import XRemoteError


class XModelDynamoError(Exception):
    pass


class XModelDynamoNoHashKeyDefinedError(XRemoteError):
    pass
