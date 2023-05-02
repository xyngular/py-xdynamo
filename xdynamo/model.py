from typing import TypeVar, Optional
from xmodel.remote import XRemoteError
from xmodel.remote.model import RemoteModel
from xdynamo.api import DynApi
from xdynamo.common_types import DynKey
from xmodel.base.model import Self


M = TypeVar('M')


class DynModel(RemoteModel, dyn_name=None, dyn_service=None):
    """
    Used to easily parse/generate JSON from xyn_sdk model's for use in Dynamo.
    So it will take advantage of all the other features of the xyn_sdk models.
    This includes automatically converting dates to/from strings, converting strings
    to numbers, looking up child-objects from other tables automatically, etc.
    It also will modify the results JSON so remove blank values; to easily prevent
    these sorts of errors in dynamo boto3 library.

    We pass in None for name/service to indicate we don't have an associated table,
    that we are more of an abstract class.
    """
    api: DynApi[Self]
    id: str

    @property
    def id(self) -> Optional[str]:
        # We could do some intelligent caching, but for now just calculate each time.
        try:
            return DynKey.via_obj(self).id
        except XRemoteError:
            return None

    @id.setter
    def id(self, value):
        structure = self.api.structure
        if type(value) is str:
            parsed_value = value.split('|')
            hash_value = parsed_value[0]
            range_value = parsed_value[1] if len(parsed_value) == 2 else None

            self.__setattr__(structure.dyn_hash_field.name, hash_value)
            if range_value:
                self.__setattr__(structure.dyn_range_field.name, range_value)
            return

        raise NotImplementedError(
            "Read-only for now, but want to support it. "
            "Supporting it would involve parsing ID with DynKey, and taking hash/range key "
            "components and setting them on the proper attributes."
            "\n\n"
            "Also, want to eventually support for using 'id' as a HashField "
            "(ie: a single/only key called 'id' in dynamo-db)"
        )
