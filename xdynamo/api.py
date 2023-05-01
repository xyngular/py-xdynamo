from typing import TypeVar, Optional, Any, Union, List, Dict, Iterable
from boto3.dynamodb.table import TableResource

from .db import DynamoDB

from xmodel.common.types import FieldNames
from xmodel.remote import XRemoteError
from xmodel.remote.api import RemoteApi
from xdynamo.client import DynClient
from xdynamo.common_types import DynKey
from xdynamo.fields import DynField
from xdynamo.structure import DynStructure
from xsentinels.default import Default
from xurls.url import Query
from xloop import xloop

M = TypeVar('M')


class DynApi(RemoteApi[M]):
    """
    Put things here that are only relevant for all DynModel's.

    The main change vs the base BaseApi class is filtering out of the JSON invalid blank values.

    Right now this model class is only used to transform json from/to Dynamo via
    the `json` and `update_from_json()` methods. See `xmodel.base.api.BaseApi` for more
    details.
    """
    client: DynClient[M]
    structure: DynStructure[DynField]

    def get_key(self, hash_key: Any, range_key: Optional[Any] = None) -> DynKey:
        """
        Easy way to generate a basic `DynKey` with hash_key, and range_key
        (if model has range key).

        If you don't provide a range-key and the model needs a range-key,
        will raise an `xmodel.remote.errors.XRemoteError`.
        """
        return DynKey(api=self, hash_key=hash_key, range_key=range_key)

    def get_via_id(
            self,
            id: Union[
                Union[int, str, DynKey],
                List[Union[int, str, DynKey]],
                Dict[str, Union[str, int]],
                List[Dict[str, Union[str, int]]],
            ],
            fields: FieldNames = Default,
            id_field: str = None,
            aux_query: Query = None
    ) -> Union[Iterable[M], M, None]:
        """
        Overridden in DynApi to convert any provided `DynKey` into string-based `id` and
        passing them to super and returning the result.

        See `xmodel.remote.api.RemoteApi.get_via_id` for more details on how this method works.

        Args:
            id: In addition to `str` and `int` values, you can also used `DynKey`(s) if you wish.
            fields: See `xmodel.remote.api.RemoteApi.get_via_id`
            id_field: See `xmodel.remote.api.RemoteApi.get_via_id`
            aux_query: See `xmodel.remote.api.RemoteApi.get_via_id`

        Returns:
            See `xmodel.remote.api.RemoteApi.get_via_id`
        """
        if id is None:
            return None

        is_list = isinstance(id, list)
        if is_list:
            id: Union[DynKey, int, str]
            new_id = [v.id if type(v) is DynKey else v for v in xloop(id)]
        else:
            new_id = id.id if type(id) is DynKey else id

        return super().get_via_id(new_id, fields=fields, id_field=id_field, aux_query=aux_query)

    @property
    def table(self) -> TableResource:
        """ Returns the boto3 table resource to use for our related DynModel.
            Don't cache or hang onto this, it's already properly cached for you via the current
            Context and so will work in every situation [unit-tests, config-changes, etc]...
        """

        if not self.structure.dyn_hash_key_name:
            raise XRemoteError(
                f"While constructing {self.structure.model_cls}, found no hash-key field. "
                f"You must have at least one hash-key field."
            )

        # Look it up each time in case config/service/env/context changes enough
        # for it to be different. DynamoDB will cache the table by name and so it's
        # very fast on subsequent lookups.
        table_name = self.structure.fully_qualified_table_name()

        # noinspection PyTypeChecker
        return DynamoDB.grab().table(name=table_name, table_creator=self._create_table)

    def _create_table(self, dynamo: DynamoDB) -> TableResource:
        return self.client.create_table()

    def json(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        See `xmodel.base.api.BaseApi.json`() for docs.

        This filters/modifies the json to make it compliant with Dynamo.
        Example: Dynamo does not like blank strings as attribute values.

        Right now this simply deletes the values out of the json dict if they are blank.
        If we start doing patch-like updates [and not full puts] with dynamo, we could
        easily adapt to detect when we are blanking a value so we can tell dynamo to
        remove the attribute in the item during the patch-like update.

        For now we only use full 'puts' to update dynamo items regardless of what actually
        changed [we can detect changes, I would like to support this sort of thing this when
        we add support for Dynamo Transactions, ie: grouping multiple creates/updates into a
        single request. With a transaction, it's possible to only update specific attributes in a
        dynamo row, while leaving any other attributes alone/in-tact.
        (https://app.shortcut.com/xyngular/story/13989) [consolidate dynamo code]
        """
        # This dict is a copy and can be mutated without a problem.
        json = super().json(**kwargs)
        if not json:
            return json

        for k in list(json.keys()):
            v = json[k]
            if isinstance(v, str) and not v:
                del json[k]

        return json
