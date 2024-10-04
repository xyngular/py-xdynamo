from typing import TypeVar, Optional, Any, Union, List, Dict, Iterable, TYPE_CHECKING
from uuid import UUID

from boto3.dynamodb.table import TableResource

from .db import DynamoDB

from xmodel.common.types import FieldNames
from xmodel.remote import XRemoteError
from xmodel.remote.api import RemoteApi
from xdynamo.client import DynClient, DynClientOptions
from xdynamo.common_types import DynKey
from xdynamo.fields import DynField
from xdynamo.structure import DynStructure
from xsentinels.default import Default, DefaultType
from xurls.url import Query
from xloop import xloop


if TYPE_CHECKING:
    from .model import DynModel
    M = TypeVar('M', bound=DynModel)
else:
    M = TypeVar('M')


def lazy_load_types_for_dyn_api(cls):
    """
    Lazy load our circular reference right before it's needed.
    This is put in as DynModel's lazy_loader, xyn_model will call us here when needed.

    See `xyn_model.base.model.BaseModel.__init_subclass__` and it's lazy_loader argument
    for more details.
    """
    if 'DynModel' not in globals():
        from xdynamo.model import DynModel
        globals()['DynModel'] = DynModel


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
    # This type-hint is only for IDE, `RemoteApi` does not use it
    # (self.model_type value is passed in when RemoteApi is allocated, in __init__ method).
    model: M

    def get(
            self,
            query: Query = None,
            *,
            top: int = None,
            fields: FieldNames | DefaultType | None = Default,
            allow_scan=False,
            consistent_read: bool | DefaultType = Default,
    ) -> Iterable[M]:
        """
        Convenience method for the `self.client.get` method.

        Generally, will get/query/scan/batch-get; generally,
        the client will figure out the best way to get the items based on the provided query.
        If the query is None or blank, it will grab everything in the table.

        Args:
            query: A dict with key the field, with optional double-underscore and operation
                (such as `{'some_field__beginswith': 'search-value'}`).
                The value is what to search for.
                If you give this a list, it implies a `__in` operator, and will do an OR
                on the values in the list.
            top:
            fields:
            allow_scan: Defaults to False, which means this will raise an exception if a scan
                is required to execute your get.  Set to True to allow a scan if needed
                (it will still do a query, batch-get, etc; if it can, it only does a scan
                if there is no other choice).

                If the query is blank or None will still do a scan regardless of what you pass
                (to return all items in the table).
            consistent_read: Defaults to Model.api.structure.dyn_consistent_read_by_default,
                which can be set via class arguments when DynModel subclass is defined.

                You can use this to override the model default. True means we use consistent
                reads, otherwise false.

        Returns:

        """
        return self.client.get(query, top=top, fields=fields, allow_scan=allow_scan, consistent_read=consistent_read)

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
                    int | str | UUID,
                    List[int | str | UUID],
                    Dict[str, str | int | UUID],
                    List[Dict[str, str | int | UUID]],
            ],
            fields: FieldNames = Default,
            id_field: str = None,
            aux_query: Query = None,
            consistent_read: bool | DefaultType = Default,
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
            consistent_read: Defaults to Model.api.structure.dyn_consistent_read_by_default,
                which can be set via class arguments when DynModel subclass is defined.

                You can use this to override the model default. True means we use consistent
                reads, otherwise false.

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

        if consistent_read is Default:
            return super().get_via_id(new_id, fields=fields, id_field=id_field, aux_query=aux_query)

        # If we have a non-Default consistent-read, then temporarily inject the option.
        dyn_options = DynClientOptions()
        dyn_options.consistent_read = consistent_read
        with dyn_options:
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

    def delete(self, *, condition: Query = None):
        """
        REQUIRES associated model object [see self.model].

        Convenience method to delete this single object in API.

        If you pass in a condition, it will be evaluated on the dynamodb-service side and the
        deletes will only happen if the condition is met.
        This can help prevent race conditions if used correctly.

        If there is a batch-writer currently in use, we will try to use that to batch the deletes.

        Keep in mind that if you pass in a `condition`, we can't use the batch write.
        We will instead send of a single-item delete request with the condition attached
        (bypassing any current batch-writer that may be in use).

        Args:
            condition: Conditions in query will be sent to dynamodb; object will only be deleted
                if the conditions match.
                See doc comments on `xyn_model_dynamo.client.DynClient.delete_obj` for more
                details.
        """

        # Normally I would call super().delete(...) and have the super have a **kwargs it can
        # simply pass along (so it can do any normal checks it does).
        # Don't want to modify another library right now, so for now copying/pasting the code here.
        model = self.model
        if model.id is None:
            raise XRemoteError(
                f"A delete was requested for an object that had no id for ({model})."
            )

        self.client.delete_obj(model, condition=condition)

    def send(self, *, condition: Query = None):
        """
        """

        # Normally I would call super().delete(...) and have the super have a **kwargs it can
        # simply pass along (so it can do any normal checks it does).
        # Don't want to modify another library right now, so for now copying/pasting the code here.
        model = self.model
        if model.id is None:
            raise XRemoteError(
                f"A send was requested for an object that had no id for ({model})."
            )

        self.client.send_objs([model], condition=condition)
