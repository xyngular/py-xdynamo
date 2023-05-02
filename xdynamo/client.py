from typing import (
    TYPE_CHECKING, TypeVar, Union, Sequence, Iterable, Optional, List, Dict, Any, Set
)
from boto3.dynamodb import conditions
from boto3.dynamodb.table import BatchWriter, TableResource

from .db import DynamoDB
from xcon import xcon_settings
from logging import getLogger
from xmodel.common.types import FieldNames
from xmodel.remote import XRemoteError
from xmodel.base.fields import Converter
from xmodel.remote.client import RemoteClient
from xdynamo.common_types import (
    DynKey, DynParams, _ProcessedQuery, get_dynamo_type_from_python_type
)
from xdynamo.resources import _DynBatchResource
from xsentinels.default import Default
from xurls.url import URLStr, Query
from xloop import xloop

if TYPE_CHECKING:
    from xdynamo.model import DynModel
    from xdynamo.api import DynApi

log = getLogger(__name__)
M = TypeVar('M', bound='DynModel')


class DynClient(RemoteClient[M]):
    """
    Skeleton/Placeholder Class for future work, see story and the other classes in
    this file for more details: https://app.clubhouse.io/xyngular/story/13989
    """

    # This is only here to give IDE's a more concrete-class to use for type/code completion.
    # The type-hint is not otherwise used. That var is valid/gettable on an instance.
    # See `xmodel.remote.client.RemoteClient.api` for more details.
    api: 'DynApi[M]'

    def delete_obj(self, obj: Union['M', DynKey]):
        # raise NotImplementedError()

        # table_name = self.api.structure.dyn_name
        # params = {
        #     "TableName": table_name,
        #     # todo: find out if need "S", "N", level...
        #     "Key": DynKey.via_obj(obj).key_as_dict()
        #     # todo: put this into separate method, support '*params' for extra dynamo params
        #     #   like we do with some of the other methods.
        # }

        resource = self._table_or_batch_writer()

        # Get the DynKey
        # If we don't get a dyn-key, check for that and raise nicer, higher-level error.
        key = obj if isinstance(obj, DynKey) else DynKey.via_obj(obj)

        # To keep things simple, I am using 'put' which replaces entire item,
        # so get all properties of item regardless if they changed or not.
        # todo: Check for primary key and raise a nicer, higher-level exception in that case.
        resource.delete_item(Key=key.key_as_dict())

    def delete_objs(self, objs: Sequence[Union['M', DynKey]]):
        """ Uses a batch-writer to put the items.
            WAY more efficient then doing it one at a time.
            If you only give me one item, directly calls `delete_obj` without a batch-writer.
        """
        if not objs:
            return

        if len(objs) == 1:
            self.delete_obj(obj=objs[0])
            return

        with _DynBatchResource.grab().current_writer(create_if_none=True):
            for i in objs:
                self.delete_obj(obj=i)

    def send_objs(
            self,
            objs: Sequence['M'],
            *,
            url: URLStr = None,
            send_limit: int = None
    ):
        """
        Used to send any number of objects to Dynamo in as efficient a manner as possible.

        Args:
            objs: Objects to send to dynamo.
            url: Not used in Dynamo, ignore
            send_limit: Currently unused, we try to push as much as possible.

        """
        # For now, assume all objs come from same API, and so just get the first one...
        if not objs:
            return

        api = objs[0].api
        self._put_items(api=api, items=objs)

    def get(
            self,
            query: Query = None,
            *,
            top: int = None,
            fields: FieldNames = Default,
            **dynamo_params
    ) -> Iterable[M]:
        """
        This is the standard/abstract interface method that all RestClient's support.

        The idea behind this method is to figure out how to route this query in the most
        efficient manner we can do it in. If you want to guarantee a specific type of query
        you can use `DynClient.query`, etc.

        Generally, this is the best, generally method to use since it can adapt your request
        to the most efficient way to query Dynamo. The `DynApi` will use this method generally
        when it's asked to get something (for example, when using `DynApi.get_via_id`).

        For more info on how you can query, see:

        - [Advanced Queries](#advanced-queries)
            - [Examples](#examples_1)

        Here is the general process this method goes though to determine how to query dynamo
        for a given query:

        1. If provided query is empty.
              - Will paginate though all items in table efficiently; multiple items
                will be returned per-request in parallel.
                You'll get back a generator that gives you an object at time, but behind
                the scenes we are getting a page at a time from dynamo.
        2. We will try to batch it en-mass in parallel if we can.
           We can do this if one of the following is true:
              - Query only contains 'id' key; this is because an 'id' should have full primary key;
                meaning it will contain both hash and range key (if needed).
                You can provide a list of strings or `DynKey` objects.
                   - See `DynModel.id` for more details on how to use `str` with an 'id'.
                   - See `DynKey` for more details on what a composite primary-key as a `str` is
                     like.
              - Table only has a hash-key (no range/sort key) and you provide nothing but
                hash-keys (ie: no other attributes in query)
        3. Next, we see if we can use a Dynamo query via `DynClient.query`.
              - In the future, this will automatically querying a Secondary Index if one is
                available.
                (Not Implemented Yet, we will add this when needed)
        4. Next, fallback to using a Global Index if one is available.
              - (Not Implemented Yet, we will do it when we need it)
        5. If that's not possible, in the future we fallback to scan operation.
            - (Not Implemented Yet; This isn't implemented currently and will raise NotImplemented.
              Scan operations can be very slow and so it's not something we really want to do
              normally... I did implement this if getting everything, but with filters/attributes
              to query on I decided to wait until we need it before implementing this currently).

        .. todo:: Implement querying via Secondary/Global indexes; Scanning with query values.
            see above for details.



        Args:
            query: Dict keys are the attribute/key names and values are what
                to filter on for that name.  Operators after double `__` work just like you
                would expect for our xyngular API's... here is an example:

                ```python
                { "some_attr_name__gte": 2 }
                ```

                In this case, we look for `some_attr_name` greater than or equal to integer `2`.

                You MUST provide at least one value for the hash key of the table for right now.
                In the future, we will support doing a table-scan to support queries without
                a hash-key. But for right now it's required.

                For more information with examples see [Advanced Queries](#advanced-queries).

            top: This is supposed to only return this mean records; currently not implemented.
            fields: This is supposed to only retrieve provided field names in returned objects;
                currently not implemented.
            **dynamo_params: Extra parameters to include on the Dynamo request(s) that are
                generated. This is 100% optional.
        Returns:

        """

        if not query:
            # If no query.... just get all items via a bulk-scan.
            return self._get_all_items()

        # todo: We want basic logic in here to decide on batch_get vs query
        #       vs [eventually] dyn_scan.
        #       We sort of implement basic logic here to determin
        #       the best way to query dynamo so we have that logic mostly centralized instead
        #       of scattered everywhere. The logic is cetnered around how we will mostly be using
        #       dynamo. The times where we have exceptions, the outside world can just directly
        #       call `dyn_scan/query` them selves and customize the dynamo call more.
        #       `get` is the thing that figured out the best to this to in the general
        #       case.
        #   Future Vision of logic flow [this is not fully implemented or final yet]:
        #       1. Only have id/hash/range keys
        #           A. Have more than one id/hash/range keys.
        #               I. We can use the batch method.
        #           B. Use query
        #       2. Have id/hash + other attributes
        #           A. More than one hash
        #               I. Use several query calls.
        #           B. Only one hash
        #               I. Use single query call.
        #       3. Have no hash key, just other attributes.
        #           A. Must use `scan`.
        #               I. This is just something we will do later, don't need `scan` at moment.
        #

        query = _ProcessedQuery.process_query(query, api=self.api)
        structure = self.api.structure
        have_range_key = bool(structure.dyn_range_key_name)

        # Check to see if we only have key fields (without other filtering criteria);
        # if so we can do a batch-get, which is the fastest way to get a number of specific
        # values.  If we
        if query.contains_only_keys():
            dyn_keys = query.dyn_keys()

            # If we have no range-key, they all dyn-gets support get-batch.
            # it's only the lack of range-key, or a non 'eq' operator for range-key
            # that would disqualify a specific DynKey.
            #
            # todo: Some of these keys could support batch-get, we might consider getting
            #   the ones that do via batch-get, get others via query?

            all_support_batch_get = True
            if have_range_key:
                for dyn_key in dyn_keys:
                    if not dyn_key.range_key:
                        all_support_batch_get = False
                        break

                    if dyn_key.range_operator and dyn_key.range_operator not in ('eq', 'is_in'):
                        all_support_batch_get = False
                        break

            # We have just DynKey's, so we can do a batch get (no other conditions/filters).
            # This will automatically batch a 100 at a time for us via a generator.
            # Dynamo will fetch these in parallel!
            if all_support_batch_get and dyn_keys:
                return self.batch_get(keys=dyn_keys)

        # If we have some sort of key(s) we can use (a hash key with an optional range key).
        if query.dyn_keys():
            # todo: Support `top` and `fields`.
            # todo: Support multiple hash-keys [one query per hash key].
            # todo: unless this table does not have a range key [no hash/range to tie].

            # We have a query that has the hash-key in it, that's good enough to use a query.
            return self.query(query=query)

        # todo: Support 'scans' or always raise error? Scans are very expensive.
        # todo: Support Global + Secondary Indexes
        #  (secondary indexes are partially supported now, since they require the hash-key
        #   so we would currently do a query with a filter, and scan whole hash-key/page).
        raise NotImplementedError(
            "There are no hash-keys or id's in query, and I don't support auto routing to a scan "
            "operation or global indexes at the moment. "
            "This is what you need to do without a hash-key/id. "
            "Scan operations are slow, so for being conservative to prevent accidentally doing "
            "one. For now you need to explicitly do them unless your "
            "retrieving all records (ie: blank query). This may change in the future."
            "\n"
            "TODO: Support Global Indexes - if there is a global index "
            "then we should use them if query is using the global-index hash-key and other"
            "attributes are in global-index as well."
        )

    def batch_get(
            self, keys: Iterable[DynKey], **params: DynParams
    ) -> Iterable[M]:
        """
        Will fetch keys in largest batch at a time it can from Dynamo;
        Dynamo will fetch each page of values in parallel!

        We split up the keys into 100 increments at a time automatically right now
        For each unique hash key in the set of keys provided, Dynamo will parallel fetch
        the keys (if two keys have the same hash but different range key, dynamo will do
        them sequentially).

        In the future, we may attempt to fetch multiple 100 blocks of keys asynchronously.
        As the returned generator/iterable is gone though to increase the speed.
        We don't do that yet.

        Args:
            keys (Iterable[DynKey]): Keys to fetch.
                Can be of any size, a generator will be returned to minimize memory use.

                .. tip:: If you pass in a `set`, we will be slightly more efficient.
                    We need to ensure the results are uniquified, if you pass a set we can skip
                    doing it.

            **params: An optional set of extra parameters to include in request to Dynamo,
                if so desired.

        Returns: An Iterable/Generator that will efficiently paginate though the results for you.

        """
        structure = self.api.structure
        hash_name = structure.dyn_hash_key_name
        range_name = structure.dyn_range_key_name

        if not keys:
            return []

        def batch_pagination_generator(items):
            if not items:
                return xloop()

            table_name = structure.fully_qualified_table_name()

            # We want to merge our items with anything that could already be there...
            copy_params = {**params}
            req_items_param = copy_params.setdefault('RequestItems', {})
            table_items = req_items_param.setdefault(table_name, {})
            table_keys = table_items.setdefault('Keys', [])
            table_keys.extend(items)

            # batch_get_item is only available on dynamo-resource, not table-resource.
            return self._paginate_all_items_generator(
                method='batch_get_item',
                params=copy_params,
                use_table=False
            )

        # Go though all the keys and grab them 100 at a time from Dynamo.
        # Dynamo only supports a max of 100 keys at a time when doing a 'batch_get_item'.
        items_requested = []
        have_range = bool(range_name)

        uniquified_keys = keys
        if not isinstance(keys, set):
            uniquified_keys = set(uniquified_keys)
        uniquified_keys = list(uniquified_keys)

        for i in range(0, len(uniquified_keys), 100):
            key_subset = [key.key_as_dict() for key in set(uniquified_keys[i:i + 100])]
            for x in batch_pagination_generator(list(key_subset)):
                yield x

    def _parse_keys_from_query(self, query: Query) -> Optional[List[DynKey]]:
        query = _ProcessedQuery.process_query(query, api=self.api)

    def _parse_id(
            self, _id: Union[str, Iterable[str]]
    ) -> List[DynKey]:
        keys = []
        if not _id:
            return keys

        api = self.api
        for current_id in xloop(_id, iterate_dicts=True):
            keys.append(DynKey(api=api, id=current_id))

        if not keys:
            return keys

        return keys

    def query(self, query: Query = None, **dynamo_params: DynParams) -> Iterable[M]:
        """
        Forces `DynClient` to use a query. If you want a way for client to automatically
        figure out the best way to execute your query, use one of these instead:

        - `DynApi.get`
        - `DynClient.get`

        For more info see:

        - [Advanced Queries](#advanced-queries)
            - [Examples](#examples_1)

        For a quick summary on how to provide query,
        see 'query' argument doc (just a few lines down).
        But I would highly recommend looking at [Advanced Queries](#advanced-queries) for
        more details with examples!

        Args:
            api (DynApi): BaseApi object to use, this is how we know the table name, model class,
                etc.
            query (Query): You can give a simple dict here, modeled after how the standard rest-api
                query dict's work. Dict keys are the attribute/key names and values are what
                to filter on for that name.  Operators after double `__` work just like you
                would expect for our xyngular API's... here is an example:

                ```python
                { "some_attr_name__gte": 2 }
                ```

                In this case, we look for `some_attr_name` greater than or equal to integer `2`.

                You MUST provide at least one value for the hash key of the table when using
                `query` or boto3/dynamo will raise an exception.

                This is an easy way to fill out `KeyConditionExpression` and/or `FilterExpression`.
                This method can figure out which attribute goes with which one and construct
                both expressions as needed.

                For more information with examples see [Advanced Queries](#advanced-queries).

            **params (DynParams): You can provide other standard boto3 query parameters here as you
                need. If you provide both dynamo_params and query, the ones in query will overwrite
                ones in dynamo_params if there is a conflict;

                The `query` param could use either a `KeyConditionExpression` or `FilterExpression`
                depending on what attributes are in the query dict.

        Yields:
            M: The next object we got from dynamo. This method returns a generator
                that will eventually go through all the results from dynamo in a memory-efficient
                manner.
        """

        # todo: support in/lists as values....
        # todo: support `id`.

        structure = self.api.structure
        hash_key = structure.dyn_hash_key_name
        query = _ProcessedQuery.process_query(query, api=self.api)

        # if 'id' in query:
        #
        #
        # hash_key in query:

        # 1. If we have 'id', iterate though that and get DynKey's out of them
        # 2. Look at hash/range keys and try to match them up if they are lists into DynKey's
        # 3. Considering auto-finding out if we have a list of keys and can just do batch-get

        # Query for each dyn-key we find.
        keys = query.dyn_keys()
        if not keys:
            raise XRemoteError(
                "query got called with a query that had no valid DynKey's in it. "
                "This means we could not find any part(s) of the primary key we could use to do "
                "a query on (a Dynamo query requires at least a hash-key). "
                ""
                "If you have no conditions and just want to "
                "simply retrieve every item in the table use `DynClient.get` with no parameters. "
                ""
                "If you do have conditions/filters in query you need to do a `DynClient.dyn_scan` "
                "and have Dynamo scan the entire table. This will allow dynamo to evaluate your "
                "conditions on every item in the table."
            )

        for dyn_key in query.dyn_keys():
            params = {**dynamo_params}
            self._add_conditions_from_query(
                query=query,
                params=params,
                dyn_key=dyn_key
            )

            for value in self._paginate_all_items_generator(method='query', params=params):
                yield value

    def _add_conditions_from_query(
            self,
            query: Query,
            params: DynParams,
            dyn_key: DynKey = None,
    ):
        query = _ProcessedQuery.process_query(query, api=self.api)
        key_names: Set[str] = set()
        api = self.api
        structure = self.api.structure

        if dyn_key:
            key_names.add('id')
            key_names.add(structure.dyn_hash_key_name)
            range_name = structure.dyn_range_key_name
            if range_name:
                key_names.add(range_name)

        def add_criterion(cond_list, condition_base, name, operator, value):
            # It just so happens the basic Django filter operators are generally named the same
            # as the ones in the boto3 dynamo library. So we grab the condition/operator
            # via the same names. _ProcessedQuery will normalize the names for us.

            # Construct condition by allocating base, grabbing operator and assigning value.
            operator = getattr(condition_base(name), operator, None)
            condition = None
            if operator:
                field = structure.get_field(name)
                if field and field.converter:
                    value = field.converter(
                        api,
                        Converter.Direction.to_json,
                        field,
                        value
                    )
                condition = operator(value)

            # If we found a condition operator, use it.
            # Otherwise we construct and raise a helpful error message.
            if operator is not None:
                cond_list.append(condition)
                return

            # Get all available conditions/operators from boto3 class so we can list them
            # in the exception message.
            available = [
                f for f in dir(condition_base)
                if callable(getattr(condition_base, f)) and not f.startswith("__")
            ]
            supplemental_msg = ""
            if condition_base is conditions.Key:
                supplemental_msg = (
                    f"Attr ({name}) is part of primary key, there are reduced "
                    f"operators available for keys when using a query. "
                    f"We could route this to a 'scan' operation, that would work.... "
                    f"Right now we don't automatically route this to a 'scan' operation "
                    f"because that's much slower and you probably really are wanting "
                    f"to do a query. You can use `dyn_scan` your self directly if that's "
                    f"what you really want to do. Or we could implement an Option/Flag to "
                    f"auto-route to a scan operation when needed."
                )

            raise XRemoteError(
                f"Using unknown boto3/dynamo operator ({operator}), "
                f"for query on attr ({name}); "
                f"the available ones are ({available}). "
                f"{supplemental_msg}"
            )

        filters = []
        keys = []

        for (name, criterion) in query.items():
            if name in key_names:
                # This is handled later...
                continue
            for (operator, value) in criterion.items():
                add_criterion(
                    cond_list=filters,
                    condition_base=conditions.Attr,
                    name=name,
                    operator=operator,
                    value=value
                )

        # Add the dyn-key conditions if needed...
        if dyn_key:
            add_criterion(
                cond_list=keys,
                condition_base=conditions.Key,
                name=structure.dyn_hash_key_name,
                operator='eq',
                value=dyn_key.hash_key
            )

            range_key = dyn_key.range_key
            if range_key:
                operator = dyn_key.range_operator or 'eq'
                # If we have an 'in' operator, we translate that to 'eq' for this purpose.
                # We should be called with separate values if there is more than one dyn_key,
                # and so are 'simulating' the `is_in` operator aspect.
                if operator == 'is_in':
                    operator = 'eq'
                add_criterion(
                    cond_list=keys,
                    condition_base=conditions.Key,
                    name=structure.dyn_range_key_name,
                    operator=operator,
                    value=range_key
                )

        params_to_mod = (('KeyConditionExpression', keys), ('FilterExpression', filters))
        for (param_key, exp_list) in params_to_mod:
            for key in exp_list:
                exp = params.get(param_key)
                exp = exp & key if exp is not None else key
                params[param_key] = exp

    def _table_or_batch_writer(self) -> Union[BatchWriter, TableResource]:
        """
        Gets either a table or a batch-writer. So you should only call methods that are
        supported by a BatchWriter on this [since it could be one].
        All BatchWriter methods are also supported by a Dynamo TableResource so you'll be safe
        as long as you limit calls to what BatchWriter supports.
        """
        batch_writer = _DynBatchResource.grab().current_writer()
        if batch_writer:
            return batch_writer.batch_writer(api=self.api)

        return self.api.table

    def _get_item_by_id(self, _id: str) -> Optional[M]:
        api = self.api
        structure = api.structure

        if structure.dyn_range_key_name:
            split_id = _id.split(structure.dyn_id_delimiter)
            if len(split_id) != 2:
                raise XRemoteError(
                    f"Have id ({_id}) but delimiter ({structure.dyn_id_delimiter}) is in it "
                    f"more/less than once and {api} needs both a hash and range key."
                )
            hash_key, range_key = split_id
        else:
            hash_key = _id
            range_key = None

        # Construct an expression with hash and if needed, range keys.
        key_exp = conditions.Key(structure.dyn_hash_key_name).eq(hash_key)
        if range_key:
            key_exp = key_exp & conditions.Key(structure.dyn_range_key_name).eq(range_key)

        response = api.table.query(
            ConsistentRead=True,
            KeyConditionExpression=key_exp
        )

        data = None
        db_datas = response['Items']
        if db_datas:
            data = db_datas[0]

        if not data:
            return None

        return api.model_type(data)

    # todo: BaseModel objects are capable of letting us know if something actually changed or not.
    #       At some point take advantage of that.
    #       This would allow us to prevent putting an unchanged item into dynamo [saves cost].
    def _put_item(self, item: 'DynModel'):
        """
        Put item into dynamo-table.

        :param item:
            Item to put in.
        """
        # Check to see if there is anything I actually need to send.
        if not item.api.json(only_include_changes=True):
            log.debug(f"Dynamo - {item} did not have any changes to send, skipping.")
            return

        resource = self._table_or_batch_writer()

        structure = self.api.structure
        hash_name = structure.dyn_hash_key_name
        if not getattr(item, hash_name, None):
            raise XRemoteError(f"Item {item} needs a value for hash key ({hash_name}).")

        range_name = structure.dyn_range_key_name
        if range_name and not getattr(item, range_name, None):
            raise XRemoteError(f"Item {item} needs a value for range key ({range_name}).")

        # To keep things simple, I am using 'put' which replaces entire item,
        # so get all properties of item regardless if they changed or not.
        # todo: Check for primary key and raise a nicer, higher-level exception in that case.
        resource.put_item(Item=item.api.json())

    def _put_items(self, api: 'DynApi', items: Sequence['DynModel']):
        """ Uses a batch-writer to put the items.
            WAY more efficient then doing it one at a time.
            If you only give me one item, directly calls `put_item` without a batch-writer.
        """
        if not items:
            return

        if len(items) == 1:
            self._put_item(item=items[0])
            return

        with _DynBatchResource.grab().current_writer(create_if_none=True):
            for i in items:
                self._put_item(item=i)

    def _get_all_items(self):
        return self._paginate_all_items_generator(method='scan', params={})

    def _paginate_all_items_generator(
            self, method: str,
            params: Dict[str, Any],
            use_table=True,
    ) -> Iterable[M]:
        api = self.api
        model_type = api.model_type
        # Get table name, and also ensures table exists.
        table = api.table
        table_name = table.name
        resource = table if use_table else DynamoDB.grab().db

        while True:
            table_method = getattr(resource, method)
            response = table_method(**params)
            last_key = response.get('LastEvaluatedKey', None)

            db_datas = response.get('Items')

            if not db_datas:
                responses: Dict[str, List] = response.get("Responses")
                if responses:
                    db_datas = responses[table_name]
                else:
                    db_datas = tuple()

            for data in db_datas:
                yield model_type(data)

            if last_key:
                params['ExclusiveStartKey'] = last_key
                continue

            unprocessed = response.get('UnprocessedKeys')
            if not unprocessed:
                return

            # We need to try the fetch again for the remaining items...
            # todo: Boto/AWS recommend an exponential backoff when we retry in this case...
            #   see BatchGetItem / batch_get_item
            params['RequestItems'] = unprocessed
            continue

    def create_table(self):
        """
        This is mainly here to create table when mocking aws for unit tests. If the table
        really does not exist in reality, this also can create it.

        Time To Live Notes:

        We can't enable TimeToLive during table creation, we have to wait until after it's
        created. This is a minor issue, since the table will still function correctly,
        the queries will still filter out expired items like normal.  The only difference
        is we could get charged extra for storage we are not using. We need to still filter
        items out of our queries because deletion does not happen immediately [could take up
        to 48 hours].

        At this point, it's expected that you'll have to go into the AWS dynamo console
        to setup automatic TimeToLive item deletion for a table if you want this to create it for
        you.

        In reality, serverless framework is expected to setup the real tables for services that
        are running directly in aws; and that's where you should setup the TTL stuff for real
        tables.
        """
        structure = self.api.structure
        hash_key = structure.dyn_hash_key_name
        hash_type = structure.get_field(hash_key).type_hint
        key_schemas = [
            # Partition Key
            {'AttributeName': hash_key, 'KeyType': 'HASH'}
        ]
        attribute_definitions = [
            {
                'AttributeName': hash_key,
                'AttributeType': get_dynamo_type_from_python_type(hash_type)
            }
        ]

        # If we have a range-key, add that in.
        range_key = structure.dyn_range_key_name
        if range_key:
            range_type = structure.get_field(range_key).type_hint
            key_schemas.append({
                'AttributeName': range_key,
                'KeyType': 'RANGE'
            })
            attribute_definitions.append({
                'AttributeName': range_key,
                'AttributeType': get_dynamo_type_from_python_type(range_type)
            })

        return DynamoDB.grab().db.create_table(
            TableName=structure.fully_qualified_table_name(),
            KeySchema=key_schemas,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',
            Tags=[{'Key': 'DDBTableGroupKey', 'Value': xcon_settings.service}]
        )
