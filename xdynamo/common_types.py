import dataclasses
from enum import Enum, auto as EnumAuto  # noqa
from typing import TYPE_CHECKING, Union, Any, Optional, Dict, Iterable, Tuple, Set, Type

from xdynamo.errors import XModelDynamoError, XModelDynamoNoHashKeyDefinedError
from xmodel.remote import XRemoteError
from xmodel.base.fields import Converter
from xurls.url import Query
from xloop import xloop

if TYPE_CHECKING:
    from xdynamo.api import DynApi
    from xdynamo.model import DynModel
    from xdynamo.fields import DynField

DynParams = Dict[str, Any]

_type_to_aws_type_map = {
    int: "N",
    float: "N",
    complex: "N",
    bool: "BOOL",
    str: "S",
    dict: "M",
    list: "L"
}

operator_alias_map = {
    "in": 'is_in',
    "exact": 'eq',
    "": 'eq',
    None: 'eq'
}
"""
Used to normalize some common operators (that we use in other systems) to the one used in Dynamo.
"""


def get_dynamo_type_from_python_type(some_type: Type) -> str:
    dyn_type = _type_to_aws_type_map.get(some_type)
    if dyn_type is not None:
        return dyn_type

    # todo: consider making the type we send to dynamo overridable
    #   default map is `get_dynamo_type_from_python_type`.
    #   generally, unless it's a basic type we default to `str`
    #   (example: datetime types use str).
    return _type_to_aws_type_map[str]


@dataclasses.dataclass(frozen=True, eq=True)
class DynKey:
    api: 'DynApi' = dataclasses.field(compare=False)
    # We only compare with `id`, this should represent our identity sufficiently.
    id: str = None
    hash_key: Union[Any] = dataclasses.field(default=None, compare=False)
    range_key: Optional[Any] = (
        dataclasses.field(default=None, compare=False)
    )
    range_operator: str = dataclasses.field(default=None, compare=False)
    require_full_key: bool = dataclasses.field(default=True, compare=False)

    def __str__(self):
        return self.id or ''

    @classmethod
    def via_obj(cls, obj: 'DynModel') -> 'DynKey':
        structure = obj.api.structure
        hash_name = structure.dyn_hash_key_name

        if not hash_name:
            raise XModelDynamoNoHashKeyDefinedError(
                f"While constructing {structure.model_cls}, found no hash-key field. "
                f"You must have at least one hash-key field."
            )

        hash_value = getattr(obj, hash_name)

        if hash_value is None:
            raise XModelDynamoError(
                f"Unable to get DynKey due to `None` for dynamo hash-key ({hash_value}) "
                f"on object {obj}."
            )

        range_name = structure.dyn_range_key_name
        range_value = None
        if range_name:
            range_value = getattr(obj, range_name)
            if range_value is None:
                raise XModelDynamoError(
                    f"Unable to get DynKey due to `None` for dynamo range-key ({range_name}) "
                    f"on object {obj}."
                )

        return DynKey(api=obj.api, hash_key=hash_value, range_key=range_value)

    def key_as_dict(self):
        structure = self.api.structure
        hash_field = structure.dyn_hash_field
        range_field = structure.dyn_range_field

        def run_converter(field: 'DynField', value) -> Any:
            converter = field.converter
            if not converter:
                return value

            return converter(
                self.api,
                Converter.Direction.to_json,
                field,
                value
            )

        # Append the keys for the items we want into what we will request.
        item_request = {hash_field.name: run_converter(hash_field, self.hash_key)}
        if range_field:
            item_request[range_field.name] = run_converter(range_field, self.range_key)
        return item_request

    def __post_init__(self):
        structure = self.api.structure
        delimiter = structure.dyn_id_delimiter
        range_name = structure.dyn_range_key_name
        need_range_key = bool(range_name)

        hash_key = self.hash_key
        range_key = self.range_key
        api = self.api

        _id = self.id
        if _id is not None and not isinstance(_id, str):
            # `self.id` must always be a string.
            # todo: Must check for standard converter method
            _id = str(_id)
            object.__setattr__(self, 'id', _id)

        require_full_key = self.require_full_key

        # First, figure out `self.id` if not provided.
        if not _id:
            if not hash_key:
                raise XRemoteError(
                    f"Tried to create DynKey with no id ({_id}) or no hash key ({hash_key})."
                )

            if require_full_key and need_range_key and not range_key:
                raise XRemoteError(
                    f"Tried to create DynKey with no id ({_id}) or no range key ({range_key})."
                )

            key_names = [(structure.dyn_hash_key_name, hash_key)]
            # Generate ID without delimiter to represent an entire hash-page (ie: any range value)
            if need_range_key and range_key is not None:
                key_names.append((range_name, range_key))

            keys = []
            for key_name, key_value in key_names:
                field = structure.get_field(key_name)
                converter = field.converter
                final_value = key_value
                if converter:
                    final_value = converter(
                        api,
                        Converter.Direction.to_json,
                        field,
                        key_value
                    )
                keys.append(final_value)

            _id = delimiter.join([str(x) for x in keys])
            object.__setattr__(self, 'id', _id)
        elif need_range_key and delimiter not in _id:
            raise XRemoteError(
                f"Tried to create DynKey with an `id` ({_id}) "
                f"that did not have delimiter ({delimiter}) in it. "
                f"This means we are missing the range-key part for field ({range_name}) "
                f"in the `id` that was provided.  Trying providing the id like this: "
                f'"{_id}{delimiter}'
                r'{range-key-value-goes-here}".'  # <-- want to directly-output the `{` part.
            )

        # If we got provided a hash-key directly, no need to continue any farther.
        if hash_key:
            if require_full_key and need_range_key and not range_key:
                raise XRemoteError(
                    f"Have hash_key ({hash_key}) without needed range_key while creating DynKey."
                )
            # We were provided the hash/range key already, as an optimization I don't use time
            # checking to see if they passed in the same values that they may have passed in `id`.
            return

        # They did not pass in hash_key, so we must parse the `id` they provided
        # and then set them on self.

        if not need_range_key:
            # If we don't need range key, there is no delimiter to look for.
            hash_key = _id
        else:
            split_id = _id.split(delimiter)
            if len(split_id) != 2:
                raise XRemoteError(
                    f"For dynamo table ({self.api.model_type}): Have id ({_id}) but delimiter "
                    f"({delimiter}) is either not present, or is in it more than once. "
                    f"'id' needs to contain exactly one hash and range key combined together "
                    f"with the delimiter, ie: 'hash-key-value{delimiter}range-key-value'. "
                    f"See xdynamo.dyn_connections documentation for more details on how "
                    f"this works."
                )
            # todo: Consider converting these `from_json`, like we convert `to_json`
            #   when we put the keys into the `id` (see above, where we generate `id` if needed)?
            hash_key = split_id[0]
            range_key = split_id[1]

        object.__setattr__(self, 'hash_key', hash_key)
        object.__setattr__(self, 'range_key', range_key)


class DynKeyType(Enum):
    """ Possible values for field option keys. """
    hash = EnumAuto()
    range = EnumAuto()


class _ProcessedQuery(Dict[str, Dict[str, Any]]):
    """
    A Dict/MutableMapping that will split the attribute name from the operator when given a
    `xurls.url.Query` via the `_ProcessedQuery.process_query` method.

    It will produce a dict/mapping that will map the attribute name in the query-key to
    another dict.  This second internal dict will map a normalized operator/condition name to
    it's value.

    Returning a `_ProcessedQuery` allows for an optimization.
    If we are asked to process a `_ProcessedQuery`, we will just return the result without
    having to actually check any of the values.

    Lets the RestClient pass around post-processed query results so it does not have to processes
    them a second time. This allows it to tell the difference between processed and unprocessed
    queries easily.

    We should only have keys with the attribute name as a string.
    Values should be a set with the operator(s) in them.
    """
    api: 'DynApi'

    @staticmethod
    def process_query(query: Query, *, api: 'DynApi') -> '_ProcessedQuery':
        if isinstance(query, _ProcessedQuery):
            # We already processed this in the past, just return it, no need to process it again.
            return query

        processed_query = _ProcessedQuery()
        processed_query.api = api
        for (k, v) in query.items():
            parts = k.split("__")
            name = parts[0]
            operator = None
            if len(parts) > 1:
                operator = parts[1]

            # When the operator is not provided, we guess the best one to use
            if operator is None:
                # If it's a list, we do the 'in' operator by default.
                if isinstance(v, list):
                    operator = "in"

            # Map alias operators to the standard one, otherwise keep current operator.
            operator = operator_alias_map.get(operator, operator)

            # Store value / operator in sub-dict...
            criterion = processed_query.setdefault(name, {})
            criterion[operator] = v
        return processed_query

    def generate_all_operator_values_for_name(self, name: str) -> Iterable[Tuple[str, str]]:
        """
        Produces a generator that will go though all operator/value combinations for
        the query param `name`. Each yield will be a tuple with operator as first,
        and value as second item in tuple.

        Args:
            name: name in query to use

        Returns: Tuple, first is operator and second is value.
            If the value is a list, will yield each value in list as a separate item.

        """
        for operator, values in self.get(name, ()).items():
            for value in xloop(values):
                yield operator, value

    def contains_only_keys(self):
        """ Return True if we have a key, and no other non-keys; Otherwise False. """
        api = self.api
        structure = api.structure
        key_names = {'id', structure.dyn_hash_key_name}
        range_key = structure.dyn_range_key_name
        if range_key:
            key_names.add(range_key)

        have_key = False
        for name in self:
            if name not in key_names:
                return False
            have_key = True

        return have_key

    _cached_dyn_keys = None

    def dyn_keys(self) -> Set[DynKey]:
        cached = self._cached_dyn_keys
        if cached is not None:
            return cached

        cached = self._generate_dyn_keys()
        self._cached_dyn_keys = cached
        return cached

    def _generate_dyn_keys(self) -> Set[DynKey]:
        """ Generate a set of DynKey's. This way they are uniquified. """
        api = self.api
        structure = api.structure
        hash_key = structure.dyn_hash_key_name
        range_key = structure.dyn_range_key_name
        dyn_keys = set()

        if hash_key and range_key in self and hash_key in self:
            hash_gen = self.generate_all_operator_values_for_name(hash_key)
            range_gen = list(self.generate_all_operator_values_for_name(range_key))

            # Go though every combination of hash + range keys....
            for hash_combo in hash_gen:
                for range_combo in range_gen:
                    range_operator = range_combo[0]
                    if range_operator == 'is_in':
                        range_operator = 'eq'

                    if range_operator == 'between':
                        next_range = next(range_gen, None)
                        if next_range[0] != 'between':
                            raise XRemoteError(
                                f"You must provide a second value for 'between' operator on range "
                                f"key ({range_key}), next value ({next_range[1]} had operator "
                                f"({next_range[0]})."
                            )
                        dyn_key = DynKey(
                            api=api,
                            hash_key=hash_combo[1],
                            range_key=[range_combo[1], next_range[1]],
                            range_operator='between'
                        )
                    else:
                        dyn_key = DynKey(
                            api=api,
                            hash_key=hash_combo[1],
                            range_key=range_combo[1],
                            range_operator=range_operator
                        )
                    dyn_keys.add(dyn_key)
        elif hash_key in self:
            for operator, value in self.generate_all_operator_values_for_name(hash_key):
                dyn_key = DynKey(
                    api=api,
                    hash_key=value,
                    # We want a key that represents an entire hash-page if there is a range-key.
                    require_full_key=False
                )
                dyn_keys.add(dyn_key)

        if 'id' != range_key and 'id' in self:
            for operator, value in self.generate_all_operator_values_for_name('id'):
                dyn_key = DynKey(
                    api=api,
                    id=value,
                    range_operator=operator
                )
                dyn_keys.add(dyn_key)

        return dyn_keys
