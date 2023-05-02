from xdynamo import DynKey, DynModel, HashField, RangeField, DynBatch, DynField
from xmodel import JsonModel, Field
from typing import List, Dict, Union, Optional, Type, Any, Callable
import moto
from typing import TypeVar
import pytest
import moto
from moto import mock_dynamodb
import dataclasses
from xmodel.remote import XRemoteError
import datetime as dt
from dateutil.relativedelta import relativedelta

T = TypeVar('T')
M = TypeVar('M')

simple_obj_hash_key = "my-hash1"
simple_obj_range_key = "my-range1"


def utc() -> dt.datetime:
    """ Returns a datetime with current utc. """
    return dt.datetime.now(dt.timezone.utc)

# ----------------------
# ***** My Models ******


class SubObj(JsonModel):
    sub_name: str = Field(include_in_repr=True)
    queue: bool


class ItemWithRangeKey(DynModel, dyn_name=None):
    hash_field: str = HashField()
    range_field: str = RangeField()
    name: str
    basic_bool: bool
    items: List[Dict[str, str]]

    do_not_compare: bool = False
    """ Not stored in dynamo, is set to True if assert_with should not assert values,
        This is used with objects that I want to directly delete with only keys and not other
        values.
    """

    def assert_with(self, other: "ItemWithRangeKey"):
        assert other is not self, "We should not be comparing exact same object, caching object?"

        if self.do_not_compare or other.do_not_compare:
            return
        assert self.name == other.name
        assert self.id == other.id
        assert self.basic_bool == other.basic_bool


class ItemWithRangeKeyForStr(
    ItemWithRangeKey,
    dyn_name="testItemWithRangeKey"
):
    hash_field: str = HashField()
    range_field: str = RangeField()


class ItemWithRangeKeyForInt(
    ItemWithRangeKey,
    dyn_name="testItemWithRangeWithIntKeys",
):
    hash_field: int = HashField()
    range_field: int = RangeField()


class ItemWithRangeKeyForDateTime(
    ItemWithRangeKey,
    dyn_name="testItemWithRangeWithIntKeys",
):
    hash_field: dt.datetime = HashField()
    range_field: dt.datetime = RangeField()


class ItemOnlyHash(DynModel, dyn_name="testItemOnlyHash"):
    hash_field_id: str = HashField()
    basic_int: int
    dict_list: List[Dict[str, str]]
    basic_str: str
    child_item: ItemWithRangeKeyForStr
    sub_item: SubObj


# ------------------------
# ***** My Fixtures ******

def default_generate_hash_value(obj: 'ObjTestValues', field: DynField, value: Any, x: int):
    # Could use field converter, but keeping it simple.
    return value + field.type_hint(x)


def datetime_generate_hash_value(
        obj: 'ObjTestValues',
        field: DynField,
        value: dt.datetime,
        x: int
):
    # Could use field converter, but keeping it simple.
    return value + relativedelta(seconds=x)


ValueGenerator = Callable[["ObjTestValues", DynField, Any, int], Any]


@dataclasses.dataclass
class ObjTestValues:
    model_cls: Type[ItemWithRangeKey]
    hash_value: Any
    range_value: Any
    value_generator: ValueGenerator = default_generate_hash_value

    def generate_hash_value(self, x: int):
        field = self.model_cls.api.structure.get_field('hash_field')
        return self.value_generator(self, field, self.hash_value, x)

    def generate_range_value(self, x: int):
        field = self.model_cls.api.structure.get_field('range_field')
        return self.value_generator(self, field, self.range_value, x)


@dataclasses.dataclass
class ObjTestDef(ObjTestValues):
    get_via_id: Union[dict, str, ItemWithRangeKeyForStr, DynKey] = None


@pytest.fixture(params=[
    ObjTestValues(
        model_cls=ItemWithRangeKeyForStr,
        hash_value=simple_obj_hash_key,
        range_value=simple_obj_range_key
    ),
    ObjTestValues(
        model_cls=ItemWithRangeKeyForInt,
        hash_value=2,
        range_value=4
    ),
    ObjTestValues(
        model_cls=ItemWithRangeKeyForDateTime,
        hash_value=utc(),
        range_value=utc(),
        value_generator=datetime_generate_hash_value
    ),
])
def simple_obj_values(request) -> ObjTestValues:
    """ Used as base-cases to start with, with various range/hash values/types. """
    return request.param


@pytest.fixture(params=[
    lambda v: {"hash_field": v.hash_value, "range_field": v.range_value},
    lambda v: f"{v.hash_value}|{v.range_value}",
    lambda v: v.model_cls(
            # We want this object to ONLY have the keys on it, not looking it up.
            hash_field=v.hash_value,
            range_field=v.range_value,
            do_not_compare=True
    ),
    lambda v: v.model_cls(hash_field=v.hash_value, range_field=v.range_value).id,
    lambda v: DynKey.via_obj(
        v.model_cls(hash_field=v.hash_value, range_field=v.range_value)
    ),
    lambda v: DynKey(
            api=v.model_cls.api,
            hash_key=v.hash_value,
            range_key=v.range_value
    )
])
def simple_obj_def(request, simple_obj_values) -> ObjTestDef:
    """ Takes base-case via `simple_obj_values` and adds a number of ways to lookup
        objects via id to it via a callable template parameterized value.
    """
    template: Callable[[ObjTestValues], Any] = request.param
    get_via_id = template(simple_obj_values)

    kwargs = {}
    for field in dataclasses.fields(ObjTestValues):
        kwargs[field.name] = getattr(simple_obj_values, field.name)

    return ObjTestDef(
        **kwargs,
        get_via_id=get_via_id,
    )


@pytest.fixture()
def simple_obj(simple_obj_def) -> 'ItemWithRangeKey':
    # Use `simple_obj_2` to two objects into dynamo, helps ensure tests in general
    # don't get this one and only get the simple_obj, where relevant
    obj = simple_obj_def.model_cls(
        hash_field=simple_obj_def.hash_value,
        range_field=simple_obj_def.range_value
    )
    obj.name = "start-name"
    obj.basic_bool = True
    obj.api.send()

    assert len(list(simple_obj_def.model_cls.api.get())) == 1, "Have incorrect count."
    return obj


@pytest.fixture()
def simple_obj_second(simple_obj_def) -> 'ItemWithRangeKey':
    hash_extra = simple_obj_def.generate_hash_value(2)
    range_extra = simple_obj_def.generate_range_value(3)

    obj2 = simple_obj_def.model_cls(
        hash_field=hash_extra,
        range_field=range_extra
    )
    obj2.name = range_extra
    obj2.basic_bool = True
    obj2.api.send()

    assert len(list(simple_obj_def.model_cls.api.get())) == 2, "Have incorrect count."
    return obj2


@pytest.fixture()
def simple_obj_get_via_id(simple_obj, simple_obj_def) -> Optional[ItemWithRangeKey]:
    """ List of various ways to lookup simple object via `ItemWithRangeKey.api.get_via_id`. """
    get_via_id = simple_obj_def.get_via_id
    if isinstance(get_via_id, DynModel):
        obj = get_via_id
    else:
        obj = simple_obj_def.model_cls.api.get_via_id(get_via_id)
    assert obj, f"We were unable to retrieve object via {get_via_id}"
    return obj


@pytest.fixture(autouse=True)
@pytest.mark.order(-10)
def mock_dynamo_db():
    with mock_dynamodb() as mock:
        yield mock


# --------------------------
# ***** My Unit Tests ******

def test_basic_dyn_class():
    class ItemOnlyHash(DynModel, dyn_name="testItemOnlyHash"):
        hash_field_id: str = HashField()
        basic_int: int

    fields = ItemOnlyHash.api.structure.fields
    print(fields)


def test_basic_json_with_blank_data():
    model = SubObj()
    model.sub_name = "398221"

    results = model.api.json()
    # `model.queue` should no be in JSON, since it should default to `None` since it was unset.
    assert results == {"sub_name": "398221"}, "Was `queue` value filtered out?"


def test_basic_delete(simple_obj, simple_obj_get_via_id, simple_obj_def):
    """Tests deleting data generated via simple_obj fixture via simple_obj_get_via_id fixture."""
    # Try to delete it and see if getting it again will return None now.
    simple_obj_get_via_id.api.delete()
    assert simple_obj_def.model_cls.api.get_via_id(simple_obj.id) is None


def test_deleting_via_delete_objs_via_dynkey(simple_obj, simple_obj_get_via_id, simple_obj_def):
    dyn_key = DynKey.via_obj(simple_obj)

    # Try to delete it using only dyn-key see if getting it again will return None now.
    simple_obj_def.model_cls.api.client.delete_objs([dyn_key])
    assert simple_obj_def.model_cls.api.get_via_id(simple_obj.id) is None


def test_deleting_via_delete_obj_via_dynkey(simple_obj, simple_obj_get_via_id, simple_obj_def):
    dyn_key = DynKey.via_obj(simple_obj)

    # Try to delete it using only dyn-key see if getting it again will return None now.
    simple_obj_def.model_cls.api.client.delete_obj(dyn_key)
    assert simple_obj_def.model_cls.api.get_via_id(simple_obj.id) is None


def test_getting_simple_obj(simple_obj, simple_obj_get_via_id):
    # See if all of the simple objects we get compare the same to original simple object.
    simple_obj.assert_with(simple_obj_get_via_id)


def test_getting_multiple_obj(simple_obj, simple_obj_second, simple_obj_def):
    model_cls = simple_obj_def.model_cls

    name1 = "my-name-1"
    name2 = "my-name-2"
    hash2 = simple_obj_def.generate_hash_value(10)
    range1 = simple_obj_def.generate_range_value(1)
    range2 = simple_obj_def.generate_range_value(2)
    some_other_range = simple_obj_def.generate_range_value(521)

    # Two more items, each with the same hash but different range-key.
    obj1 = model_cls()
    obj1.hash_field = hash2
    obj1.range_field = range1
    obj1.name = name1

    obj2 = model_cls()
    obj2.hash_field = hash2
    obj2.range_field = range2
    obj2.name = name2

    simple_obj_other = model_cls()
    simple_obj_other.hash_field = simple_obj.hash_field
    simple_obj_other.range_field = some_other_range
    simple_obj_other.name = "simple_obj_other"
    simple_obj_other.api.send()

    all_objs = [simple_obj, obj1, obj2, simple_obj_second, simple_obj_other]
    original_obj_map = {obj.id: obj for obj in all_objs}

    # Bulk-insert multiple objects at the same time....
    model_cls.api.client.send_objs([obj1, obj2])

    # Verify we can lookup all objects in table.
    objs = list(model_cls.api.get())
    assert len(objs) == len(all_objs)

    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # lookup objects only for a specific hash....
    objs = list(model_cls.api.get(query={"hash_field": hash2}))
    original_obj_map = {obj.id: obj for obj in [obj1, obj2]}
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # See if we can query via list (which should use `in` operator by default)....
    objs = list(model_cls.api.get(
        query={"hash_field": hash2, "name": [name2, name1]}
    ))
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # See if we can query via in operator explicitly
    objs = list(model_cls.api.get(
        query={"hash_field": hash2, "name__in": [name2, name1]}
    ))
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # name is not a list in any of the table's items, so when used `exact` with list nothing
    # should come back (since they are all strings).
    objs = list(model_cls.api.get(
        query={"hash_field": hash2, "name__exact": [name2, name1]}
    ))
    assert len(objs) == 0

    # See if we can query by non-keys + hash at same time correctly.
    objs = list(model_cls.api.get(
        query={"hash_field": hash2, "name": name2}
    ))
    original_obj_map = {obj.id: obj for obj in [obj2]}
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # Lookup multiple objects via different hashes:
    objs = list(model_cls.api.get(
        query={"hash_field": [simple_obj.hash_field, hash2]}
    ))
    original_obj_map = {obj.id: obj for obj in [simple_obj, simple_obj_other, obj1, obj2]}
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # Single hash multiple range, but should only match one result.
    objs = list(model_cls.api.get(
        query={
            "hash_field": simple_obj_other.hash_field,
            "range_field": [simple_obj_other.range_field, hash2]
        }
    ))
    original_obj_map = {obj.id: obj for obj in [simple_obj_other]}
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])

    # Single hash multiple range, but should only match one result.
    objs = list(model_cls.api.get(
        query={
            "hash_field": [simple_obj_other.hash_field, obj1.hash_field],
            "range_field": [simple_obj_other.range_field, obj1.range_field]
        }
    ))
    original_obj_map = {obj.id: obj for obj in [obj1, simple_obj_other]}
    assert len(objs) == len(original_obj_map.values())
    for obj in objs:
        obj.assert_with(original_obj_map[obj.id])


def test_send_obj_with_related_child_and_sub_obj(simple_obj_values):
    o_with_range = ItemWithRangeKeyForStr()

    o_with_range.hash_field = "my-hash3"
    o_with_range.range_field = "my-range3"
    o_with_range.name = "my-name3"
    o_with_range.hello = False

    o_with_range.api.send()

    o2 = ItemOnlyHash()
    o2.child_item = o_with_range

    o2.hash_field_id = "test-id3"

    # todo: Consider supporting setting a 'JSONDict' directly on sub-item field and having it
    #  automatically create the proper sub-item type [via `ModelAsSubJsonDict(json_dict)`]?
    o2.sub_item = SubObj(sub_name="my-sub-name", queue=True)
    # ----> FYI: could also do it by passing json-dict via first argument:
    # SubObj({'sub_name': "my-sub-name", 'queue': True})

    o2.api.send()
    assert o2.sub_item.api.json() == {"sub_name": "my-sub-name", "queue": True}

    o_gotten = ItemOnlyHash.api.get_via_id("test-id3")
    original_json = o_gotten.sub_item.api.json()
    got_json = o_gotten.sub_item.api.json()

    # Ensure we got something and they are the same
    assert original_json == got_json

    # Ensure child-object can be lazily looked up, and data in it is correct.
    o_gotten.child_item.assert_with(o_with_range)


def test_pagination(simple_obj_values):
    model_cls = simple_obj_values.model_cls

    # Concatenating this much makes str type keys get large;
    # Makes it paginate a number of times.
    # Dynamo pagination is based on data size in each page of results.
    # A small number of objects with lots of data is much faster then tens of thousands of objects.
    lots_of_data = simple_obj_values.generate_range_value(0)
    if model_cls.api.structure.dyn_hash_field.type_hint is str:
        for x in range(15):
            lots_of_data += simple_obj_values.generate_range_value(x)

    with DynBatch():
        obj = model_cls()
        for x in range(40):
            obj.hash_field = lots_of_data
            obj.range_field = simple_obj_values.generate_range_value(x)
            obj.api.send()

    result = list(model_cls.api.get())
    assert len(result) == 40


class MultipleHashes(DynModel, dyn_name="tableWithError1"):
    """ This class has an intentional error, it has two hash fields. """
    hash_field_1: str = HashField()
    hash_field_2: str = HashField()


class MultipleRanges(DynModel, dyn_name="tableWithError2"):
    """ This class has an intentional error, it has two range fields. """
    hash_field_1: int = HashField()
    range_field_1: int = RangeField()
    range_field_2: str = RangeField()


def test_multiple_keys_error():
    """ See if we detect multiple hash/range fields in structure correctly.
        System should lazily figure this out the first time we ask the the BaseModel's
        `DynModel.api`.
    """
    with pytest.raises(XRemoteError):
        MultipleHashes.api.get()
    with pytest.raises(XRemoteError):
        MultipleRanges.api.get()
