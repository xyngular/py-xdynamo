"""
## ORM Dynamo Overview
[orm-dynamo-overview]: #orm-library-overview

Intended use of this library is for an quick way to get/retrieve objects from Dynamo tables.

Things that it can help with:

- If table does not exist, it will create it automatically.
    - Helps when unit-testing with `@moto.mock_dynamodb` decorator, since mock expects you to
        create the table before using it. This means tables are automatically/lazily created as
        needed (with no special effort on the part of the unit test).
        .. tip:: To use
    - When running any code locally, it can automatically create the table for you without
        any special effort.
    - Normally, lambdas are not given permission to create table as we want
        cloud-formation/serverless to manage the table.
        During deployment into aws serverless/cloud-formation should be the one creating table
        if needed
- Works like with the other standard `xmodel.remote.model.RemoteModel`'s:
    - `DynModel` and other related classed work very similar.
        Supports the same basic/common methods.
    - Easy to use single 'string' to identify any dynamo object via `DynModel.id`
        - will synthesize a string representing the full primary key, for use
            with other BaseModel's when using child objects (ie: they know how to look each other up).
    - Easy/Standard way to Paginate
        - When you use the standard `DynApi.get` method, it will return a generator giving you
            one object at a time while bulk-getting up to 100 per-request to dynamo, depending on
            how it had to query the data for you
            (the goal is for it to figure out the most efficient way to query automatically).
        - You don't have to worry about what page of results you are on or how it works,
            you just loop/run the generator and it will eventually give you back all of
            the objects. [Auto Prefetch Children](./#auto-prefetch-children)
        - **TODO** (rest-client `xmodel_rest.RestClient` has this, need to do it still for
            `DynClient`).  You can still use methods in `xmodel.children`
            to bulk-grab them.
            - If your curious, see [Auto Prefetch Children](./#auto-prefetch-children)
                for more info about how to use the auto-pre-fetch feature with the standard
                `xmodel.base.model.BaseModel`.
    - Simple way to insert/update/delete objects individually or in bulk.
    - Full power of the JSON to/from BaseModel's infrastructure.
        - Including automated conversion of types to/from, such as dates.
        - Default values
        - Read only fields, etc.
    - Central spot to put future high-level dynamo code to share among our projects.

### Quick Start

If you don't know much about the ORM, have a look at
[ORM Library Overview](./#orm-library-overview) first.  It's an overview of the basic concepts.

Index/Summary of the main classes you'll be interacting with:

- `DynModel`: Represents an object in a table.
- `DynApi`: Basically represents the table, it's the central 'hub' class that lets you get to the
    - `DynStructure`: List of fields and other class-level info about the `DynModel`.
    - `DynClient`: Wraps boto, figures out the request to use with boto and executes it.
- `DynKey`: Contains a hash + range keys, along with ways to put them together into an
    `DynModel.id` string and splitting them apart again.
- `DynField`: Represents a field on a `DynModel`.  Automatically created in `DynStructure` if
    it's not user allocated on a attribute/field on `DynModel`.
    - `HashField`: Special `DynField` object, indicated field is the Hash of the table.
    - `RangeField`: Indicates the Range field of the table (if there is one).
- `DynBatch`: Context manager (ie: `with` object).
    Allows you to batch non-transaction put's (so system will just use strait put') and deletes.

### Example Data Models

Examples are probably the best way to get a 'quick start', here are some below.

First, I'll get a few BaseModel's defined. After I'll show examples of using them.

This first one show's a table with a Hash + Range key, along with a list of dicts along
with some basic data fields (str/bool).

.. note:: You can see models vary similar to these in action in sine unit-tests
    Look at
    [tests/test_dynamo.py](https://github.com/xyngular/py-xyn-model-dynamo/blob/master/tests/test_dynamo.py)
    in xdynamo source if your interested.

>>> class ModelWithRangeKey(
...     DynModel,
...     # ---> used for end of table name:
...     dyn_name="modelWithRangeKey"
... ):
...     my_hash: str = HashField()
...     my_range: str = RangeField()
...     name: str
...     a_number: int
...     hello: bool
...     items: List[Dict[str, str]]

Here is a normal non-dynamo model. We will be using this as a way to parse a sub-dict
automatically into a regular model object.
We enabled `Field.include_in_repr` in the below example, it will make `sub_name` print out
in string when object is converted to a string (such as when logging object out).

>>> from xmodel.fields import Field
>>> class ModelAsSubJsonDict(BaseModel, has_id_field=False):
...
...     # It puts 'sub-name' into the object description when converting object
...     # to a string
...     # (ie: such as when you log out a object of type 'ModelAsSubJsonDict')
...     sub_name: str = Field(include_in_repr=True)
...     queue: bool

Here is a second `DynModel` for a separate table.  It has a relationship to a
`ModelWithRangeKey`.

>>> class ModelOnlyHash(
...     DynModel,
...     dyn_name="visibleShipConfirm",
...     dyn_service="experimental"
... ):
...     hash_only: str = HashField()
...     name: str
...     items: List[Dict[str, str]]
...     a_number: int
...     test_item_id: str
...     test_item: ModelWithRangeKey
...     sub_item: ModelAsSubJsonDict

In the real dynamo table, it would store `VisiblePackage.sub_item` as a
`test_item_id` attribute by grabbing the `DynModel.id` from the `ModelWithRangeKey` object.

It would lazily lookup object if you try to access `VisiblePackage.sub_item`, just like you
would expect.

.. todo:: rest-client `xmodel_rest.RestClient` has an ability to auto pre-fetch children.
    Still need to do it for `DynClient`. You can still use methods in `xmodel.children`
    to bulk-grab them.

    If your curious, see [Auto Prefetch Children](./#auto-prefetch-children)
    for more info about how to use the auto-pre-fetch feature with the standard
    `xmodel.base.model.BaseModel`.

Create a few items in a table (for illustrative purposes, for the following examples):

>>> ModelWithRangeKey(my_hash="my-h1", my_range="my-r1", name="A").api.send()
>>> ModelWithRangeKey(my_hash="my-h2", my_range="my-r2", name="B").api.send()

### Basics of Getting Items

Quick Example of getting an item, this gets the item by a `"my-h1"` hash-key and
`"my-r1"` range-key:

>>> ModelWithRangeKey.api.get_via_id({'my_hash': "my-h1", 'my_range': "my-r1"})
ModelWithRangeKey(my_hash: "my-h1", my_range: "my-r1")

Various ways Get Item are below, in general you need a hash-key to do a dynamo-query and to
generally get items. If you don't have a hash-key value, then you must scan the table.

Right now, you can grab all items in a table via `DynApi.get`.
This will pass a blank query to `DynClient.get`, which will make it do a
full-table scan and return all items.

Right now, `DynClient` won't scan and will instead raise an
`xmodel.remote.errors.XRemoteError`
if you pass in a non-blank query without including a hash-key value.

.. todo:: Support scanning entire table with a non-blank query to filter it with.
    This will be supported in the future, right now it's unimplemented.
    Most of the time, you'll really, really want to query the table in any case.
    Querying the table (with a hash-key) is **MUCH** faster then scanning it.

I have various examples below, but in general we support querying in these ways,
you can use `DynApi.get` or `DynClient.get`.
These methods will figure out the best way to execute get request/query, generally detailed
below:

- If provided query is blank, scans and returns all items/objects in table.
- Generally if you have both (and only) hash and range keys, it will do a batch-get automatically
    which lets us query for 100 objects at a time. Otherwise it will fall-back to a query.
    When doing a query, dynamo only supports one query-request per-hash.
    - If the table structure has only a hash-key (and no range key),
        then it's a list of only hashes.
- If you only have a:
    - hash-key:  Will return all objects with that hash, could be multiple objects if the table
        structure has a range-key.
        - If table only supports hash-key, it will be either a single object or empty list.
    - single hash + range keys: Return's a single object if it exists, or empty-list.
        - Consider using `XynApi.get_via_id`, it won't return a `list` if you provide
            the `id` value as a single string/dict as first argument.
    - multiple Hash + range keys: Will query every combination of hash + range keys automatically.
        It can use a batch-get for this and attempt to lookup up to 100 objects per-request.
        If you have more than 100, we will split up the requests for you automatically
        (you'll just see a single stream of objects come back).
    - `id`: If you have the `DynModel.id` for an object, it contains one or both keys and can
        be used to query object via `DynApi.get_via_id`.
    - `DynKey`: You can use this via `DynApi.get_via_id` to query for the object.
        - `DynKey`'s represet one or both components of a DynamoDB primary key.
- If you have other attributes besides just the range/hash key:
    - We need to fallback to a query in this case, to support filtering by non-key attributes.
    - One query per-hash/range key in the query.
        - If the range-key is using `between` operator, we still only do one query since dynamo supports
            this operator in a query.
            - We'll talk more about operators later in the [Advanced Queries](#advanced-queries)
                section further on.
        - If you use a list of values with range-key, it will have to use
            multiple queries, one per-hash/range key combination(s) provided.


First there is `DynApi.get_via_id` which can take a list of id strings,
or a list of dicts with a hash-key and (optionally) a range-key:

>>> ModelWithRangeKey.api.get_via_id({'my_hash': "my-h1", 'my_range': "my-r1"})
ModelWithRangeKey(my_hash: "my-h1", my_range: "my-r1")

This will produce an error, you must provide all parts of the key to use `get_via_id`,
so it needs the range-key part:

>>> ModelWithRangeKey.api.get_via_id({'my_hash': "my-h1"})
Raises XRemoteError

If you want all the objects for a particular hash regadless of the range-key, you can
uss the `DynApi.get` method instead. We wrap it in a list, because a generator is
normally returned (to showcase all output). You can put the returned generator in
a `for` loop instead if you want. The generator will correctly paginate all results
for you automatically. Here is the example:

>>> list(ModelWithRangeKey.api.get({'my_hash': "my-h1"}))
[ModelWithRangeKey(..., my_range="my-r1"), ModelWithRangeKey(..., my_range="my-r2")]

You can also query on other non-key attributes,

>>> list(ModelWithRangeKey.api.get({'my_hash': "my-h1"}))

You can also use a generic object that represents a dynamo key with `DynKey`.
You can pass `DynKey` objects directly into `DynApi.get_via_id`.
Other methods let you also directly pass `DynKey`'s, such as `DynClient.delete_objs`.

>>> key = DynKey(hash_key='my-h1', range_key='my-r1')
>>> ModelWithRangeKey.api.get_via_id(key)

To you can grab a list of them, a list of dicts or a list of keys,
when you provide a list you get a generator back that will paginate though
the results correctly.

>>> list(ModelWithRangeKey.api.get_via_id([key]))
[ModelWithRangeKey(....)]

Also, if your table only has a hash-key, you can just directly provide it's string/int value;
or a list of them:

>>> ModelOnlyHash.api.get_via_id('a-hash-key')

This is the same value as you get from the model's `DynModel.id` attribute.

>>> obj = ModelOnlyHash(hash_only = 'a-hash-key')
>>> assert obj.id == 'a-hash-key'

Objects with a range-key by default have them joined together with hash-key via a pipe `|`
delimiter, like so:

>>> obj = ModelWithRangeKey(my_hash="my-h1", my_range="my-r1")
>>> assert obj.id == 'my-h1|my-r1'

You can change the joining/delimiter string via `DynStructure.dyn_id_delimiter`.
Just like with the normal `xmodel.base.model.BaseModel`, you can set these attributes via the
class arguments on `DynModel` subclasses.
(for more details on class arguments, see `xmodel.base.model.BaseModel.__init_subclass__`
and `DynStructure.configure_for_model_type` for the DynModel specific ones available).

Every `DynModel` has this 'virtual' `DynModel.id` value that is the primary key of
the object. THis is the hash-key, plus range-key value (if object has one) of an object.
This single string uniquely identified the object.

The rest of the `xmodel.remote` can use this just like any `id` from
other `xmodel.base.model.BaseModel` objects. Meaning, this virtual `id` value can be
stored in other places to form relationships (as you can see with the above
example on `ModelOnlyHash.sub_item`.



### Updating Items

Just like other `xmodel.base.model.BaseModel` objects, `DynModel` can be changes and then
the changes sent to Dynamo like so:

>>> obj = ModelOnlyHash(hash_only="a-hash-key")
>>> obj.carrier = "new-carrier"
>>> obj.api.send()

You can also mass-update/create objects via `DynClient.send_objs`:

>>> list_of_objs: List[ModelOnlyHash]
>>> ModelOnlyHash.api.client.send_objs(list_of_objs)

Right now we only support "Putting" objects into dynamo (ie: not patching them).
It will replace the entire item in dynamo, no mater what you change or update on the object.

.. todo:: In the future, there will be an option to patch object(s) via `DynTransaction`.
    The feature has not been finished yet.
    It will use a Dynamo Transaction to do it.
    See [Todo/Future: Batch via Transaction](#todofuture-batch-via-transaction)

### Deleting Items

>>> obj.api.delete()

You can mass delete via:

>>> key = DynKey(hash_key='my-h1', range_key='my-r1')
>>> objs = list(ModelWithRangeKey.api.get_via_id(key))
>>> ModelWithRangeKey.api.client.delete_objs(objs)

You can also give the `DynClient.delete_objs` a list of `DynKey`'s.

>>> ModelWithRangeKey.api.client.delete_objs([key])

This allows you to delete objects without having to create full-models and looking to see
which field is the hash/range key.  The `DynKey` always accepts the values the same way.


### Batch Updating / Deleting

You can send a one-off list of multiple objects to update/delete all at once (see examples above).

If you want a section of code that gets executed to batch-delete/update you can directly use
the `DynBatch` class. It's a context-manager and can apply batch Put's and Delete's via a
`with` statment that will continue to apply it no mater how deep the call stack is inside
the `with` statement. See `DynBatch` for more details

Quick Example:

>>> # DynModel objects of some sort....
>>> obj1: ModelOnlyHash
>>> obj2: ModelOnlyHash
>>> obj3: ModelOnlyHash
>>> with DynBatch():
...     obj1.carrier = "changed"
...     obj1.api.send()
...     obj2.carrier = "changed"
...     obj2.api.send()
...     obj3.api.delete()

This would end up sending both updates and the delete in the same request.
It works by 'batching' a number of objects at a time and sending them.
If there are still objects to send by the time the `with` statement exits,
the renaming unsent objects are sent to dynamo.

### Todo/Future: Batch via Transaction

.. todo:: In the future, there will be a class called `DynTransaction` that you can use to
    batch transactions together. It would also allow bulk-partial updating/patching objects
    instead of using 'puts' to replace entire object. And to tie a set of objects together
    that must all be written or rolled back in one go.

## Mocking Dynamo with moto

You can use the `moto` 3rd part dependency, and their `@moto.mock_dynamodb` method decorator
to 'mock' dynamo. There is something to be aware though, but they are easy to get correct:

- The mocking library expects you to first your dynamo tables via boto3 calls
    before they are used.  `xdynamo.DynClient` will check and ensure the tables
    are created automatically in a lazy fashion
    (ie: first time an attempt to send/get a `DynMode` from Dynamo).
- In an effort to reuse already opened connections to dynamo, we use a shared resource
    `xdynamo.dyn_connections.DynamoDB`.  It's important that the connection is created
    while `moto` is active for `moto` to mock the connection/session.

    .. note:: `xinject.fixtures.context` fixture is automatically used when `xinject` is
        installed as a dependency.
        This ensures when a `xdynamo.dyn_connections.DynamoDB` is asked for, a brand new
        one will be created for the unit test and therefore create a new connection/session.

    - It's relatively expensive to open a new connection vs using an already existing
        connection to Dynamo since Dynamo has to setup a encryption key context in it's self.

## Advanced Queries
[advanced-queries]: #advanced-queries

Ensure you've read the earilier segment about [Basics of Getting Items](#basics-of-getting-items)
first before reading this more advanced section.

`DynApi.get` and `DynClient.get`, for the `query` param can accept more then
just the hash-key and range-key; other attributes are allowed to filter the results even more!

Dynamo has a concept of a 'query' and a 'scan' to get data. A query needs to at least each
by the hash-key (range-key is optional).  Scan will let you search with any attribute but has
to look at all records, and so is slow.

If you at least provide the hash-key, you can filter by other attributes and still get
decent performance (as it only has to scan records with that hash).  If you also provide the
range-key, that's even better as that will narrow it down to one-record, and is fast since
Dynamo can use an index.

You can also provide a list of hash-keys, with other attributes to fitler by.
`DynClient` will automatically break it up into multiple queries, one per-hash key
automatically and paginate all the results together for you.

### Operators

It also supports operators, like `some_field__gte: 3` would look for things that are
greater-than-or-equal to `3`. A hash-key must be exact, or a list (ie: `__in`).

When wanting to query, the hash-field has to be an exact-equals value (no other special operators)
or a `list` value and/or `__in` operator.  When it's a list (which maps it's self automatically
to the `__in` operator by default) we simulate it by doing multiple queries, one per-item
in the list for the hash and then concatenating the results together via the generator
that gets returned (ie: lazily execute each query as the generator runs).

To get full list of operators, see operator method names on `bot3.dynamodb.conditions.Attr`
(and it's AttributeBase superclass). Also have a mapping at `operator_alias_map` that maps
some of our standard xyngular-api operators to how it's named in Dynamo.

I also have a list right below:

non-hash query operators you can use:

#### Operator List

- eq
- lt
- lte
- gt
- gte
- begins_with
- between
- is_int
_ exists
_ not_exists
- contains
- size
- attribute_type

See examples below to see how operators can be used.

For more info, take a look at some of the following examples and also look at
`DynClient.get`.

### Examples
[examples_1]: #examples_1

```python
ModelOnlyHash.api.get(
    query={
        'hash_only':['vis-track-id-2', 'vis-track-id-1'],
        'carrier': 'c2'
    }
)
```

This will do two queries in Dynamo, one for each:

1. 'hash_only: vis-track-id-2' + 'carrier: c2'
2. 'hash_only: vis-track-id-1' + 'carrier: c2'

It will combine the results of both together into a single generator and return the generator.
You can iterate on the generator to get all the results, all paginated for you.

You can use operators after the keyword, just like you can do for the Xyngular APIs:

```python
ModelOnlyHash.api.get(
    query={
        'hash_only':['vis-track-id-2', 'vis-track-id-1'],
        'a_number__gte': 2
    }
)
```

Looks for the two different `hash_only`'s that have a `a_number` attribute that
is greater then or equal to 2.


>>> my_objs = ModelWithRangeKey.api.get(
...     query={
...         'my_hash':['hash-1', 'hash-2'],
...         'my_range':['range-1', 'range-2'],
...         'a_number__gte': 2
...     }
... )


It looks for all of the following combinations of range/hash with four seperate queries
(internally):

- hash-1, range-1
- hash-1, range-2
- hash-2, range-1
- hash-2, range-2

Each of these queries will also have a filter where `a_number` attribute will need
to be greater then or equal to 2.

`DynApi.get` will return a generator, that will return all objects returns from
these four internal queries it will do for you.

All you have to do is run the generator to see all the objects, like so:

>>> for o in my_objs:
...     print(o.a_number)
--- outputs one number per-object in table that match query ---

If the above example query only had `my_hash` and `my_range` and **NOT** `a_number__gte`,
it would have executed a very-fast batch-get on dynamo with the hash/range key combinations.

Doing four query/requests are still pretty fast,
but doing a batch-get can be executed in a single request (100 key combinations per-request)!

Like I said earlier, the nice thing about `DynApi.get` / `DynClient.get`
is that it can figure out the best way to query the objects for you based on what you give
so you don't have figure it out your self.

"""  # noqa -- Some lines can't be cut shorter (Url's).

from xdynamo.model import DynModel
from xdynamo.fields import DynField, HashField, RangeField
from xdynamo.common_types import DynKey
from xdynamo.resources import DynBatch

__all__ = [
    "DynKey", "DynModel", "HashField", "RangeField", "DynBatch", "DynField"
]
