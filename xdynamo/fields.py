import dataclasses
from typing import Optional, Type, Dict, TypeVar, Generic, Union
from xmodel import Field, Converter
from xdynamo.common_types import DynKeyType
from xsentinels.default import Default
import typing_inspect
from typing import Hashable
from xsentinels.sentinel import Sentinel

T = TypeVar('T')


class _HashSentinel(Sentinel):
    pass


class _RangeSentinel(Sentinel):
    pass


Hash = Union[T, None]
""" Used in type-hints to wrap other type-hints to declare that it could be that
    type or `Null` value. Indicates that a value of Null could be assigned.

    You can use it like so:

    >>> from null_type import Nullable, Null
    >>>
    >>> nullable_string: Nullable[str]
    >>>
    >>> # You can assign a `str` or a `Null` and it will be type-correct:
    >>> nullable_string = "hello!"
    >>> nullable_string = Null
"""

HashStr = Hash[str]


@dataclasses.dataclass
class _UnwrapResults:
    unwrapped_type: Type
    """ The type(s) with Hash/Range generic types filtered out.
    """
    is_hash: bool = False
    is_range: bool = False


def _unwrap_generic_hash_or_range_types(type_to_unwrap: Type, /) -> _UnwrapResults:
    """
    Returns the first non-Null or non-None type inside the optional/Union type as
    the `unwrapped_type` result.

    If the type passed in is not an optional/nullable/union type, then set returned
    `unwrapped_type` to the type unaltered.

    Args:
        type_to_unwrap: Type to inspect and unwrap the optionality/nullability/union from.
    Returns:
        UnwrapResults: With the unwrapped_type, and if type is Nullable and/or Optional.
            If the Union has more than one none-Null/None type in it, then we will return
            a Union with the None and Null types filtered out.
    """
    if not typing_inspect.is_generic_type(type_to_unwrap):
        return _UnwrapResults(type_to_unwrap)

    NoneType = type(None)
    saw_null = False
    saw_none = False
    types = []

    hint_union_sub_types = typing_inspect.get_args(type_to_unwrap)
    for sub_type in hint_union_sub_types:
        # if sub_type is NullType:
        #     saw_null = True
        #     continue

        if sub_type is NoneType:
            saw_none = True
            continue

        types.append(sub_type)

    if len(types) == 1:
        # No other non-Null/None types, use the type directly.
        unwrapped_type = types[0]
    else:
        # Construct final Union type with the None/Null filtered out.
        unwrapped_type = Union[tuple(types)]

    return UnwrapResults(unwrapped_type, is_nullable=saw_null, is_optional=saw_none)


class DynField(Field):
    dyn_key: Optional[DynKeyType] = Default

    def resolve_defaults(
            self,
            name,
            type_hint: Type,
            default_converter_map: Optional[Dict[Type, Converter]] = None,
            parent_field: "DynField" = None
    ):
        # pydoc3 will copy the parent-class doc-comment if left empty here;
        # that's exactly what I want so leaving doc-comment blank.
        super().resolve_defaults(
            name=name,
            type_hint=type_hint,
            default_converter_map=default_converter_map,
            parent_field=parent_field
        )

        if self.dyn_key:
            if not self.was_option_explicitly_set_by_user('include_in_repr'):
                self.include_in_repr = True


class HashField(DynField):
    dyn_key = DynKeyType.hash


class RangeField(DynField):
    dyn_key = DynKeyType.range
