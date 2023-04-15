from typing import Optional, Type, Dict
from xmodel import Field, Converter
from xmodel_dynamo.common_types import DynKeyType
from xsentinels.default import Default


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
