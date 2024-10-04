from typing import Optional, Set, TypeVar, TYPE_CHECKING

from xcon import xcon_settings
from xmodel.remote import RemoteStructure
from xmodel.remote import XRemoteError
from xdynamo.fields import DynField
from xdynamo.common_types import DynKeyType
from xsentinels.default import Default

F = TypeVar('F', bound=DynField)


class DynStructure(RemoteStructure[F]):
    """
    Putting it here right now, it uses most of the super-class stuff, with a few extra fields.
    See `xmodel.base.structure.BaseStructure` for more details;

    Use these names on the DynModel's as class-arguments to configure these. Those values
    will eventually be put into this structure for use by the DynClient/DynApi classes.
    """

    # Todo: Refer to parent docs, but put more info about how virtual-id's are used for our
    #  dynamo tables.
    virtual_id: bool = True

    dyn_name: str = Default
    """ Table name, minus service/environment name.
        We generally want to use camelCase for this and no dashes/underscores.

        If left as `Default`, will use a "camelCase" version of the model class name.
    """

    dyn_service: str = Default
    """ Service to use, by `Default` we use current `xcon.conf.XconSettings.service`.
        If Blank/None we won't include it in the name.
    """

    dyn_environment: str = Default
    """ Environment to use, by `Default` we use current `xcon.conf.XconSettings.environment`.
        If Blank/None we won't include it in the name.
    """

    dyn_hash_key_name: str = None
    """ Name of hash key [for now both model and table attribute must be same name].
        If left as None, an error will be raised when we try connect to table.

        You can more easily indicate this via `HashField`, ie:

        >>> class MyModel(DynModel):
        ...     my_hash: str = HashField()

        When you do this, we will fill in `dyn_hash_key_name` for you.
    """

    dyn_range_key_name: str = None
    """ Name of range key [for now both model and table attribute must be same name];
        If left as None, we don't have one on the table.

        You can more easily indicate this via `RangeField`, ie:

        >>> class MyModel(DynModel):
        ...     my_hash: str = RangeField()

        When you do this, we will fill in `dyn_range_key_name` for you.
    """

    dyn_consistent_read: bool = Default
    """ If True will default reads for the associated model to be consistent,
        Otherwise it won't use consistent reads by default.

        Individual gets/queries can override this default via function param.
    """

    dyn_id_delimiter = "|"
    """
    Default way to delimit the hash/range keys when used in external systems and with code under
    the `xmodel` library. In general, it's assumed we only have one key that can uniquely
    identify model objects/items in general. For DynamoDB, if it has a range key we can merge
    both the hash-key and range-key together with this character. By default it's a pipe `|`.
    But you can set it to something else here if needed.

    This makes it so other systems can store just one value to refer to an item in a dynamo
    table, ie: "some-hash-value|some-range-value".  When other BaseModel's from other systems
    need to get a Dynamo model, they can look it up via this single 'id' value.
    """

    @property
    def dyn_hash_field(self) -> DynField:
        return self.get_field(self.dyn_hash_key_name)

    @property
    def dyn_range_field(self) -> Optional[DynField]:
        return self.get_field(self.dyn_range_key_name)

    def has_id_field(self):
        id_field = self.get_field('id')
        return (id_field.dyn_key is DynKeyType.hash) if id_field else False

    def configure_for_model_type(
            self,
            *,
            # These fields are used to name the table
            # format: `{dyn_service}-{dyn_environment}-{dyn_name}`
            dyn_name: str = Default,
            dyn_service: str = Default,
            dyn_environment: str = Default,
            dyn_id_delimiter: str = Default,
            dyn_consistent_read: bool = Default,

            **kwargs
    ):
        """
        See superclass method `xmodel.base.structure.BaseStructure.configure_for_model_type`
        first. It has more information about what calls this method and how to
        customize value via class arguments
        (see `xmodel.base.model.BaseModel.__init_subclass__` for more about the class argument
        angle).

        This method accepts every argument in the super-class version, with the addition
        of ones more specific to Dynamo. So se

        This method accepts named-class-parameters for `xdynamo.model.DynModel`
        subclasses.

        See super-class method `xmodel.base.structure.BaseStructure.configure_for_model_type`
        doc's for additional arguments and details.

        Args:
            dyn_name: Name of the table.  We will add dyn_service and current environment to the
                final name.  See `DynStructure.fully_qualified_table_name` for more details.

                This can also be `None` to indicate that it has no direct-table
                (ie: this is an abstract base class, with commonly shared fields, etc).

            dyn_service: Name of the service to use for that portion of the table name.
                If not provided, and `xcon` is available, will use `con.conf.XconSettings.service`.
                Right now `xcon` is a required dependency, but will make it optional in the future.

                In that future version, leaving this as `Default` would make it not format
                the service name into the table name.

            dyn_environment: Name of the environment to use for that portion of the table name.
                Right now `xcon` is a required dependency, but will make it optional in the future.

                In that future version, leaving this as `Default` would make it not format
                the environment name into the table name.

            dyn_id_delimiter: Delimiter to use for the `id` value when table has both a
                hash and range key (to delimit the values of the two).

                By default, a pipe char `|` is used.

            dyn_consistent_read: Lets model default to consistent reads by default.
                If unspecified, consistent reads are not used by default.
                You can override this on a per-get/query/scan basis via function param.

            **kwargs: These all come from class-arguments given to the
                `xdynamo.model.DynModel` at class-definition time that need to be sent to
                my super-class via
                `xmodel.base.structure.BaseStructure.configure_for_model_type`. See that
                for more on what other arguments are supported.
        """
        super().configure_for_model_type(**kwargs)

        # Resolve default `dyn_name` if needed
        if dyn_name is Default:
            # Attempt to use model class name (with first char lower-cased) if we have one.
            model_name = self.model_cls.__name__
            dyn_name = f'{model_name[:1].lower()}{model_name[1:]}' if model_name else ''

        if dyn_name == '':
            raise XRemoteError(
                f"The `dyn_name` of dynamo model ({self.model_cls}) was blank/None; "
                f"model must be `None` or have a non-blank `dyn_name`."
            )

        self.dyn_name = dyn_name
        self.dyn_service = dyn_service
        self.dyn_environment = dyn_environment
        self.dyn_consistent_read = dyn_consistent_read

        if dyn_id_delimiter:
            self.dyn_id_delimiter = dyn_id_delimiter

        type_to_attr_map = {
            DynKeyType.hash: 'dyn_hash_key_name',
            DynKeyType.range: 'dyn_range_key_name',
        }

        encountered_types: Set[DynKeyType] = set()
        for field in self.fields:
            # We can assume all fields are DynField subclasses, since it's the field_type
            key_type = field.dyn_key
            if not key_type:
                continue

            attr_name = type_to_attr_map[key_type]
            if key_type in encountered_types:
                last_encountered_name = getattr(self, attr_name, None)
                raise XRemoteError(
                    f"We have multiple ({DynKeyType.hash}) key-type fields: "
                    f"({last_encountered_name}) and ({field.name}) for model ({self.model_cls}). "
                    f"There can only be one ({DynKeyType.hash}) field key-type on a dynamo table."
                )

            encountered_types.add(key_type)
            setattr(self, attr_name, field.name)

        # If user manually set this to 'True', then keep it; otherwise determine it based
        # on if there was a hash-key on 'id'.
        # If the `id` field is not the hash-key, then we don't consider us having a
        # `id` field. Instead, we have a virtual-id field that is a combination of both
        # the hash-key field value with the range-key value (if we have a range-key).
        # The purpose of that is to indicate that the `id` field is not a normal field
        # on this class, but a virtual property instead (see `DynModel.id`).

        from xdynamo.model import DynModel
        if self.model_cls is DynModel:
            # Restore `DynModel.id` normal non-field property for DynModel.
            # We want it to be a virtual-id.
            # (BaseModel by default will capture field properties into `fget` and `fset`
            #  in the Field object, and then execute properties when needed after doing it's
            #  normal thing.... we want to replace that functionality with our own property,
            #  and not have it go through the normal things... but still treat 'id' like a
            #  normal property otherwise).
            id_field = self.get_field('id')
            self.model_cls.id = property(fget=id_field.fget, fset=id_field.fset)

    def fully_qualified_table_name(self) -> str:
        """
        Fully qualified name of the table in Dynamo as a str.
        Format is: '{dyn_service}-{dyn_environment}-{dyn_name}'
        """
        components = []
        service = self.dyn_service
        if service is Default:
            service = xcon_settings.service

        env = self.dyn_environment
        if env is Default:
            env = xcon_settings.environment

        if service:
            components.append(service)

        if env:
            components.append(env)

        name = self.dyn_name
        if not name:
            raise XRemoteError(
                f"Tried to get `fully_qualified_table_name` but have no table name for {self}"
            )
        components.append(name)
        return "-".join(components)

    @property
    def endpoint_description(self):
        return self.fully_qualified_table_name()
