"""
Used to keep track of a shared connection and Dynamo table resources.

If you want to easily send/get information from Dynamo via orm.

Important Classes

- `DynamoDB`: A resource (see `xyn_resource` for more details) that keeps
    track of a shared boto3 dynamo session/connection. It also caches the fact if a table
    is created (ie: no need to check every time, just the first time we use a table).

- `DynamoTableCreator`: Interface used by `DynamoDB` to have an easy way to initiate the creation
    of a Dynamo table if needed (if it does not exist).

"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError
from xinject import Dependency

from xboto.resource import dynamodb
from .errors import XModelDynamoError

log = logging.getLogger(__name__)

__all__ = ["DynamoTableCreator", "DynamoDB"]

_auto_create_table_only_in_environments = {'unittest', 'local'}


class DynamoTableCreator:
    """A callable that can, when called, create the needed Dynamo table, and then return the newly
    created table as a boto3 table resource [via: return dynamo.db.create_table(...)].
    """

    def __call__(self, dynamo: DynamoDB) -> Any:
        """Something like this::

        return dynamo.db.create_table(TableName="SomeName", KeySchema=[...], ...)
        """
        raise NotImplementedError("Must Implement To Create Dynamo Table.")


class DynamoDB(Dependency, attributes_to_skip_while_copying=["_table", "_verified", "_db"]):
    """
    Resource that represents a DynamoDB connection.  This allows us to pool the dynamo
    connection among everything that uses Dynamo, so that we can reuse existing connections.
    This speeds up Dynamo by a fair amount.  Every time you open a connection with Dynamo,
    it has to figure out how to get the encryption key, but subsequent requests using the same
    connection don't have to.
    """
    _tables: Dict[str, Any]
    _verified: Dict[str, bool]

    def __init__(self):
        self._tables = {}
        self._verified = {}

    @property
    def db(self):
        return dynamodb

    def table(self, name: str, table_creator: Optional[DynamoTableCreator] = None):
        """Returns existing table or creates + returns one if we don't have the resource
        currently. When we create a new one, we will remember it in a weak-fashion in
        case we get asked again. Once all other references to the table resource are
        gone the python garbage-collector will cleanup our reference automatically.
        If that happens and we get asked for that table_resource, we will create and
        return a new one [and remember if for future use].

        The reason to provide a table_creator is so you can easily Mock DynamoDB, as it
        will be created in the 'Mock' framework automatically.

        Also when devs run the code locally without deploying it, it will create the table
        for them.

        When you deploy code via serverless, it should be the thing the creates the table
        for the deployed environment [ie: testing/prod/etc].  The table should already exist
        that way when the app runs, and so it does **NOT** need table-creating AWS permissions.

        Args:
            name: Name of the table to get a resource for.
            table_creator (Union[DynamoTableCreator, None]): ...
                If None: Won't verify table is created or ready.

                If DynamoTableCreator:
                    Verifies the table exists and is ready. If it does not exist we create
                    the table by 'calling' whats passed in here and wait for it to be ready.
                    If the table is in a status that indicates it can't be used at the moment
                    [example: If table is 'DELETING'], we raise an XynLibError.
        """
        verified = self._verified.get(name, False)

        try:
            from xcon import xcon_settings
            # We only verify/create-table-if-needed in specific environments.
            if xcon_settings.environment not in _auto_create_table_only_in_environments:
                verified = True
        except ImportError:
            # If `xcon` unavailable, just assume we don't want to auto-create tables
            # todo: Put in a configurable setting that allows one to turn on/off
            #       auto-table-creation.
            #       (and some way to communicate billing mode???).
            #
            # todo: Log about why not creating tables, but log it only once.
            verified = True

        table = self._tables.get(name)
        if table is not None and verified:
            return table

        if table is None:
            table = dynamodb.Table(name)

        if verified:
            self._tables[name] = table
            return table

        if not table_creator:
            # Don't verify table if we don't have a table creator, just return it.
            self._tables[name] = table
            self._verified[name] = False
            return table

        try:
            log.info(f"Getting Table Status for ({name}).")
            status = table.table_status
            # It turns out, a table is still usable while "UPDATING", so only worry about
            # DELETING and CREATING.
            if status == "CREATING":
                log.warning(
                    f"Dynamo status for table ({name}) is CREATING; based on past experience "
                    f"we can still use the table [to at least read values] if the table is "
                    f"being restored from backup while it's in the CREATING status. So I am not "
                    f"going to wait for table to become ACTIVE, I'll try to use the table "
                    f"immediately."
                )
                # This is how we could wait for the table, disabling for now, see log.warning ^^^
                # table.wait_until_exists()
            elif status == "DELETING":
                raise XModelDynamoError(f"Dynamo Table ({name}) status is 'DELETING'???")
        except ClientError:
            # This means the table has not been created yet, we create and wait for it to exist.
            log.warning(f"Dynamo table ({name}) does not exist, creating...")
            table = table_creator(dynamo=self)
            table.wait_until_exists()

        self._tables[name] = table
        self._verified[name] = True
        return table
