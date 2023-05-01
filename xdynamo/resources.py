from typing import Dict, Optional, Any, Set, TYPE_CHECKING
from boto3.dynamodb.table import BatchWriter

from xmodel.remote import XRemoteError
from xinject import DependencyPerThread

if TYPE_CHECKING:
    from xdynamo.api import DynApi


class _DynBatcher(object):
    """
    Used by ``Table`` as the context manager for batch writes.

    You likely don't want to try to use this object directly.
    """
    _table_to_boto_writer: Dict[Any, BatchWriter]
    _enter_count = 0
    _dyn_batch_resources_added_to: Set['_DynBatchResource']

    def __init__(self):
        self._dyn_batch_resources_added_to = set()
        self._table_to_boto_writer = dict()

    def batch_writer(self, api: 'DynApi') -> BatchWriter:
        if self._enter_count <= 0:
            raise XRemoteError("Must use DynBatch via `with` statement as a context manager.")

        table = api.table

        writer_map = self._table_to_boto_writer
        table_id = id(table)
        batch_writer = writer_map.get(table_id)
        if batch_writer:
            return batch_writer

        structure = api.structure

        pkeys = [structure.dyn_hash_key_name]
        range_key = structure.dyn_range_key_name
        if range_key:
            pkeys.append(range_key)

        batch_writer = table.batch_writer(overwrite_by_pkeys=pkeys)
        writer_map[table_id] = batch_writer
        # Activate writer, we are
        batch_writer.__enter__()
        return batch_writer

    def __enter__(self):
        enter_count = self._enter_count
        if enter_count <= 0 and self._table_to_boto_writer:
            # If we have writers at this point, there is a serious problem.
            # We may have left over writers
            raise XRemoteError(
                "We got 'entered' as a context manager, but we have writers. Having writers at "
                "this point is a serious problem. We may have left-over writers from a previous "
                "use as a context manager [via `with`]?"
            )

        if enter_count < 0:
            raise XRemoteError(
                "We got 'entered' as a context manager, but we have a negative enter count of "
                f"({enter_count})! This indicates a problem in DynBatch."
            )

        try:
            if enter_count > 0:
                return

            resource = _DynBatchResource.grab()
            resource.add_writer(self)
            self._dyn_batch_resources_added_to.add(resource)
        finally:
            self._enter_count += 1

    def __exit__(self, type, value, traceback):
        enter_count = self._enter_count
        enter_count -= 1
        if enter_count < 0:
            raise XRemoteError("DynBatch enter/exit count is below zero, we got unbalanced.")
        self._enter_count = enter_count
        if enter_count == 0:
            # Exit all batch writers, clear all writers.
            # It's assumed that a writer we have already had '__enter__' called on it.
            for writer in self._table_to_boto_writer.values():
                writer.__exit__(type, value, traceback)

            for resource in self._dyn_batch_resources_added_to:
                resource.remove_writer(self)

            # Remove all internal references, we have cleaned up and closed them all up.
            self._table_to_boto_writer.clear()
            self._dyn_batch_resources_added_to.clear()


class _DynBatchResource(DependencyPerThread):
    # Instead of inheriting from `ThreadUnsafeResource`, we set flag directly ourselves.
    # This allows us to be compatible with both v2 and v3 of xinject.
    #
    # It only makes sense to have _DynBatchResource on a single-thread.
    writers: Dict[id, 'DynBatch']

    def __init__(self):
        self.writers = {}

    def add_writer(self, writer: 'DynBatch'):
        writer_id = id(writer)
        writers = self.writers
        if writer_id in writers:
            raise XRemoteError(
                f"Adding writer {writer} to DynBatchResource, but that writer is "
                "already there!  The writer should not do this, it's state is bad."
            )

        writers[writer_id] = writer

    def remove_writer(self, writer: 'DynBatch'):
        self.writers.pop(id(writer))

    def have_writer(self) -> bool:
        return bool(self.writers)

    def current_writer(self, create_if_none: bool = False) -> Optional['DynBatch']:
        writers = self.writers
        if not writers:
            return DynBatch() if create_if_none else None

        return list(writers.values())[-1]


class DynBatch(_DynBatcher):
    """
    Allows one to batch bulk updates/deletes (via dynamo put/delete-item) with a context manager.
    You can bulk-delete/update currently via:

    - `DynClient.delete_objs`
    - `DynClient.update_objs`

    But if you want to combine a number of separate update/delete object calls
    (including with other calls to `DynClient.delete_objs` / `DynClient.update_objs`)
    into the same request(s), this class allows you to do that.

    For example code, see [Batch Updating Deleting](#batch-updating-deleting).
    """
    pass


class DynTransaction(_DynBatcher):
    """
    .. todo:: In the future, there will be a class called `DynTransaction` that you can use to
        batch transactions together. It would also allow bulk-partial updating/patching objects
        instead of using 'puts' to replace entire object. And to tie a set of objects together
        that must all be written or rolled back in one go.
    """
    pass
