from os import getpid
from uuid import uuid4

from ._memmapping_reducer import _get_temp_dir
from .memory import StoreBase

_shelf = None


class ShelfFuture(object):
    def __init__(self, store_backend, id):
        self.store_backend = store_backend
        self.id = id

    def result(self):
        return self.store_backend.load_item((self.id,))


class Shelf(StoreBase):
    _folder_name = f"joblib_shelf_{getpid()}"

    def shelve(self, data):
        id = uuid4().hex
        self.store_backend.dump_item((id,), data)
        return ShelfFuture(self.store_backend, id)


def shelve(data):
    global _shelf
    if _shelf is None:
        _shelf = Shelf(_get_temp_dir("")[0])
    return _shelf.shelve(data)
