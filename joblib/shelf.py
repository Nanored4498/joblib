import atexit
import os
import shutil
from uuid import uuid4

from ._memmapping_reducer import _get_temp_dir
from .memory import _store_backend_factory

_shelf = None
_futures = dict()


class ShelfFuture(object):
    def __init__(self, location, id):
        self.location = location
        self.id = id

    def result(self):
        global _futures
        id = (self.location, self.id)
        value = _futures.get(id, None)
        if value is None:
            value = _store_backend_factory("local", self.location).load_item((self.id,))
            _futures[id] = value
        return value

    def clear(self):
        _store_backend_factory("local", self.location).clear_item((self.id,))


class Shelf(object):
    """An object for storing values to be used later"""

    def __init__(self, location, /, backend_options=None):
        if backend_options is None:
            backend_options = {}
        self.store_backend = _store_backend_factory(
            "local",
            location,
            verbose=1,
            backend_options=dict(compress=False, mmap_mode=None, **backend_options),
        )
        atexit.register(self.close)

    def shelve(self, data):
        if self.store_backend is None:
            raise RuntimeError(
                "You may be trying to shelve using an already closed shelf."
            )
        id = uuid4().hex
        self.store_backend.dump_item((id,), data)
        return ShelfFuture(self.store_backend.location, id)

    def clear(self):
        """Erase the complete storage directory."""
        if self.store_backend is not None:
            self.store_backend.clear()

    def close(self):
        if self.store_backend is not None:
            shutil.rmtree(self.store_backend.location)
            self.store_backend = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __repr__(self):
        return "{class_name}(location={location})".format(
            class_name=self.__class__.__name__,
            location=(
                None if self.store_backend is None else self.store_backend.location
            ),
        )


def shelve(data):
    global _shelf
    if _shelf is None:
        location = _get_temp_dir("")[0]
        location = os.path.join(location, f"joblib_shelf_{os.getpid()}")
        _shelf = Shelf(location)
    return _shelf.shelve(data)


def clear_shelf():
    if _shelf is not None:
        _shelf.clear()
