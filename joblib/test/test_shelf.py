from random import random

from joblib import Parallel, delayed, shelve
from joblib.shelf import ShelfFuture, _futures
from joblib.testing import parametrize, raises


@parametrize("data", [42, "text", ["pi", 3.14, None], {"a": 0, 1: "b"}])
def test_shelve(data):
    future = shelve(data)
    assert future.result() == data
    id = (future.location, future.id)
    assert id in _futures
    assert _futures[id] == data


def test_bad_shelf_access():
    x, y = 42, 69
    sx, sy = map(shelve, (x, y))
    assert sx.location == sy.location
    assert sx.id != sy.id
    for id in "abc":
        if id != sx.id and id != sy.id:
            break
    for loc in "ab":
        if loc != sx.location:
            break
    with raises(KeyError, match="Non-existing item"):
        ShelfFuture(sx.location, id).result()
    with raises(KeyError, match="Non-existing item"):
        ShelfFuture(loc, sx.id).result()


def test_shelve_parallel():
    N, R = 100, 40
    S = N - R + 1
    data = [random() for _ in range(N)]
    shelved_data = shelve(data)

    def f(data, i):
        return sum(data.result()[i : i + R])

    expected = [sum(data[i : i + R]) for i in range(S)]
    out = Parallel(n_jobs=4)(delayed(f)(shelved_data, i) for i in range(S))
    assert out == expected
