"""
=================================================
Using tqdm to track progress with joblib.Parallel
=================================================

This example illustrates how to use a ``tqdm`` progress bar together with
:class:`joblib.Parallel`.

We present two main approaches.

The first approach is simple to implement but may have drawbacks,
depending on what you want to display.

The second approach works reliably but requires defining a subclass of
:class:`~joblib.Parallel`.
"""

##############################################################################
# Using a generator
##############################################################################

##############################################################################
# We first define a task that sleeps for a given amount of time and returns
# that value. We also define a list of sleep times that will be used.

import time

times = [7, 2, 3, 5, 6, 4, 1]


def task(t):
    time.sleep(0.3 * t)
    return t


##############################################################################
# .. warning::
#
#     The following solution reports the number of dispatched tasks, and not
#     the number of completed tasks.
#
# A ``tqdm`` progress bar takes an iterable as input and returns an iterable.
# A :class:`~joblib.Parallel` call also takes an iterable as input.
#
# A first solution is therefore to apply ``tqdm`` to the tasks passed to
# :class:`~joblib.Parallel`. The progress bar will then show the number of
# tasks dispatched to the workers.

from tqdm import tqdm

from joblib import Parallel, delayed

p = Parallel(n_jobs=2, pre_dispatch=4, batch_size=1)
out = p(delayed(task)(t) for t in tqdm(times))
print(*out)

##############################################################################
# As can be observed, the progress bar goes from 0 tasks to 4 dispatched tasks.
# This is due to the ``pre_dispatch=4`` argument, which instructs
# :class:`~joblib.Parallel` to dispatch 4 tasks initially.
# Afterwards, it iterates over slices of ``n_jobs*batch_size`` tasks. In this
# example, the progress bar advances in steps of 2 because ``n_jobs=2`` and
# ``batch_size=1``.
#
# For large ``batch_size`` values, this progress bar is not very precise
# regarding the number of tasks being started. Moreover, this solution reports
# the number of dispatched tasks, whereas one might be interested in the number
# of completed tasks. The following examples show how to track completed tasks.
# instead.
#
# .. warning::
#
#     The following solution does not work as intended and is shown here
#     purely for educational purposes.
#
# Since a :class:`~joblib.Parallel` call returns an iterable over the outputs
# of each task (by default, a list), one might try applying ``tqdm`` to the
# returned value.
#
# As shown below, this is not convenient, because we must wait for all tasks
# to complete before the output ``list`` becomes available.

p = Parallel(n_jobs=2)
out = p(delayed(task)(t) for t in times)
print(*tqdm(out))

##############################################################################
# As a result, the progress bar only appears after all tasks have completed,
# and it immediately reaches 100%.
#
# .. warning::
#
#     The following solution provides limited progress reporting.
#
# A possible workaround is to use ``return_as='generator'`` in order to
# obtain a generator that yields the outputs of each task as they become
# available.

p = Parallel(n_jobs=2, return_as="generator")
out = p(delayed(task)(t) for t in times)
print(*tqdm(out, total=len(times)))

##############################################################################
# The progress bar now appears to work. However, we can still observe that
# it jumps from 1/7 directly to 4/7.
#
# This happens because when using ``return_as='generator'``, outputs are
# yielded in the same order as the input tasks. The first yielded value
# therefore corresponds to the task that takes 0.7 seconds to complete
# (recall ``times = [7, 2, 3, 5, 6, 4, 1]``). When this first task finishes,
# the second and third tasks have already completed as well, since
# 2 + 3 < 7. This explains the jump from 1/7 to 4/7.
#
# Note also that we must provide total=len(times) to tqdm, as the generator
# returned by :class:~joblib.Parallel does not define a length, unlike a list.
#
# .. warning::
#
#     The following solution produces outputs in a different order than the
#     input tasks.
#
# If you do not care about the order of the outputs and want more informative
# progress reporting, you can use ``return_as='generator_unordered'``.
#
# In this case, results are yielded as soon as tasks complete, allowing the
# progress bar to advance smoothly without waiting for slower tasks.

p = Parallel(n_jobs=2, return_as="generator_unordered")
out = p(delayed(task)(t) for t in times)
print(*tqdm(out, total=len(times)))

##############################################################################
# The progress bar now advances one step at a time. Note also how the order
# of the outputs differs from the original input order.
# If you need to preserve the original ordering, you could modify the task
# function to also return the index of the task.


##############################################################################
# Using a subclass of :class:`joblib.Parallel`
##############################################################################

##############################################################################
# The :class:`~joblib.Parallel` class provides a method
# :meth:`~joblib.Parallel.print_progress`, which is called each time a task
# completes. By default, this method prints varying amounts of information
# depending on the value of the ``verbose`` parameter.
#
# Here, we override this method in a custom subclass in order to update a
# ``tqdm`` progress bar instead.


class ParallelTqdm(Parallel):
    def __call__(self, iterable, n_tasks):
        self.tqdm = tqdm(total=n_tasks)
        return super().__call__(iterable)

    def print_progress(self):
        self.tqdm.update()
        if self.n_completed_tasks == self.tqdm.total:
            self.tqdm.close()


p = ParallelTqdm(n_jobs=2)
out = p((delayed(task)(t) for t in times), len(times))
print(*out)

##############################################################################
# This approach provides accurate progress reporting. However, some users may
# prefer one of the previous solutions if they are looking for a simpler
# implementation.
