import inspect
import multiprocessing
import weakref
from .util import SENTINEL

__author__ = 'Jorge R. Herskovic <jherskovic@mdanderson.org>'


class SequenceError(Exception):
    pass


class _QTask(object):
    """Describes one task in a _Pipeline."""
    def __init__(self, callable, num, chunk_size, setup, teardown, **kwargs):
        self._callable = callable
        self._num = multiprocessing.cpu_count() if num is None or num < 1 else num
        self._chunk_size = 1 if chunk_size is None or chunk_size < 1 else chunk_size
        self._setup = setup
        self._teardown = teardown
        self._kwargs = kwargs
        self._processes = []
        self._input = None
        self._output = None

    def _run_in_process(self):
        persistent = None
        if self._setup is not None:
            persistent = self._setup()

        is_generator = inspect.isgeneratorfunction(self._callable)
        outgoing_chunk = []

        while True:
            chunk = self._input.get()
            if chunk == SENTINEL:
                break

            for item in chunk:
                if persistent is None:
                    if isinstance(item, tuple):
                        result = self._callable(*item, **self._kwargs)
                    else:
                        result = self._callable(item, **self._kwargs)
                else:
                    if isinstance(item, tuple):
                        result = self._callable(*item, process_persistent=persistent, **self._kwargs)
                    else:
                        result = self._callable(item, process_persistent=persistent, **self._kwargs)

                if is_generator:
                    for each_result in result:
                        outgoing_chunk.append(each_result)
                        if len(outgoing_chunk) >= self._chunk_size:
                            self._output.put(outgoing_chunk)
                            outgoing_chunk = []
                else:
                    if result is None:
                        # Valueless function, or no result whatsoever.
                        pass
                    outgoing_chunk.append(result)

                if len(outgoing_chunk) >= self._chunk_size:
                    self._output.put(outgoing_chunk)
                    outgoing_chunk = []

        if len(outgoing_chunk) > 0:
            self._output.put(outgoing_chunk)

        if self._teardown is not None:
            self._teardown(persistent)

        return

    def instantiate(self, input, output):
        self._input = input
        self._output = output

        num_copies = multiprocessing.cpu_count() if self._num is None else self._num

        self._processes = [multiprocessing.Process(target=self._run_in_process) for x in range(num_copies)]
        [x.start() for x in self._processes]

    def join(self):
        if len(self._processes) == 0:
            return

        [self._input.put(SENTINEL) for x in self._processes]
        [x.join() for x in self._processes]
        return


class _Pipeline(object):
    """Manages a multi-stage Extract, Transform, Load process."""

    def __init__(self, max_size=-1):
        self._max_size = max_size
        self._tasks = []
        self._origins = []
        self._destinations = []
        self._queues = []
        self._actual_tasks = None
        self._finalize = weakref.finalize(self, _Pipeline._cleanup, self._queues)

    def _new_task(self, callable, num=None, chunk_size=1, setup=None, teardown=None, **kwargs):
        if self._actual_tasks is not None:
            raise SequenceError("You are trying to add a task to a pipeline that already started.")

        return _QTask(callable, num, chunk_size, setup, teardown, **kwargs)

    def add_task(self, callable, num=1, chunk_size=1, setup=None, teardown=None, **kwargs):
        new_task = self._new_task(callable, num=num, chunk_size=chunk_size, setup=setup, teardown=teardown, **kwargs)
        self._tasks.append(new_task)

    def add_origin(self, *args, **kwargs):
        new_task = self._new_task(*args, **kwargs)
        self._origins.append(new_task)

    def add_destination(self, *args, **kwargs):
        new_task = self._new_task(*args, **kwargs)
        self._destinations.append(new_task)

    def start(self):
        # Every task has an input and an output queue, of maximum max_size items
        # The first queue is fed by "feed", of course.
        if self._actual_tasks is not None:
            raise SequenceError("You are trying to start a pipeline that already started.")

        self._queues.append(multiprocessing.Queue(self._max_size))
        self._actual_tasks = self._origins + self._tasks + self._destinations
        for t in self._actual_tasks:
            self._queues.append(multiprocessing.Queue(self._max_size))
            t.instantiate(self._queues[-2], self._queues[-1])

        return

    def feed_chunk(self, chunk):
        """Takes a chunk of items (i.e. a list of items) and feeds them to the pipeline."""
        if self._actual_tasks is None:
            raise SequenceError("You are feeding a pipeline that hasn't started.")

        self._queues[0].put(chunk)

    def feed(self, item):
        """Feeds a single item to the pipeline."""
        self.feed_chunk([item])

    @property
    def results_queue(self):
        return self._queues[-1]

    def join(self):
        """Signals the end of processing, then waits for the associated tasks to end. Once the tasks end,
        puts an end-of processing Sentinel marker in the outgoing queue."""
        if self._actual_tasks is None:
            raise SequenceError("You are joining a pipeline that hasn't started.")

        [x.join() for x in self._actual_tasks]
        self.results_queue.put(SENTINEL)

    def as_completed(self):
        while True:
            result_chunk = self.results_queue.get()
            if result_chunk == SENTINEL:
                break

            for result in result_chunk:
                yield result

    @staticmethod
    def _cleanup(queues):
        # Clean up the remaining queues.
        for q in queues:
            if q is not None:
                q.close()