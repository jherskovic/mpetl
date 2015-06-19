# MPETL

An opinionated framework for big MultiProcessing Extract, Transform, Load jobs.

©2015 Jorge Herskovic <first initial + last name @ gee mail dot com>

## Introduction

We write lots of large ETL jobs. Most of them start with slicing input into little pieces, then processing each of those
pieces, then writing the results somewhere else. We tend to use Python's multiprocessing module for this, and many of
our problems are embarrassingly parallel. But that use of multiprocessing leads to a lot of boilerplate code.
Boilerplate that interfupts the flow of our programs and makes them look ugly and less readable.

Hence, MPETL. To only write this once and let the actual processing code take center stage.

## Usage

MPETL works on pipelines and is meant for embarrassingly parallel ETL jobs. A pipeline starts at an *origin* and ends in
*destinations*. It has *tasks* that get applied to data along the way. It can consist of any number of intermediate
tasks. Origins are responsible for yielding chunks of data that can be ingested by tasks, which are responsible for
producing chunks of data that can be ingested by destinations.

The only thing MPETL guarantees here is that the data will make it all the way through, and that the pipeline will be
parallel.

Parameters MUST be picklable, since they're going into Queues under the covers.

The following is an example of a single-origin, single-destination, single-task pipeline that capitalizes a nursery
rhyme in an unnecessarily convoluted way.

### Basic example
```python
import mpetl

EXAMPLE_TEXT = """Mary had a little lamb,
His fleece was white as snow,
And everywhere that Mary went,
The lamb was sure to go.
"""

def origin_function(text):
    for line in text:
        yield (line,)

    return

def process_line(one_line):
    return one_line.upper()

def destination_function(one_line):
    print(one_line)


if __name__ == "__main__":
   pipeline = mpetl.Pipeline()
   pipeline.add_origin(origin_function)
   pipeline.add_task(process_line)
   pipeline.add_destination(destination_function)

   pipeline.start()
   pipeline.feed(EXAMPLE_TEXT)
   pipeline.join()
```

### More advanced example

Let's say we want to uppercase all of the text in all of the files in a pathspec, in parallel. It will then write them
to destination files in a new directory. It's clearly not productive to feed() them to the pipeline one by one,
so we'll use multiple origin functions, and keep the lines "organized" by decorating them with their filename.

All mpetl functions must return a single value. You can cheat by packing as many as you want into a dictionary or tuple.
If your return value is iterable, it WILL be iterated over. You can avoid this by stashing your return value in a tuple.
Tuples will be conveniently unpacked for the next call.

Also, this example illustrates a common pattern - a Lockable. Since we can't write to the same file twice
simultaneously, we use a Lockable to create a multiprocessing lock around a resource (in this case, the destination
file). Lockables consume memory until their respective origin expires and its processing is finished. Lockables go hand
in hand with the decorate-process-undecorate pattern, and as such mpetl needs to know it will be used. You can do this
by creating a DecoratedPipeline instead of a base Pipeline. For the same reason, you may ONLY create a Lockable around
the Decorator. Any calls to Lockable where the decorator hasn't been seen before as the output of an origin function
will fail. DecoratedPipelines will auto-decorate the output of a function for you, but you're responsible for 
receiving it and doing something meaningful with it.

```python
import mpetl
import glob
import os

def read_file(filename):
    with open(filename) as f:
        for line in f:
            yield (line,)

def process_line(filename, line):
    return (line.upper(),)

def write_output(filename, line, destination_path):
    with mpetl.Lockable(filename):
        with open(os.path.join(destination_path, filename), 'a') as f:
            f.write(line)

if __name__ == "__main__":
    # The size of the Queues can be specified to ensure that there aren't too many items in flight at the same time.
    # This is useful if elements are big, or consume a lot of memory.
    pipeline = mpetl.DecoratedPipeline(max_size=1000)

    # By default, the pipeline functions create multiprocessing.cpu_count() copies of each one. If this is not
    # what you want, use the num parameter to specify how many to create.
    pipeline.add_origin(read_file)
    pipeline.add_task(process_line, num=100)

    # Note that unknown keyword arguments get passed as-is to pipeline components as well
    pipeline.add_destination(write_output, destination_path="/tmp")

    pipeline.start()
    for filename in glob.glob("/usr/share/doc/xterm/*.html"):
        pipeline.feed(filename)
    pipeline.join()
```

### Chunk size
IPC overhead can dominate a pipeline if the processing itself is relatively cheap. You can therefore specify a 
*chunk_size* when calling add_task or its siblings. The results will be gathered in *chunk_size* blocks to be passed 
around. Note that this will interfere with multiprocessing granularity, i.e., the ability to distribute work well, so
it should be used only for very large numbers of fast tasks. 

### Database access
Database access is expensive and creating connections over and over can chew a lot of overhead. mpetl will let you
specify a callable that will be called one per process, and that can return a value that will be then passed to your
task function as the *process_persistent* parameter. Give the add_task/origin/destination declaration a *setup*
parameter that is a callable which returns a single value. For obvious reasons, this callable should be self-contained.
You can also specify a *teardown* which will receive the same object and can... well... tear it down.

    pipeline.add_task(save_stuff_to_db, setup=create_connection)

If you do this, `save_stuff_to_db` needs to take a `process_persistent` parameter:

    def save_stuff_to_db(stuff_to_be_saved, process_persistent):

### add_origin and add_destination
`add_origin` and `add_destination` behave like `add_task`. All origins will be executed in the chronological order 
they are added. All destinations will be executed in the chronological order in which they are added. Origins always 
execute before tasks, which always execute before destinations.

In other words, the following order of calls:
```python

pipeline.add_destination(function_1)
pipeline.add_origin(function_2)
pipeline.add_task(function_3)
pipeline.add_task(function_4)
pipeline.add_destination(function_5)
pipeline.add_task(function_6)
pipeline.add_origin(function_7)
pipeline.add_task(function_8)
```

Will result in the following actual pipeline:
```function_2 -> function_7 -> function_3 -> function_4 -> function_6 -> function_8 -> function_1 -> function_5```

Don't say I didn't warn you. The idea is that you should write your pipelines in the order in which they execute.

### Conditional routing, branching pipelines, etc.
TBD
