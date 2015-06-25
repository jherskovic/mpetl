__author__ = 'jrherskovic'

import unittest
import multiprocessing
from mpetl import Pipeline

# Convoluted test case: We create four Pipelines. The first one feeds two, conditionally, which then feed back into
# one. The first pipeline will generate numbers up to a certain one. The second pipeline will receive all numbers
# divisible by three and divide them by three; they should therefore all be integers. The third pipeline will receive
# numbers NOT divisible by three.


def first_pipeline_origin(number):
    for i in range(number):
        yield i

def first_pipeline_destination(number):
    if number % 3 == 0:
        Pipeline.send("divisible", number)
    else:
        Pipeline.send("nondivisible", number)


def divisible_pipeline_start(number):
    for i in range(number // 3):
        yield i

def divisible_pipeline_dest(number):
    Pipeline.send("final", number * 7)


def nondivisible_pipeline_start(number):
    for i in range(number):
        yield i

def nondivisible_pipeline_task(number):
    return number / 2

def nondivisible_pipeline_destination(number):
    Pipeline.send("final", number * 3)


def final_pipeline_start(number, accumulator=[0], counter=[0]):
    accumulator[0] += number
    counter[0] += 1
    if counter[0] % 10 == 0:
        yield accumulator[0]
        accumulator[0] = 0

    return

def final_pipeline_reduce(number, accumulator=[0], counter=[0]):
    accumulator[0] += number
    counter[0] += 1
    if counter[0] % 17 == 0:
        yield accumulator[0]
        accumulator[0] = 0

    return

class TestPipeline(unittest.TestCase):
    def build_first_pipeline(self):
        self.first = Pipeline("first one")
        self.first.add_origin(first_pipeline_origin)
        self.first.add_destination(first_pipeline_destination)

    def test_first_pipeline(self):
        self.build_first_pipeline()
        # ensure the pipeline is started.
        # trick the pipeline into sending all results to a single queue by registering it manually
        q = multiprocessing.Queue()
        Pipeline._messaging.register_pipeline_queue("divisible", q)
        Pipeline._messaging.register_pipeline_queue("nondivisible", q)
        self.first.start()
        self.first.feed(6)
        # We should get back the set (0, 1, 2, 3, 4, 5)
        self.first.join()
        self.assertEqual(set(q.get()[0] for x in range(6)), set(x for x in range(6)))


if __name__ == '__main__':
    unittest.main()
