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
    if counter[0] % 9 == 0:
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
        q.close()

    def build_divisible_pipeline(self):
        self.divisible = Pipeline("divisible")
        self.divisible.add_origin(divisible_pipeline_start)
        self.divisible.add_destination(divisible_pipeline_dest)

    def test_divisible_pipeline(self):
        q = multiprocessing.Queue()
        self.build_divisible_pipeline()
        Pipeline._messaging.register_pipeline_queue("final", q)
        self.divisible.start()
        self.divisible.feed(6)
        self.divisible.join()
        # We should get two numbers, 0 * 7 and 1 * 7
        self.assertEqual(set(q.get()[0] for x in range(2)), set([0, 7]))
        q.close()

    def build_nondivisible_pipeline(self):
        self.nondivisible = Pipeline("nondivisible")
        self.nondivisible.add_origin(nondivisible_pipeline_start, num=1)
        self.nondivisible.add_task(nondivisible_pipeline_task, num=2)
        self.nondivisible.add_destination(nondivisible_pipeline_destination, num=2)

    def test_nondivisible_pipeline(self):
        q = multiprocessing.Queue()
        self.build_nondivisible_pipeline()
        Pipeline._messaging.register_pipeline_queue("final", q)
        self.nondivisible.start()
        self.nondivisible.feed(6)
        self.nondivisible.join()
        # We should get six numbers, 0, 1, 2, 3, 4, 5, each / 2 and * 3
        self.assertEqual(set(q.get()[0] for x in range(6)), set(x / 2 * 3 for x in range(6)))
        q.close()

    def build_final_pipeline(self):
        self.final = Pipeline("final")
        self.final.add_origin(final_pipeline_start)
        self.final.add_destination(final_pipeline_reduce, num=1)

    def test_final_pipeline(self):
        self.build_final_pipeline()
        self.final.start()
        for i in range(17):
            for j in range(20):
                self.final.feed(j)

        # We will get a single number; as to its value, it will depend on the exact way the calls get distributed, so
        # it's impossible to know. It'll be greater than 17*10, at least.
        self.final.join()
        value = [x for x in self.final.as_completed()]
        self.assertGreaterEqual(value[0], 170)

    def test_assemble_contraption(self):
        self.build_first_pipeline()
        self.build_divisible_pipeline()
        self.build_nondivisible_pipeline()
        self.build_final_pipeline()

        self.first.start()
        self.divisible.start()
        self.nondivisible.start()
        self.final.start()

        self.first.feed(100)
        self.first.join()
        self.divisible.join()
        self.nondivisible.join()
        self.final.join()

        result = [x for x in self.final.as_completed()]
        self.assertGreater(len(result), 0)

if __name__ == '__main__':
    unittest.main()
