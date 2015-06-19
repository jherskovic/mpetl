__author__ = 'jrherskovic'

import unittest
import multiprocessing
from mpetl import _QTask

def null_task(parameter):
    return (parameter,)


class test_qtask(unittest.TestCase):
    def test_creation(self):
        null_qtask = _QTask(None, None, None, None, None)
        self.assertIsInstance(null_qtask, _QTask)

    def test_creation_with_callable(self, num=1, chunk_size=1):
        self.qtask = _QTask(null_task, num, chunk_size, None, None)
        self.assertIsInstance(self.qtask, _QTask)

    def create_and_instantiate(self, num=1, chunk_size=1):
        self.test_creation_with_callable(num, chunk_size)
        self.input_q = multiprocessing.Queue()
        self.output_q = multiprocessing.Queue()
        self.qtask.instantiate(self.input_q, self.output_q)

    def test_instantiation(self):
        self.create_and_instantiate()
        self.qtask.join()

    def test_multi_instantiate(self):
        self.create_and_instantiate(4)
        self.qtask.join()

    def test_huge_instantiate(self):
        self.create_and_instantiate(100)
        self.qtask.join()

    def test_process_one_thing(self, num=1, chunk_size=1):
        self.create_and_instantiate(num, chunk_size)
        self.input_q.put(("Hello",))
        self.qtask.join()
        # Remember that internally we pass lists
        self.assertEqual(self.output_q.get(), [("Hello",)])

    def test_process_one_thing_with_four_processes(self):
        self.test_process_one_thing(4)

    def test_process_one_thing_with_47_processes(self):
        self.test_process_one_thing(47)

    def test_process_one_thing_with_47_processes_and_large_chunks(self):
        self.test_process_one_thing(47, 10)

    def test_process_one_thousand_things(self, num=1, chunk_size=1):
        self.create_and_instantiate(num, chunk_size)
        [self.input_q.put((x,)) for x in range(1000)]
        self.qtask.join()

        # There's no guarantee that the results are in order, so we must test them as sets
        result = set(self.output_q.get()[0][0] for x in range(1000))
        self.assertEqual(result, set(x for x in range(1000)))

    def test_process_1000_things_63_processes(self):
        self.test_process_one_thousand_things(63)

    def test_process_1000_things_63_processes_prime_chunks(self):
        self.test_process_one_thousand_things(63, 7)


if __name__ == '__main__':
    unittest.main()
