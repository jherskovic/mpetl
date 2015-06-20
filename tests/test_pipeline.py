__author__ = 'jrherskovic'

import unittest
from mpetl import Pipeline, SequenceError

# The following functions use different operations so a change in order will ruin them

def first_stage(parameter):
    return (parameter + 1,)

def second_stage(parameter):
    return (parameter - 3,)

def third_stage(parameter):
    return (parameter * 5,)

def iterator_origin(up_to):
    for i in range(up_to):
        yield (i,)

    return

class TestPipeline(unittest.TestCase):
    def test_basic_pipeline(self):
        self.pipe = Pipeline()
        self.pipe.add_task(first_stage)
        self.pipe.add_task(second_stage)
        self.pipe.add_task(third_stage)
        self.pipe.start()
        self.pipe.feed((0,))
        self.pipe.join()
        result = [x for x in self.pipe.as_completed()]
        self.assertEqual(result, [(-10,)])

    def test_correct_ordering(self):
        self.pipe = Pipeline()
        self.pipe.add_destination(third_stage)
        self.pipe.add_task(second_stage)
        self.pipe.add_origin(first_stage)
        self.pipe.start()
        self.pipe.feed((0,))
        self.pipe.join()
        result = [x for x in self.pipe.as_completed()]
        self.assertEqual(result, [(-10,)])

    def test_more_pipeline(self):
        # In the following pipeline, we'll use ONE origin to send the numbers one through 100 through the pipeline
        self.pipe = Pipeline()
        self.pipe.add_origin(iterator_origin, num=1)
        self.pipe.add_task(first_stage)
        self.pipe.add_task(second_stage)
        self.pipe.add_task(third_stage)
        self.pipe.start()
        self.pipe.feed((100,))
        self.pipe.join()
        result = set(x[0] for x in self.pipe.as_completed())
        expected = set((x + 1 - 3) * 5 for x in range(100))
        self.assertEqual(result, expected)

    def test_bad_sequence(self):
        # You can't feed a pipe before starting it
        self.pipe = Pipeline()
        self.assertRaises(SequenceError, self.pipe.feed, (1,))

    def test_bad_join(self):
        # You can't join an unstarted pipeline
        self.pipe = Pipeline()
        self.assertRaises(SequenceError, self.pipe.join)

    def test_chunked_pipeline(self):
        # In the following pipeline, we'll use ONE origin to send the numbers one through 100 through the pipeline
        self.pipe = Pipeline()
        self.pipe.add_origin(iterator_origin, num=1, chunk_size=11)
        self.pipe.add_task(first_stage, num=1, chunk_size=17)
        self.pipe.add_task(second_stage, num=1, chunk_size=3)
        self.pipe.add_task(third_stage, num=1, chunk_size=9)
        self.pipe.start()
        self.pipe.feed((100,))
        self.pipe.join()
        result = set(x[0] for x in self.pipe.as_completed())
        expected = set((x + 1 - 3) * 5 for x in range(100))
        self.assertEqual(result, expected)

    def test_very_parallel_pipeline(self):
        # In the following pipeline, we'll use ONE origin to send the numbers one through 100 through the pipeline
        self.pipe = Pipeline()
        self.pipe.add_origin(iterator_origin, num=1, chunk_size=11)
        self.pipe.add_task(first_stage, num=20, chunk_size=17)
        self.pipe.add_task(second_stage, num=17)
        self.pipe.add_task(third_stage, num=7, chunk_size=9)
        self.pipe.start()
        self.pipe.feed((100,))
        self.pipe.join()
        result = set(x[0] for x in self.pipe.as_completed())
        expected = set((x + 1 - 3) * 5 for x in range(100))
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
