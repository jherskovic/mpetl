#!/usr/bin/env python

__author__ = 'Jorge R. Herskovic <jherskovic@gmail.com>'

import unittest
from mpetl.pipeline import _Pipeline
from mpetl.messaging import *

def generator(up_to):
    for i in range(up_to):
        yield i
    return

def conditional_routing(number, pipeline_msg):
    """Routes even numbers to a pipeline called "even" and odd numbers to a pipeline called "odd"
    """
    #print("Got", number)
    if number % 2 == 0:
        pipeline_msg.send_message("even", [number])
    else:
        pipeline_msg.send_message("odd", [number])


def gathering_function(number):
    #print("Gathering",number)
    return number


class test_messaging(unittest.TestCase):
    def setUp(self):
        self.messaging = MessagingCenter()
        self.messaging.start()

    def test_result(self):
        self.evens = _Pipeline()
        self.evens.add_destination(gathering_function)
        self.evens.start()

        self.odds = _Pipeline()
        self.odds.add_destination(gathering_function)
        self.odds.start()

        self.original = _Pipeline()
        self.original.add_origin(generator)
        self.original.add_destination(conditional_routing, pipeline_msg = self.messaging)
        self.original.start()

        self.messaging.register_pipeline_queue("even", self.evens.input_queue)
        self.messaging.register_pipeline("odd", self.odds)

        self.original.feed(100)

        self.original.join()
        self.messaging.flush()

        self.odds.join()
        self.evens.join()
        evens = [x for x in self.evens.as_completed()]
        odds = [x for x in self.odds.as_completed()]

        self.messaging.forget_pipeline("odd")
        self.messaging.forget_pipeline("even")

if __name__ == '__main__':
    unittest.main()
