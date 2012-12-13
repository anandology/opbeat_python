# -*- coding: utf-8 -*-

# from mock import Mock
from unittest2 import TestCase
from opbeat_python.metrics.aggregator import StatSet, Aggregator


class StatSetTest(TestCase):

    def test_init(self):
        statset = StatSet()

    def test_update_empty(self):
        statset = StatSet()

        statset.update(9.1)

        self.assertEqual(statset.avg_value, 9.1)
        self.assertEqual(statset.min_value, 9.1)
        self.assertEqual(statset.max_value, 9.1)
        self.assertEqual(statset.sample_count, 1)

    def test_update_notempty(self):
        statset = StatSet()

        statset.update(9.1)
        statset.update(91.11)
        self.assertEqual(statset.avg_value, (9.1 + 91.11) / 2)
        self.assertEqual(statset.min_value, 9.1)
        self.assertEqual(statset.max_value, 91.11)
        self.assertEqual(statset.sample_count, 2)

    def test_update_notempty_multiple(self):
        statset = StatSet()

        statset.update(9.1)
        statset.update(91.11)
        statset.update(12.11)
        self.assertEqual(statset.avg_value, (9.1 + 91.11 + 12.11) / 3)
        self.assertEqual(statset.min_value, 9.1)
        self.assertEqual(statset.max_value, 91.11)
        self.assertEqual(statset.sample_count, 3)

    def test_update_negative(self):
        statset = StatSet()

        statset.update(9.1)
        statset.update(-23.1)
        self.assertEqual(statset.avg_value, (9.1 - 23.1) / 2)
        self.assertEqual(statset.min_value, -23.1)
        self.assertEqual(statset.max_value, 9.1)
        self.assertEqual(statset.sample_count, 2)

class AggregatorTest(TestCase):

    def test_init(self):
        aggregator = Aggregator()

    def test_record_simple_point(self):
        aggregator = Aggregator()
        aggregator.record_point('metric1', 12.3)
        aggregator.record_point('metric1', 0.3)

        points = aggregator.get_values()

        self.assertEqual(len(points), 1)
        point = points[0]

        self.assertEqual(point['avg_value'], (12.3 + 0.3) / 2)
        self.assertEqual(point['min_value'], 0.3)
        self.assertEqual(point['max_value'], 12.3)
        self.assertEqual(point['sample_count'], 2)

    # def test_record_two_metrics(self):
    #     aggregator = Aggregator()
    #     aggregator.record_point('metric1', 12.3)
    #     aggregator.record_point('metric1', 0.2)
    #     aggregator.record_point('metric2', 0.3)
    #     aggregator.record_point('metric2', 11.1)

    #     points = aggregator.get_points()

    #     self.assertEqual(len(points), 2)
    #     point = points[0]

    #     self.assertEqual(point['avg_value'], (12.3 + 0.3) / 2)
    #     self.assertEqual(point['min_value'], 0.3)
    #     self.assertEqual(point['max_value'], 12.3)
    #     self.assertEqual(point['sample_count'], 2)

    #     