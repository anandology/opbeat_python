# -*- coding: utf-8 -*-

# from mock import Mock
from unittest2 import TestCase
from opbeat_python.metrics.aggregator import StatSet, Aggregator
from opbeat_python.metrics import (set_client, begin_measure, end_measure,
                                   flush_metrics, get_client,
                                   measure)


from time import sleep

from tests.helpers import get_tempstoreclient
import logging

logger = logging.getLogger()

class StatSetTest(TestCase):
    def test_init(self):
        StatSet()

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

        points = aggregator.get_and_clear_values()
        self.assertEqual(len(aggregator.points), 0)

        self.assertEqual(len(points), 1)
        point = points[0]

        self.assertEqual(point['avg_value'], (12.3 + 0.3) / 2)
        self.assertEqual(point['min_value'], 0.3)
        self.assertEqual(point['max_value'], 12.3)
        self.assertEqual(point['sample_count'], 2)

    def test_record_simple_point_with_segments(self):
        aggregator = Aggregator()
        aggregator.record_point('metric1', 12.3, {'key1': 'val1'})
        aggregator.record_point('metric1', 0.3, {'key1': 'val1'})

        points = aggregator.get_and_clear_values()
        self.assertEqual(len(aggregator.points), 0)

        self.assertEqual(len(points), 1)
        point = points[0]

        self.assertEqual(point['avg_value'], (12.3 + 0.3) / 2)
        self.assertEqual(point['min_value'], 0.3)
        self.assertEqual(point['max_value'], 12.3)
        self.assertEqual(point['segments'], {'key1': 'val1'})
        self.assertEqual(point['sample_count'], 2)

    def test_record_two_metrics(self):
        aggregator = Aggregator()
        aggregator.record_point('metric1', 12.3)
        aggregator.record_point('metric1', 0.2)
        aggregator.record_point('metric2', 0.3)
        aggregator.record_point('metric2', 11.1)

        points = sorted(
                        aggregator.get_and_clear_values(),
                        key=lambda d: d['metric']
                       )

        self.assertEqual(len(aggregator.points), 0)

        self.assertEqual(len(points), 2)

        point = points[0]

        self.assertEqual(point['metric'], 'metric1')
        self.assertEqual(point['avg_value'], (12.3 + 0.2) / 2)
        self.assertEqual(point['min_value'], 0.2)
        self.assertEqual(point['max_value'], 12.3)
        self.assertEqual(point['sample_count'], 2)

        point = points[1]
        self.assertEqual(point['metric'], 'metric2')
        self.assertEqual(point['avg_value'], (0.3 + 11.1) / 2)
        self.assertEqual(point['min_value'], 0.3)
        self.assertEqual(point['max_value'], 11.1)
        self.assertEqual(point['sample_count'], 2)


class MetricsTest(TestCase):

    def setUp(self):
        self.client = get_tempstoreclient()
        set_client(self.client)

        self.assertEqual(self.client, get_client())

    def test_measure_once(self):
        metric_name = 'my_metric1'
        begin_measure(metric_name)
        sleep(0.1)
        end_measure(metric_name)

        flush_metrics()

        self.assertEqual(len(self.client.events), 1)
        point = self.client.events[0]

        self.assertEqual(point[0]['metric'], metric_name)

    def test_measure_with_segments(self):
        metric_name = 'my_metric1'
        segments = {'key1': 'var1'}
        begin_measure(metric_name, segments)
        sleep(0.1)
        end_measure(metric_name, segments)

        flush_metrics()

        self.assertEqual(len(self.client.events), 1)
        point = self.client.events[0]

        logger.debug(point)

        self.assertEqual(point[0]['metric'], metric_name)
        self.assertEqual(
                            point[0]['points'][0]['values'][0]['segments'],
                            segments
                        )

        value = point[0]['points'][0]['values'][0]['avg_value']
        self.assertAlmostEqual(value, 0.1, places=2)

    def test_measure_context(self):
        metric_name = 'my_metric1'

        with measure(metric_name):
            sleep(0.1)

        flush_metrics()

        self.assertEqual(len(self.client.events), 1)
        point = self.client.events[0]

        logger.debug(point)

        self.assertEqual(point[0]['metric'], metric_name)
        self.assertEqual(
                            point[0]['points'][0]['values'][0]['segments'],
                            {}
                        )

        value = point[0]['points'][0]['values'][0]['avg_value']
        self.assertAlmostEqual(value, 0.1, places=2)

    def test_measure_context_with_segments(self):
        metric_name = 'my_metric1'
        segments = {'key1': 'var1'}
        with measure(metric_name, segments):
            sleep(0.1)

        with measure(metric_name, segments):
            sleep(0.1)

        flush_metrics()

        self.assertEqual(len(self.client.events), 1)
        point = self.client.events[0]

        logger.debug(point)

        self.assertEqual(point[0]['metric'], metric_name)
        self.assertEqual(
                            point[0]['points'][0]['values'][0]['segments'],
                            segments
        )

        value = point[0]['points'][0]['values'][0]
        self.assertAlmostEqual(value['avg_value'], 0.1, places=2)

        self.assertEqual(value['sample_count'], 2)
