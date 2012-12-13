"""
opbeat_python.contrib.async
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 Opbeat
:license: BSD, see LICENSE for more details.
"""

from collections import defaultdict


class StatSet(object):
    """
        Represents a set of stats for a metric
    """
    def __init__(self):
        self.avg_value = None
        self.min_value = None
        self.max_value = None
        self.sample_count = 0

    def update(self, value):
        """
            Update the stats-set with the given value
        """
        self.sample_count += 1

        if self.avg_value is None:
            self.avg_value = value
            self.min_value = value
            self.max_value = value
        else:
            # We could also just store the sum. It might get big though.
            self.avg_value = (
                        ((self.avg_value * (self.sample_count - 1)) + value)
                            / self.sample_count
                        )
            self.min_value = min(self.min_value, value)
            self.max_value = max(self.max_value, value)


class Aggregator(object):
    """
        Aggregates multiple datapoints into a single
        point per metric/segments values
    """
    def __init__(self):
        self.clear_values()

    def record_point(self, metric, value, segments={}):
        """
            Record a single point.
        """

        segments = frozenset(segments)  # this way it can be hashed
        key = (metric, segments)
        self.points[key].update(value)

    def get_values(self):
        """
            Extract values from aggregator.
        """
        return [
            {
                'segments':segments,
                'avg_value':statset.avg_value,
                'min_value':statset.min_value,
                'max_value':statset.max_value,
                'sample_count':statset.sample_count
            }
            for (metric, segments), statset in self.points.items()
        ]

    def clear_values(self):
        self.points = defaultdict(StatSet)

