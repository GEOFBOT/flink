# ###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from flink.functions.GroupReduceFunction import GroupReduceFunction

class AggregationFunction(GroupReduceFunction):
    def __init__(self, aggregation, field):
        super(AggregationFunction, self).__init__()
        self.aggregations = [(aggregation, field)]

    def add_aggregation(self, aggregation, field):
        """
        Add an additional aggregation operator
        :param aggregation: Built-in aggregation operator to apply
        :param field: Field on which to apply the specified aggregation
        """
        self.aggregations.append((aggregation, field))

    def reduce(self, iterator, collector):
        item = list(iterator.next())
        # Apply each specified aggregation operator onto corresponding field
        for x in iterator:
            for aggregation, field in self.aggregations:
                item[field] = aggregation(field).aggregate(item[field], x[field])

        collector.collect(tuple(item))


class AggregationOperator(object):
    def __init__(self, field):
        self.field = field

    def aggregate(self, num, num2):
        pass


class Sum(AggregationOperator):
    def aggregate(self, num, num2):
        return num + num2


class Min(AggregationOperator):
    def aggregate(self, num, num2):
        return num if num < num2 else num2


class Max(AggregationOperator):
    def aggregate(self, num, num2):
        return num if num > num2 else num2



