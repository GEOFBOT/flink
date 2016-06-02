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
from flink.functions import GroupReduceFunction


class AggregationFunction(GroupReduceFunction):
    def __init__(self, field):
        super(AggregationFunction, self).__init__()
        self.field = field

    def reduce(self, iterator, collector):
        item = iterator.next()
        collector.collect(self.aggregate(item))

    def aggregate(self, iterator):
        pass


class Sum(AggregationFunction):
    def aggregate(self, iterator):
        item = iterator.next()
        n = item[self.field]
        n += sum([x[self.field] for x in iterator])

        item[self.field] = n
        return item


class Min(AggregationFunction):
    def aggregate(self, iterator):
        item = iterator.next()
        n = item[self.field]
        for x in iterator:
            n2 = x[self.field]
            if n2 < n:
                n = n2

        item[self.field] = n
        return item


class Max(AggregationFunction):
    def aggregate(self, iterator):
        item = iterator.next()
        n = item[self.field]
        for x in iterator:
            n2 = x[self.field]
            if n2 > n:
                n = n2

        item[self.field] = n
        return item



