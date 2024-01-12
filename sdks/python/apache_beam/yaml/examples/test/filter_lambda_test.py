# coding=utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# pytype: skip-file

import unittest

from apache_beam.yaml.examples.test.test_yaml_example import test_yaml_example

YAML_FILE = '../transforms/elementwise/filter_callable.yaml'

EXPECTED = [
    "{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}",
    "{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}",
    "{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}"
]


class FilterYamlTest(unittest.TestCase):
  def test_filter_callable_yaml(self):
    test_yaml_example(YAML_FILE, EXPECTED)


if __name__ == '__main__':
  unittest.main()
