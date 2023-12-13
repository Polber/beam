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
# pylint:disable=line-too-long

import unittest
from unittest import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml.examples.elementwise.filter_lambda_yaml import \
  filter_lambda_yaml


def check_perennials(actual):
  expected = '''
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
'''.splitlines()[1:]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.yaml.examples.backend.run_yaml_example.print', str)
class FilterYamlTest(unittest.TestCase):
  def test_filter_lambda_yaml(self):
    filter_lambda_yaml(check_perennials)


if __name__ == '__main__':
  unittest.main()
