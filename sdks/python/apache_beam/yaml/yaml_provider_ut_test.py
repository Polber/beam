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

"""Unit tests for cross-language generate sequence."""

# pytype: skip-file

import logging
import unittest

import apache_beam as beam
from apache_beam.yaml.yaml_mapping import MapToFields
from apache_beam.yaml.yaml_provider import ExternalProvider, InlineProvider, MetaInlineProvider
from apache_beam.utils import python_callable


class InlineProviderTest(unittest.TestCase):
  def __init__(self, method_name='runTest'):
    super().__init__(method_name)
    self._type = 'PyMap'
    self._func_str = 'lambda x: x*x'
    self.provider = InlineProvider(dict({self._type: lambda fn: beam.Map(
      python_callable.PythonCallableWithSource(fn))}))

  def test_available(self):
    self.assertEqual(True, self.provider.available())

  def test_provided_transforms(self):
    self.assertEqual([self._type], list(self.provider.provided_transforms()))

  def test_create_transform(self):
    def create_transform():
      return self.provider.create_transform(self._type, {'fn': self._func_str}, create_transform)

    self.assertEqual('<ParDo(PTransform) label=[Map(%s)]>' % self._func_str, str(create_transform()))

class MetaInlineProviderTest(unittest.TestCase):
  def __init__(self, method_name="runTest"):
    super().__init__(method_name)
    self._type = 'MapToFields'
    self.provider = MetaInlineProvider(dict({self._type: MapToFields}))

  def test_create_transform(self):
    def create_transform():
      return self.provider.create_transform(self._type, {'keep': "x==1"}, create_transform)

    self.assertEqual('<_PTransformFnPTransform(PTransform) label=[%s(create_transform)]>' % self._type, str(create_transform()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
