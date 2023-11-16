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

import apache_beam as beam
from apache_beam.utils import python_callable

from apache_beam.yaml import main
from apache_beam.yaml import cache_provider_artifacts


class PrintTransform(beam.PTransform):
  def __init__(self, element=None):
    super().__init__()
    self._element = element

  def expand(self, p):
    def get_map_print():
      if self._element is not None:
        return beam.Map(
            python_callable.PythonCallableWithSource(
                f"lambda row: print(row.{self._element})"))
      return beam.Map(print)

    return p | get_map_print()


def run_yaml_example(pipeline_spec):
  cache_provider_artifacts.cache_provider_artifacts()
  main.run(argv=[f"--pipeline_spec={pipeline_spec}", "--save_main_session"])
