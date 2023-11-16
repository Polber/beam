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
from apache_beam.yaml import yaml_provider
# pytype: skip-file

from apache_beam.yaml.examples.backend.run_yaml_example import run_yaml_example

pipeline_spec = f"""
pipeline:
  type: chain
  transforms:
    - type: Create
      name: Gardening plants
      config:
        elements: {sorted(yaml_provider.standard_providers().keys())}
    - type: MapToFields
      name: Filter perennials
      config:
        language: python
        drop: [element]
        append: true
        fields:
          element:
            callable: "lambda row: str(row.element).split('-')[0]"
          value: "1"
    - type: Combine
      config:
        language: python
        group_by: element
        combine:
          value: sum
    - type: MapToFields
      config:
        fields:
          element: element
    - type: PyTransform
      name: Print results
      config:
        constructor: \
          apache_beam.yaml.examples.backend.run_yaml_example.PrintTransform
        args:
          element: "element"
options:
  yaml_experimental_features: Combine
"""

if __name__ == '__main__':
  run_yaml_example(pipeline_spec)
