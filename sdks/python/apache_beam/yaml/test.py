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

import argparse

import yaml

import apache_beam as beam
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import MillisInstant
from apache_beam.yaml import YamlTransform
from apache_beam.yaml import yaml_transform

# Workaround for https://github.com/apache/beam/issues/28151.
LogicalType.register_logical_type(MillisInstant)

YAML = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromBigtable
      config:
        table_id: read_table
        instance_id: test-id-20231009-221202-416761
        project_id: cloud-teleport-testing
    # - type: MapToFields
    #   config:
    #     language: javascript
    #     fields:
    #       col1: 
    #         callable: 'function fn(row) { return row.column_families["cf1"]["col1"] }'
    #       col2: 'column_families["cf1"]["col2"]'
    # - type: MapToFields
    #   config:
    #     language: python
    #     fields:
    #       col1value: int.from_bytes(col1[0].value)
    #       col2value: 
    #         callable: 'lambda row: int.from_bytes(row.col2[0].value)'
    # - type: WriteToCsv
    #   config:
    #     path: results
    # - type: WriteToBigtable
    #   config:
    #     table_id: write_table
    #     instance_id: test-id-20231009-200138-972054
    #     project_id: cloud-teleport-testing
'''

def run():

  # import cloudpickle
  # with open('/Users/jkinard/Documents/variable.pkl', 'rb') as file:
  #   r = cloudpickle.load(file)
  # print()

  with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
      pickle_library='cloudpickle')) as p:

    print("Building pipeline...")
    yaml_transform.expand_pipeline(p, YAML)
    print("Running pipeline...")


if __name__ == '__main__':
  run()
