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
import unittest

import yaml

from apache_beam.yaml.yaml_template import preprocess_template
from apache_beam.yaml.yaml_template import YamlTemplate
from apache_beam.yaml.yaml_transform import SafeLineLoader

import apache_beam as beam


class YamlTemplateTest(unittest.TestCase):
  def __init__(self, methodName="runYamlTemplateTest"):
    unittest.TestCase.__init__(self, methodName)
    self.parser = argparse.ArgumentParser()


  def test_preprocess_template(self):
    pipeline_args = [f'--test_parameter=test']
    options = beam.options.pipeline_options.PipelineOptions(
          pipeline_args)
    spec = '''
          template:
            name: "My_Test_Template"
            display_name: "Test Yaml Template"
            description: "A template for testing Beam Yaml templates."
          
            parameters:
              - name: "test_parameter"
                help: "Test parameter."
                required: true
          pipeline:
            type: chain
            transforms:
              - type: MyTransform
                config:
                  input: $test_parameter
          '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    pipeline = spec['pipeline']
    template = spec['template']
    transforms = pipeline['transforms']
    processed = preprocess_template(YamlTemplate(template, options))(transforms[0])
    self.assertEqual(processed['config']['input'], 'test')