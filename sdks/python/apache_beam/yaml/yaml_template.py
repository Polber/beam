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

from apache_beam.options import pipeline_options


def preprocess_template(template):
  def _set_parameters(spec):
    for key, value in spec['config'].items():
      if isinstance(value, str) and value[0] == '$':
        if template:
          parameter = template.get_parameter(value[1:])
          if parameter:
            spec['config'][key] = parameter
          # else:
          #   raise ValueError(
          #       f'Missing parameter --{value[1:]}')
    return spec

  return _set_parameters


class YamlTemplate:

  _pipeline_options = None

  def __init__(self, template_spec: dict, options):
    self.name = template_spec.get('name', "Yaml_Template")
    self.display_name = template_spec.get('display_name', "Yaml Template")
    self.description = template_spec.get('description', "")
    self.set_options(
        self._extract_parameters(template_spec.get('parameters', {})), options)

  @classmethod
  def set_options(cls, parameters, options):
    template_options = cls.YamlTemplateOptions()
    for parameter in parameters:
      parameter.required &= parameter.name not in options._all_options
    template_options.set_parameters(parameters)
    cls._pipeline_options = options.view_as(cls.YamlTemplateOptions)

  class YamlTemplateParameter:
    def __init__(self, name, help_text, typ=str, required=False, default=None):
      self.name = name
      self.required = required
      self.default = default
      self.help_text = help_text
      self.type = typ
      self._validate()

    def _validate(self):
      pass

  class YamlTemplateOptions(pipeline_options.PipelineOptions):

    _parameters = []

    @classmethod
    def _add_argparse_args(cls, parser):
      for parameter in cls._parameters:
        parser.add_argument(
            f"--{parameter.name}",
            dest=parameter.name,
            required=parameter.required,
            type=parameter.type,
            default=parameter.default,
            help=parameter.help_text)

    @classmethod
    def set_parameters(cls, parameters):
      cls._parameters = parameters

  def _extract_parameters(self, template_spec):
    return [
        self.YamlTemplateParameter(
            parameter['name'],
            parameter['help'],
            typ=self.map_type(parameter, parameter.get('type', 'str')),
            required=parameter.get('required', None),
            default=parameter.get('default', None))
        for parameter in template_spec
    ]

  @staticmethod
  def map_type(parameter, typ):
    typ = str(typ).lower()
    if typ == 'int' or typ == 'integer':
      return int
    elif typ == 'float':
      return float
    elif typ == 'bool' or typ == 'boolean':
      return bool
    elif typ == 'str' or typ == 'string':
      return str
    raise ValueError(f"Unsupported type {typ} for parameter {parameter}")

  @classmethod
  def _set_options(cls, options):
    cls._pipeline_options = options

  @classmethod
  def get_parameter(cls, parameter):
    if parameter in cls._pipeline_options._visible_option_list():
      return cls._pipeline_options.__getattr__(parameter)
    return None
