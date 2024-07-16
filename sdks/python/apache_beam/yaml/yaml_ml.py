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

"""This module defines yaml wrappings for some ML transforms."""
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import options
from apache_beam.yaml.yaml_utils import SafeLineLoader

try:
  from apache_beam.ml.transforms import tft
  from apache_beam.ml.transforms.base import MLTransform
  # TODO(robertwb): Is this all of them?
  _transform_constructors = tft.__dict__
except ImportError:
  tft = None  # type: ignore


def normalize_ml(spec):
  if spec['type'] == 'RunInference':
    config = spec.get('config')
    for required in ('model_handler', ):
      if required not in config:
        raise ValueError(
            f'Missing {required} parameter in RunInference config '
            f'at line {SafeLineLoader.get_line(spec)}')
    model_handler = config.get('model_handler')
    if not isinstance(model_handler, dict):
      raise ValueError(
          'Invalid model_handler specification at line '
          f'{SafeLineLoader.get_line(spec)}. Expected '
          f'dict but was {type(model_handler)}.')
    for required in ('type', 'config'):
      if required not in model_handler:
        raise ValueError(
            f'Missing {required} in model handler '
            f'at line {SafeLineLoader.get_line(model_handler)}')
    typ = model_handler['type']
    extra_params = set(SafeLineLoader.strip_metadata(model_handler).keys()) - {
        'type', 'config'
    }
    if extra_params:
      raise ValueError(
          f'Unexpected parameters in model handler of type {typ} '
          f'at line {SafeLineLoader.get_line(spec)}: {extra_params}')
    model_handler_provider = ModelHandlerProvider.handler_types.get(typ, None)
    if model_handler_provider:
      model_handler_provider.validate(model_handler['config'])
    else:
      raise NotImplementedError(
          f'Unknown model handler type: {typ} '
          f'at line {SafeLineLoader.get_line(spec)}.')

  return spec


class ModelHandlerProvider:
  handler_types: Dict[str, Callable[..., "ModelHandlerProvider"]] = {}

  def __init__(self, handler, preprocess=None, postprocess=None):
    self._handler = handler
    self._preprocess = self.parse_processing_transform(
        preprocess, 'preprocess') or self.preprocess_fn
    self._postprocess = self.parse_processing_transform(
        postprocess, 'postprocess') or self.postprocess_fn

  def get_output_schema(self):
    return Any

  @staticmethod
  def parse_processing_transform(processing_transform, typ):
    def _parse_config(callable=None, path=None, name=None):
      if callable and (path or name):
        raise ValueError(
            f"Cannot specify 'callable' with 'path' and 'name' for {typ} "
            f"function.")
      if path and name:
        return python_callable.PythonCallableWithSource.load_from_script(
            FileSystems.open(path).read().decode(), name)
      elif callable:
        return python_callable.PythonCallableWithSource(callable)
      else:
        raise ValueError(
            f"Must specify one of 'callable' or 'path' and 'name' for {typ} "
            f"function.")

    if processing_transform:
      if isinstance(processing_transform, dict):
        return _parse_config(**processing_transform)
      else:
        raise ValueError("Invalid model_handler specification.")

  def underlying_handler(self):
    return self._handler

  def preprocess_fn(self, row):
    raise ValueError(
        'Handler does not implement a default preprocess '
        'method. Please define a preprocessing method using the '
        '\'preprocess\' tag.')

  def create_preprocess_fn(self):
    return lambda row: (row, self._preprocess(row))

  @staticmethod
  def postprocess_fn(x):
    return x

  def create_postprocess_fn(self):
    return lambda result: (result[0], self._postprocess(result[1]))

  @staticmethod
  def validate(model_handler_spec):
    raise NotImplementedError(type(ModelHandlerProvider))

  @classmethod
  def register_handler_type(cls, type_name):
    def apply(constructor):
      cls.handler_types[type_name] = constructor
      return constructor

    return apply

  @classmethod
  def create_handler(cls, model_handler_spec) -> "ModelHandlerProvider":
    typ = model_handler_spec['type']
    config = model_handler_spec['config']
    try:
      result = cls.handler_types[typ](**config)
      if not hasattr(result, 'to_json'):
        result.to_json = lambda: model_handler_spec
      return result
    except Exception as exn:
      raise ValueError(
          f'Unable to instantiate model handler of type {typ}. {exn}')


@ModelHandlerProvider.register_handler_type('VertexAIModelHandlerJSON')
class VertexAIModelHandlerJSONProvider(ModelHandlerProvider):
  def __init__(
      self,
      endpoint_id,
      endpoint_project,
      endpoint_region,
      preprocess=None,
      postprocess=None,
      experiment=None,
      network=None,
      private=False,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None):

    try:
      from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
    except ImportError:
      raise ValueError(
          'Unable to import VertexAIModelHandlerJSON. Please '
          'install gcp dependencies: `pip install apache_beam[gcp]`')

    _handler = VertexAIModelHandlerJSON(
        endpoint_id=str(endpoint_id),
        project=endpoint_project,
        location=endpoint_region,
        experiment=experiment,
        network=network,
        private=private,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
    )

    super().__init__(_handler, preprocess, postprocess)

  @staticmethod
  def validate(model_handler_spec):
    for required in ('endpoint_id', 'endpoint_project', 'endpoint_region'):
      if required not in model_handler_spec:
        raise ValueError(
            f'Missing {required} in model handler '
            f'at line {SafeLineLoader.get_line(model_handler_spec)}')

  def get_output_schema(self):
    return RowTypeConstraint.from_fields([('example', Any), ('inference', Any),
                                          ('model_id', Optional[str])])

  def create_postprocess_fn(self):
    return lambda result: (
        result[0],
        beam.Row(
            example=result[1].example,
            inference=self._postprocess(result[1].inference),
            model_id=result[1].model_id))


def normalize_input_fields(
    pcoll,
    keep: Optional[List[str]] = None,
    drop: Optional[List[str]] = None,
    only: Optional[str] = None):
  if len([arg for arg in (keep, drop, only) if arg]) > 1:
    raise ValueError("Must specify only one of 'keep', 'drop' or 'only'.")
  try:
    input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  except (TypeError, ValueError) as exn:
    if keep:
      raise ValueError("Can only 'keep' fields on a schema'd input.") from exn
    if drop:
      raise ValueError("Can only 'drop' fields on a schema'd input.") from exn
    elif only:
      raise ValueError("Can only specify 'only' on a schema'd input.") from exn
    input_schema = {}

  input_fields = set(input_schema.keys())
  if drop:
    if not isinstance(drop, list):
      raise ValueError("'drop' must specify a list of fields.")
    return input_fields.difference(set(drop))
  elif keep:
    if not isinstance(keep, list):
      raise ValueError("'keep' must specify a list of fields.")
    return set(keep)
  elif only:
    if not isinstance(drop, str):
      raise ValueError("'only' must specify a single string field.")
    return {only}

  return input_fields


@beam.ptransform.ptransform_fn
def run_inference(
    pcoll,
    model_handler: Dict,
    inference_tag: Optional[str] = 'inference',
    inference_args: Optional[Dict[str, Any]] = None,
    **filter_args) -> beam.PCollection[beam.Row]:

  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')
  input_fields = normalize_input_fields(pcoll, **filter_args)

  model_handler_provider = ModelHandlerProvider.create_handler(model_handler)

  filtered_input_pcoll = pcoll | beam.Select(*input_fields)
  schema = RowTypeConstraint.from_fields(
      list(
          RowTypeConstraint.from_user_type(
              filtered_input_pcoll.element_type.user_type)._fields) +
      [(inference_tag, model_handler_provider.get_output_schema())])

  output = (
      filtered_input_pcoll
      | 'RunInference' >> RunInference(
          model_handler=KeyedModelHandler(
              model_handler_provider.underlying_handler()).with_preprocess_fn(
                  model_handler_provider.create_preprocess_fn()).
          with_postprocess_fn(model_handler_provider.create_postprocess_fn()),
          inference_args=inference_args)
      | beam.Map(
          lambda row: beam.Row(**{
              inference_tag: row[1], **row[0]._asdict()
          })).with_output_types(schema))

  return output


def _config_to_obj(spec):
  if 'type' not in spec:
    raise ValueError(f"Missing type in ML transform spec {spec}")
  if 'config' not in spec:
    raise ValueError(f"Missing config in ML transform spec {spec}")
  constructor = _transform_constructors.get(spec['type'])
  if constructor is None:
    raise ValueError("Unknown ML transform type: %r" % spec['type'])
  return constructor(**spec['config'])


@beam.ptransform.ptransform_fn
def ml_transform(
    pcoll,
    write_artifact_location: Optional[str] = None,
    read_artifact_location: Optional[str] = None,
    transforms: Optional[List[Any]] = None):
  if tft is None:
    raise ValueError(
        'tensorflow-transform must be installed to use this MLTransform')
  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')
  # TODO(robertwb): Perhaps _config_to_obj could be pushed into MLTransform
  # itself for better cross-language support?
  return pcoll | MLTransform(
      write_artifact_location=write_artifact_location,
      read_artifact_location=read_artifact_location,
      transforms=[_config_to_obj(t) for t in transforms] if transforms else [])


if tft is not None:
  ml_transform.__doc__ = MLTransform.__doc__
