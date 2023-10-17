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
from yaml import SafeLoader

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import MillisInstant
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform

# Workaround for https://github.com/apache/beam/issues/28151.
LogicalType.register_logical_type(MillisInstant)


def _configure_parser(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--pipeline_spec', help='A yaml description of the pipeline to run.')
  parser.add_argument(
      '--pipeline_spec_file',
      help='A file containing a yaml description of the pipeline to run.')
  parser.add_argument(
      '--provider_spec', help='A yaml description of the providers to include.')
  parser.add_argument(
      '--provider_spec_file',
      help='A file containing a yaml description of the providers to include.')
  return parser.parse_known_args(argv)


def _pipeline_spec_from_args(known_args):
  if known_args.pipeline_spec_file and known_args.pipeline_spec:
    raise ValueError(
        "Exactly one of pipeline_spec or pipeline_spec_file must be set.")
  elif known_args.pipeline_spec_file:
    with FileSystems.open(known_args.pipeline_spec_file) as fin:
      pipeline_yaml = fin.read().decode()
  elif known_args.pipeline_spec:
    pipeline_yaml = known_args.pipeline_spec
  else:
    raise ValueError(
        "Exactly one of pipeline_spec or pipeline_spec_file must be set.")

  return pipeline_yaml


def _provider_spec_from_args(known_args):
  if known_args.provider_spec_file and known_args.provider_spec:
    raise ValueError(
        "Only one of provider_spec or provider_spec_file can be set.")
  elif known_args.provider_spec_file:
    provider_yaml = FileSystems.open(
        known_args.provider_spec_file).read().decode()
  elif known_args.provider_spec:
    provider_yaml = known_args.provider_spec
  else:
    return None

  return yaml_provider.parse_providers(yaml.load(provider_yaml, Loader=SafeLoader))


def run(argv=None):
  yaml_transform._LOGGER.setLevel('INFO')
  known_args, pipeline_args = _configure_parser(argv)
  pipeline_yaml = _pipeline_spec_from_args(known_args)
  pipeline_spec = yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)

  provider_spec = _provider_spec_from_args(known_args)

  with beam.Pipeline(  # linebreak for better yapf formatting
      options=beam.options.pipeline_options.PipelineOptions(
          pipeline_args,
          pickle_library='cloudpickle',
          # project='cloud-teleport-testing',
          # temp_location='gs://jkinard-test-templates/temp',
          # runner="dataflow",
          # sdk_location='container',
          # sdk_harness_container_image_overrides=
          # [".*python.*,us-central1-docker.pkg.dev/cloud-teleport-testing/jkinard-test/dataflow/beam_python3.11_sdk:2.52.0.dev"
          #  ],
          # experiments=["disable_worker_rolling_upgrade"],
          streaming=True,
          **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
              'options', {}))),
      display_data={'yaml': pipeline_yaml}) as p:
    print("Building pipeline...")
    print(pipeline_spec)
    yaml_transform.expand_pipeline(p, pipeline_spec, providers=provider_spec)
    print("Running pipeline...")


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()
