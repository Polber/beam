#
# Copyright (C) 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import argparse
import logging

import yaml

from apache_beam.io.filesystems import FileSystems
from apache_beam.yaml import main
from apache_beam.yaml import yaml_transform


def _pipeline_spec_from_args(known_args):
    if known_args.yaml:
        pipeline_yaml = FileSystems.open(known_args.yaml).read().decode()
    else:
        raise ValueError(
            "--yaml must be set.")

    return yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--yaml',
        dest='yaml',
        help='Input YAML file in Cloud Storage.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_spec = _pipeline_spec_from_args(known_args)

    print("Before: ")
    print(pipeline_spec)
    main.run(argv=pipeline_args + [f"--pipeline_spec={pipeline_spec}", "--save_main_session"]) #, '--provider_spec_file="/template/providers.yaml"'])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
