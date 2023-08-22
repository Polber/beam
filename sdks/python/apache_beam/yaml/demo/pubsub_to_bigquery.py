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
"""A word-counting workflow."""

import apache_beam as beam
import argparse
import logging
import yaml
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.yaml import yaml_transform


def _pipeline_spec_from_args(known_args):
    if known_args.yaml:
        path = known_args.yaml
        if path.startswith("gs://"):
            # GCS UDF file case
            from google.cloud import storage

            # Parse GCS file location
            gcs_file_parts = str(path[5:]).split('/')
            gcs_bucket_name = gcs_file_parts[0]
            gcs_folder = '/'.join(gcs_file_parts[1:])

            # Instantiates a client and downloads file to string
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(gcs_bucket_name)
            blob = bucket.blob(gcs_folder)
            gcs_file = blob.download_as_string().decode('utf-8')

            return gcs_file
        else:
            with open(path) as fin:
                pipeline_yaml = fin.read()
    else:
        raise ValueError(
            "--yaml must be set.")

    return yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the YAML pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--yaml',
        dest='yaml',
        help='Input YAML file in Cloud Storage or local files .')
    parser.add_argument(
        '--sdk_harness_container_image_overrides',
        dest='sdk_harness_container_image_overrides',
        help='SDK harness image override for python expansion service containers.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    if known_args.sdk_harness_container_image_overrides:
        pipeline_options = PipelineOptions(pipeline_args, pickle_library='cloudpickle',
                                           sdk_harness_container_image_overrides=
                                           [known_args.sdk_harness_container_image_overrides],
                                           streaming=True)
    else:
        pipeline_options = PipelineOptions(pipeline_args, pickle_library='cloudpickle')
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_spec = _pipeline_spec_from_args(known_args)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        yaml_transform.expand_pipeline(p, pipeline_spec)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
