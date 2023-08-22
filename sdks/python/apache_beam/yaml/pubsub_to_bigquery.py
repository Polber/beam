import json
import logging

import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.runners import DataflowRunner
from apache_beam.yaml.yaml_transform import YamlTransform

class JsonToDict(beam.PTransform):
  def expand(self, pcolls):
    class ParseJson(beam.DoFn):
      def process(self, element, *args, **kwargs):
        j = json.loads(element.decode('utf-8'))
        return j['rows']
    return pcolls | "ParseJson" >> beam.ParDo(ParseJson())


def run():
  with beam.Pipeline(
      options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle',
        streaming=True,
        project='cloud-teleport-testing',
        temp_location="gs://jkinard-test-templates/",
        runner="dataflow",
        sdk_container_image="us-central1-docker.pkg.dev/cloud-teleport-testing/jkinard-test/dataflow/beam_python3.11_sdk:2.51.0.dev",
        sdk_location="container",
        sdk_harness_container_image_overrides=['.*python.*,us-central1-docker.pkg.dev/cloud-teleport-testing/jkinard-test/dataflow/beam_python3.11_sdk:2.51.0.dev'],
        experiments=["disable_worker_rolling_upgrade"])) as p:

    project = "cloud-teleport-testing"

    p | YamlTransform(
      f'''
      type: chain
      transforms:
        - type: ReadFromPubSub
          config:
            subscription: "projects/{project}/subscriptions/jkinard-yaml-test-sub" 
            with_attributes: False
        # - type: PyTransform
        #   config:
        #     constructor: "apache_beam.yaml.pubsub_to_bigquery.JsonToDict"
        - type: JsonToDict
        # - type: MapToFields
        #   fields:
        #     name:
        #       path: udf.py
        #       name: extract_name
        #     number:
        #       path: udf.py
        #       name: extract_number
        - type: WriteToBigQuery
          config:
            table: "jkinard-yaml-table"
            dataset: "jkinard_test"
            project: "{project}"
            schema: "name:STRING,number:INT64"
            create_disposition: "CREATE_IF_NEEDED"
            write_disposition: "WRITE_APPEND"
      providers:
        - type: "pythonPackage"
          config:
            packages:
              - /Users/jkinard/beam/sdks/python/apache_beam/yaml/transforms/dist/transforms-0.0.1.tar.gz
          transforms:
            JsonToDict: "transforms.src.transforms.json_to_dict.JsonToDict"
      ''')
    # , providers={'JsonToDict': InlineProvider({'JsonToDict': JsonToDict})})


if __name__ == '__main__':
  run()