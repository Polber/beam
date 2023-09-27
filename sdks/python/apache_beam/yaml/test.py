from apache_beam.yaml import yaml_transform

import apache_beam as beam


def run(argv=None):
  with open("test.yaml", "r") as file:
    pipeline_spec = file.read()

  with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
      project="cloud-teleport-testing",
      region="us-central1",
      temp_location="gs://jkinard-test-templates/temp",
      pickle_library='cloudpickle',
      streaming=True,
      runner="dataflow",
      sdk_harness_container_image_overrides=
      [".*python.*,us-central1-docker.pkg.dev/cloud-teleport-testing/jkinard-test/dataflow/beam_python3.11_sdk:2.52.0.dev"
       ],
      sdk_container_image=
      "us-central1-docker.pkg.dev/cloud-teleport-testing/jkinard-test/dataflow/beam_python3.11_sdk:2.52.0.dev",
      sdk_location="container",
      experiments=["disable_worker_rolling_upgrade"])) as p:
    print("Building pipeline...")
    pipeline = yaml_transform.expand_pipeline(p, pipeline_spec)
    print("Running pipeline...")


if __name__ == '__main__':
  run()
