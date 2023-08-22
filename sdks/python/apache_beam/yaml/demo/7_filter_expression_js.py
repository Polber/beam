import apache_beam
import apache_beam as beam

from apache_beam.yaml import yaml_transform
from apache_beam.options import pipeline_options
from apache_beam.yaml.demo.base_demo import BaseDemo


class SimplePipeline(BaseDemo):
  @staticmethod
  def run():
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      yaml_transform.expand_pipeline(
        p,
        '''
        pipeline:
          type: chain
          transforms:
            - type: ReadFromCsv
              name: ReadCsvFile
              config:
                path: resources/input.csv
            - type: Filter
              config:
                language: javascript
                keep: 
                  expression: "letter != 'a'"
            - type: WriteToJson
              name: WriteJsonFile
              config:
                path: resources/temp/output.json
        ''')


if __name__ == "__main__":
  SimplePipeline().main()
