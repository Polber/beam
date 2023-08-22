import apache_beam as beam

from apache_beam.yaml import yaml_transform
from apache_beam.options import pipeline_options
from apache_beam.yaml.demo.base_demo import BaseDemo


class PyFilter(BaseDemo):
  @staticmethod
  def run():
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      yaml_transform.expand_pipeline(
        p,
        '''
        pipeline:
          type: chain
          
          source:
            type: ReadFromCsv
            name: ReadCsvFile
            config:
              path: resources/input.csv
          
          transforms:
            - type: PyFilter
              name: FilterHalf
              config:
                keep: "lambda x: x.number > 2"
            - type: PyMap
              name: FlipIsAlpha
              config:
                fn: 'lambda x: {"letter": x.letter, "is_alpha": not x.is_alpha}'
            - type: PyTransform
              name: AddColumn
              config:
                constructor: apache_beam.yaml.demo.base_demo.AddColumn
            - type: WithSchema
              config:
                letter: str
                custom: int
                is_alpha: bool
          
          sink:
            type: WriteToJson
            name: WriteJsonFile
            config:
              path: resources/temp/output.json
        ''')


if __name__ == "__main__":
  PyFilter().main()
