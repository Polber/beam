import apache_beam as beam

from apache_beam.yaml import yaml_transform
from apache_beam.options import pipeline_options
from apache_beam.yaml.demo.base_demo import BaseDemo


class Complex(BaseDemo):
  @staticmethod
  def run():
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      yaml_transform.expand_pipeline(
        p,
        '''
        pipeline:
          
          source:
            type: ReadFromCsv
            name: ReadCsvFile
            config:
              path: resources/input.csv
          
          transforms:
            - type: PyFilter
              name: FilterHalf
              input: ReadCsvFile
              config:
                keep: "lambda x: x.number > 2"
            - type: PyMap
              name: FlipIsAlpha
              input: FilterHalf
              config:
                fn: 'lambda x: {"letter": x.letter, "is_alpha": not x.is_alpha}'
            - type: WithSchema
              name: FlipIsAlphaSchema
              input: FlipIsAlpha
              config:
                letter: str
                is_alpha: bool
            - type: PyMap
              name: AddNumber
              input: FilterHalf
              config:
                fn: 'lambda x: {"letter": x.letter, "number": x.number + 1}'
            - type: WithSchema
              name: AddNumberSchema
              input: AddNumber
              config:
                letter: str
                number: int
            - type: Sql
              config:
                query: select left.letter, left.number, right.is_alpha from left join right using (letter)
              input:
                left: AddNumberSchema
                right: FlipIsAlphaSchema
            - type: chain
              name: ExtraProcessingForBigRows
              input: Sql
              transforms:
                - type: PyTransform
                  name: AddColumn
                  config:
                    constructor: apache_beam.yaml.demo.base_demo.AddColumn
                - type: WithSchema
                  config:
                    letter: str
                    number: int
                    custom: int
                    is_alpha: bool
          
          sink:
            type: WriteToJson
            name: WriteJsonFile
            input: ExtraProcessingForBigRows
            config:
              path: resources/temp/output.json
        ''')


if __name__ == "__main__":
  Complex().main()
