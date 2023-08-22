import glob
import os
import warnings

import pandas as pd
import apache_beam as beam


class AddColumn(beam.PTransform):
  def expand(self, pcolls):
    class AddColumnDoFn(beam.DoFn):
      def process(self, element, *args, **kwargs):
        element['custom'] = 0
        return [element]
    return pcolls | "AddColumn" >> beam.ParDo(AddColumnDoFn())


class BaseDemo:
  def main(self):
    with warnings.catch_warnings():
      warnings.filterwarnings("ignore", category=DeprecationWarning)
      # Inspect CSV
      self.inspect_csv()
      # Run pipeline
      self.run()
      # Inspect JSON
      self.inspect_json()
      self.clean_up()

  @staticmethod
  def run():
    pass

  @staticmethod
  def inspect_csv():
    print("Input CSV:")
    print(pd.read_csv('resources/input.csv'))
    print()

  @staticmethod
  def inspect_json():
    print("Output JSON:")
    with open('resources/temp/output.json-00000-of-00001', 'r') as fout:
      print(fout.read().replace(',', ',\n\t').replace('{', '{\n\t').replace(':', ': ').replace('}', '\n}'))

  @staticmethod
  def clean_up():
    for f in glob.glob("resources/temp/.temp*"):
      os.rmdir(f)
    os.remove('resources/temp/output.json-00000-of-00001')
    os.rmdir('resources/temp')
