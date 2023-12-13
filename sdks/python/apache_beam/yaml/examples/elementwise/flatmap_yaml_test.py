import unittest
from unittest import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml.examples.elementwise.flatmap_lambda_yaml import \
  flatmap_lambda_yaml


def check_plants(actual):
  expected = '''
🍓Strawberry
🥕Carrot
🍆Eggplant
🍅Tomato
🥔Potato
'''.splitlines()[1:]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.yaml.examples.backend.run_yaml_example.print', str)
class FlatMapYamlTest(unittest.TestCase):
  def test_flatmap_lambda_yaml(self):
    flatmap_lambda_yaml(check_plants)


if __name__ == '__main__':
  unittest.main()
