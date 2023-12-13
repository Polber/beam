import unittest
from unittest import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml.examples.elementwise.regex_matches_yaml import \
  regex_matches_yaml


def check_matches(actual):
  expected = '''
🍓, Strawberry, perennial
🥕, Carrot, biennial
🍆, Eggplant, perennial
🍅, Tomato, annual
🥔, Potato, perennial
'''.splitlines()[1:]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.yaml.examples.backend.run_yaml_example.print', str)
class RegexYamlTest(unittest.TestCase):
  def test_matches_yaml(self):
    regex_matches_yaml(check_matches)


if __name__ == '__main__':
  unittest.main()
