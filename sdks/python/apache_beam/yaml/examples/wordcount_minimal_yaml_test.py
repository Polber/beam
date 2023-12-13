import unittest
from unittest import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml.examples.wordcount_minimal_yaml import \
  wordcount_minimal_yaml


def check_matches(actual):
  expected = '''
king: 311
lear: 253
dramatis: 1
personae: 1
of: 483
britain: 2
france: 32
duke: 26
burgundy: 20
cornwall: 75
'''.splitlines()[1:]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.yaml.examples.backend.run_yaml_example.print', str)
class WordCountMinimalYamlTest(unittest.TestCase):
  def test_wordcount_minimal_yaml(self):
    wordcount_minimal_yaml(check_matches)


if __name__ == '__main__':
  unittest.main()
