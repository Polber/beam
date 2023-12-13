import unittest

import yaml

from apache_beam.yaml.yaml_mapping import normalize_mapping
from apache_beam.yaml.yaml_transform import SafeLineLoader


class MainTest(unittest.TestCase):
  def test_normalize_mapping_converts_drop_to_iterable(self):
    spec_yaml = '''
      type: MapToFields
      config:
        drop: col
        fields:
          col2: col2
        '''
    spec = normalize_mapping(yaml.load(spec_yaml, Loader=SafeLineLoader))
    self.assertIsInstance(spec['config']['drop'], list)