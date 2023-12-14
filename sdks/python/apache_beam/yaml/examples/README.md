# Examples Catalog

This module contains a series of Beam YAML code samples that can be run using
the Beam YAML framework using the command
```
python -m apache_beam.yaml.main --pipeline_spec_file=/path/to/example.yaml
```

## Wordcount
A good starting place is the Wordcount example under the root example directory.
This example reads in a text file, splits the text on each word, groups by each 
word, and counts the occurrence of each word. This is a classic example used in
the other SDK's and shows off many of the functionalities of Beam YAML.

## Element-wise
These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`. More information can be found about mapping transforms
[here](
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/yaml_mapping.md).

## Aggregation
These examples are experimental and require that 
`yaml_experimental_features: Combine` be specified under the `options` tag, or
by passing `--yaml_experimental_features=Combine` to the command to run the 
pipeline. i.e.
```
python -m apache_beam.yaml.main \
  --pipeline_spec_file=/path/to/example.yaml \
  --yaml_experimental_features=Combine
```
More information can be found about mapping transforms
[here](
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/yaml_combine.md).