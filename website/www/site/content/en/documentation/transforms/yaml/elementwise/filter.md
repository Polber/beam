---
title: "Filter"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Filter

{{< localstorage language language-yaml >}}

[//]: # ({{< button-pydoc path="apache_beam.transforms.core" class="Filter" >}})

Given a [User-defined Function (UDF)](/documentation/transforms/yaml/udfs), filter out all elements that don't satisfy 
the predicate. May also be used to filter based on an inequality with a given value based on the comparison ordering of
the element.

### Config parameters

#### Required
- `keep` - [UDF](/documentation/transforms/yaml/udfs) used to filter elements. If none of the sub-fields are specified, 
the function will default to `expression`.

#### Optional

* `language` - The language used in the `keep` function. When omitted, the language will default to
'generic' which can only be used with the `expression` sub-field. Available languages
are 'python', 'sql', and experimental support for 'javascript'.

## Examples

`Filter` accepts a function that keeps elements that return `True`, and filters out the remaining elements.

The following examples assume an input PCollection of Beam Rows, each with the following schema:

```
type: 'object'
properties:
  col1:
    type: 'number'
  col2:
    type: 'object'
    properties:
      str:
        type: 'string'
      values:
        type: 'array'
        items:
          type: 'number'
```


### Example 1: Filtering with a generic expression

We provide an expression which returns `True` if the element's `col1` is positive, and `False` otherwise. The input is
created using the [Create](/documentation/transforms/yaml/other/create) transform and written to a JSON file.

```
pipeline:
  transforms:
    - type: Create
      name: CreateData
      config:
        elements:
          - col1: 0
            col2:
              - str: "a"
              - values: [1,2,3]
          - col1: 1
            col2:
              - str: "b"
              - values: [4,5,6]
          - col1: 2
            col2:
              - str: "c"
              - values: [7,8,9]
    - type: Filter
      name: FilterOutElements
      input: CreateData
      config:
        language: python
        keep:
          expression: col1 > 0
    - type: WriteToJson
      name: WriteResults
      input: FilterOutElements
      config:
        path: /path/to/output
```

This can be shorthanded by putting the expression inline with `keep`.

```
- type: Filter
  config:
    keep: col1 > 0
```

### Example 2: Filtering with a language-specific expression

<span class="language-js">
Beam YAML has EXPERIMENTAL ability to use JavaScript functions.
Currently `javascript` needs to be in the `yaml_experimental_features` pipeline
option to use this feature.  
  
i.e. `python -m apache_beam.yaml.main --pipeline_spec_file=/path/to/pipeline.yaml --yaml_experimental_features=javascript`

</span>

{{< highlight py >}}
- type: Filter
  config:
    language: python
    keep: 
      expression: str.upper() != "BAD_STRING"
{{< /highlight >}}
{{< highlight java >}}
- type: Filter
  config:
    language: java
    keep: 
      expression: str.toUpperCase() != "BAD_STRING";
{{< /highlight >}}
{{< highlight js >}}
- type: Filter
  config:
    language: javascript
    keep:
      expression: str.toUpperCase() != "BAD_STRING";
{{< /highlight >}}

This can be shorthanded by putting the expression inline with `keep`.
{{< highlight py >}}
- type: Filter
  config:
    language: python
    keep: str.upper() != "BAD_STRING"
{{< /highlight >}}
{{< highlight java >}}
- type: Filter
  config:
    language: java
    keep: str.toUpperCase() != "BAD_STRING";
{{< /highlight >}}
{{< highlight js >}}
- type: Filter
  config:
    language: javascript
    keep: str.toUpperCase() != "BAD_STRING";
{{< /highlight >}}

### Playground
{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FilterLambdaYaml" show="filter_lambda_yaml" >}}
{{< /playground >}}

[//]: # (## Related transforms)

[//]: # ()
[//]: # (* [MapToFields]&#40;/documentation/transforms/yaml/elementwise/maptofields&#41; behaves the same as `Map`, but for)

[//]: # (  each input it might produce zero or more outputs.)

[//]: # ()
[//]: # ([//]: # &#40;{{< button-pydoc path="apache_beam.transforms.core" class="Filter" >}}&#41;)
