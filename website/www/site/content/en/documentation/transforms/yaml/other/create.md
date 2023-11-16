---
title: "Create"
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

# Create

{{< localstorage language language-yaml >}}

Creates a collection containing a specified set of elements. This is useful for testing, 
as well as creating an initial input to process in parallel. 

Elements given as YAML/JSON-style mappings will be interpreted as Beam rows. More information about Beam Rows
can be found [here](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/values/Row.html).

### Config parameters

#### Required
- `elements` - The PCollection elements to pass on to the rest of the pipeline. 
These can be given within a JSON or YAML structure to provide a schema for the elements.

## Examples

### Example 1: Create a collection of elements

The following example shows how to create a collection of 5 numbers and write them out to a CSV file.

```
pipeline:
  transforms:
    - type: Create
      name: CreateElements
      config:
        elements: [0, 1, 2, 3, 4]
    - type: WriteToCsv
      name: WriteResults
      input: CreateElements
      config:
        path: "/path/to/output"
```

### Example 2: Create a collection of Beam Rows using JSON

If the goal is to create a collection with a schema, a schema can be provided in JSON format. Each element pass
to the transform must have the same schema.

```
- type: Create
  config:
    elements:
      - {first: 0, second: {str: "foo", values: [1, 2, 3]}}
      - {first: 1, second: {str: "bar", values: [4, 5, 6]}}
```

The resulting rows will have the following structure.

<table class="table-bordered table-striped">
  <thead>
    <tr><th>first</th><th colspan="2">second</th></tr>
    <tr><th></th><th>str</th><th>values</th></tr>
  </thead>
  <tbody>
    <tr><td>0</td><td>"foo"</td><td>1,2,3</td></tr>
    <tr><td>1</td><td>"bar"</td><td>4,5,6</td></tr>
  </tbody>
</table>

### Example 3: Create a collection of Beam Rows using YAML

We create a Beam Row using the same data as Example 2, except we define it using YAML instead of JSON.

```
- type: Create
  config:
    elements:
      - first: 0
        second:
          str: "foo"
          values: [1, 2, 3]
      - first: 1
        second:
          str: "bar"
          values: [4, 5, 6]
```
