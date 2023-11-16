---
title: "Flatten"
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

# Flatten

{{< localstorage language language-yaml >}}

[//]: # ({{< button-pydoc path="apache_beam.transforms.core" class="Flatten" >}})

Merges multiple `PCollection` objects into a single logical
`PCollection`. A transform for `PCollection` objects
that store the same data type.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#flatten).

## Examples

### Example 1: `Flatten` multiple `PCollection` objects.

The following example shows how to read from 2 CSV files, and flatten their rows into a single PCollection that is 
written out to a new CSV file. This assumes the data contained in the CSV files have compatible schemas. 

```
pipeline:
  transforms:
    - type: ReadFromCsv
      name: pc1
      config:
        path: /path/to/input1.csv
    - type: ReadFromCsv
      name: pc2
      config:
        path: /path/to/input2.csv
    - type: Flatten
      name: FlattenElements
      input: [ pc1, pc2 ]
    - type: WriteToCsv
      input: FlattenElements
      config:
        path: test_output
```

[//]: # (## Related transforms)

[//]: # ()
[//]: # (* [MapToFields]&#40;/documentation/transforms/yaml/elementwise/maptofields&#41; behaves the same as `Map`, but for)

[//]: # (  each input it might produce zero or more outputs.)

[//]: # ()
[//]: # ([//]: # &#40;{{< button-pydoc path="apache_beam.transforms.core" class="Filter" >}}&#41;)
