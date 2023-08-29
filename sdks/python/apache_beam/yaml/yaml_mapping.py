#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This module defines the basic MapToFields operation."""
import builtins
import itertools
import json

import apache_beam as beam
from apache_beam.typehints import row_type
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import yaml_provider


# TODO(yaml) Consider adding optional language version parameter to support ECMAScript 5 and 6
def expand_mapping_func(
    transform_name,
    language,
    original_fields,
    expression=None,
    callable=None,
    path=None,
    name=None):

  # Argument checking
  if not expression and not callable and not path and not name:
    raise ValueError(
        f'{transform_name} must specify either "expression", "callable", or both "path" and "name"'
    )
  if expression and callable:
    raise ValueError(
        f'{transform_name} cannot specify "expression" and "callable"')
  if (expression or callable) and (path or name):
    raise ValueError(
        f'{transform_name} cannot specify "expression" or "callable" with "path" or "name"'
    )
  if path and not name:
    raise ValueError(f'{transform_name} cannot specify "path" without "name"')
  if name and not path:
    raise ValueError(f'{transform_name} cannot specify "name" without "path"')

  def _get_udf_from_file():
    # Local UDF file case
    if not path.startswith("gs://"):
      try:
        with open(path, 'r') as local_udf_file:
          return local_udf_file.read()
      except Exception as e:
        raise IOError(f'Error opening file "{path}": {e}')

    # GCS UDF file case
    from google.cloud import storage

    # Parse GCS file location
    gcs_file_parts = str(path[5:]).split('/')
    gcs_bucket_name = gcs_file_parts[0]
    gcs_folder = '/'.join(gcs_file_parts[1:])

    # Instantiates a client and downloads file to string
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_folder)
    gcs_file = blob.download_as_string().decode('utf-8')

    return gcs_file

  def _check_file_ext(ext):
    if not path.endswith(ext):
      raise ValueError(f'File "{path}" is not a valid {ext} file.')

  def _js2py_import():
    return (['  try:'] + ['    import js2py'] + ['  except ImportError:'] + [
        '    raise ImportError("js2py must be installed to run javascript UDF\'s for YAML mapping transforms.")'
    ])

  # Javascript UDF case
  if language == 'javascript':
    # Javascript expression case
    if expression:
      parameters = [name for name in original_fields if name in expression]
      js_func = 'function fn(' + ','.join(
          [name for name in parameters]) + '){' + 'return (' + expression + ')}'
      source = '\n'.join(['def fn(__row__):'] + _js2py_import() +
                         [f'  {name} = __row__.{name}' for name in parameters] +
                         [
                             f'  return js2py.eval_js(\'\'\'{js_func}\'\'\')(' +
                             ','.join([name for name in parameters]) + ')'
                         ])
    else:
      # Javascript file UDF case
      if not callable:
        _check_file_ext('.js')
        udf_code = _get_udf_from_file()
      # Javascript inline UDF case
      else:
        udf_code = callable

      source = '\n'.join(['def fn(__row__):'] + _js2py_import() + [
          '  row_dict = {label: getattr(__row__, label) for label in __row__._fields}'
      ] + ([f'  return js2py.eval_js(\'\'\'{udf_code}\'\'\')(row_dict)']
           if callable else ['  js = js2py.EvalJs()'] +
           [f'  js.eval(\'\'\'{udf_code}\'\'\')'] +
           [f'  return getattr(js, "{name}")(row_dict)']))
  # Python UDF case
  else:
    # Python expression case
    if expression:
      # TODO(robertwb): Consider constructing a single callable that takes
      ## the row and returns the new row, rather than invoking (and unpacking)
      ## for each field individually.
      source = '\n'.join(['def fn(__row__):'] + [
          f'  {name} = __row__.{name}'
          for name in original_fields if name in expression
      ] + ['  return (' + expression + ')'])

    # Python file UDF case
    elif path and name:
      _check_file_ext('.py')
      py_file = _get_udf_from_file()

      # Parse file into python function using given method name
      return python_callable.PythonCallableWithSource.load_from_script(
          py_file, name)

    # Python inline UDF case - already valid python code
    else:
      source = callable

  # Return source UDF code as python callable
  return python_callable.PythonCallableWithSource(source)


def _as_callable(original_fields, expr, transform_name, language):
  if expr in original_fields:
    return expr

  # TODO(yaml): support a type parameter
  # TODO(yaml): support an imports parameter
  # TODO(yaml): support a requirements parameter (possibly at a higher level)
  if isinstance(expr, str):
    expr = {'expression': expr}
  if not isinstance(expr, dict):
    raise ValueError(
        f"Ambiguous expression type (perhaps missing quoting?): {expr}")
  elif len(expr) != 1 and ('path' not in expr or 'name' not in expr):
    raise ValueError(f"Ambiguous expression type: {list(expr.keys())}")

  return expand_mapping_func(transform_name, language, original_fields, **expr)


# TODO(yaml): This should be available in all environments, in which case
# we choose the one that matches best.
class _Explode(beam.PTransform):
  def __init__(self, fields, cross_product):
    self._fields = fields
    self._cross_product = cross_product
    self._exception_handling_args = None

  def expand(self, pcoll):
    all_fields = [
        x for x, _ in named_fields_from_element_type(pcoll.element_type)
    ]
    to_explode = self._fields

    def explode_cross_product(base, fields):
      if fields:
        copy = dict(base)
        for value in base[fields[0]]:
          copy[fields[0]] = value
          yield from explode_cross_product(copy, fields[1:])
      else:
        yield beam.Row(**base)

    def explode_zip(base, fields):
      to_zip = [base[field] for field in fields]
      copy = dict(base)
      for values in itertools.zip_longest(*to_zip, fillvalue=None):
        for ix, field in enumerate(fields):
          copy[field] = values[ix]
        yield beam.Row(**copy)

    return (
        beam.core._MaybePValueWithErrors(
            pcoll, self._exception_handling_args)
        | beam.FlatMap(
            lambda row: (
                explode_cross_product if self._cross_product else explode_zip)(
                    {name: getattr(row, name) for name in all_fields},  # yapf
                    to_explode))
        ).as_result()

  def infer_output_type(self, input_type):
    return row_type.RowTypeConstraint.from_fields([(
        name,
        trivial_inference.element_type(typ) if name in self._fields else
        typ) for (name, typ) in named_fields_from_element_type(input_type)])

  def with_exception_handling(self, **kwargs):
    # It's possible there's an error in iteration...
    self._exception_handling_args = kwargs
    return self


# TODO(yaml): Should Filter and Explode be distinct operations from Project?
# We'll want these per-language.
@beam.ptransform.ptransform_fn
def _PythonProjectionTransform(
    pcoll,
    *,
    fields,
    transform_name,
    language,
    keep=None,
    explode=(),
    cross_product=True,
    error_handling=None):
  original_fields = [
      name for (name, _) in named_fields_from_element_type(pcoll.element_type)
  ]

  if error_handling is None:
    error_handling_args = None
  else:
    error_handling_args = {
        'dead_letter_tag' if k == 'output' else k: v
        for (k, v) in error_handling.items()
    }

  pcoll = beam.core._MaybePValueWithErrors(pcoll, error_handling_args)

  if keep:
    if isinstance(keep, str) and keep in original_fields:
      keep_fn = lambda row: getattr(row, keep)
    else:
      keep_fn = _as_callable(original_fields, keep, transform_name, language)
    filtered = pcoll | beam.Filter(keep_fn)
  else:
    filtered = pcoll

  # TODO(yaml) - Is there a better way to convert to pvalue.Row
  # for Filter transform without calling beam.Select
  projected = filtered | beam.Select(
      **{
          name: _as_callable(original_fields, expr, transform_name, language)
          for (name, expr) in fields.items()
      })

  if explode:
    result = projected | _Explode(explode, cross_product=cross_product)
  else:
    result = projected

  return result.as_result(
      beam.MapTuple(
          lambda element,
          exc_info: beam.Row(
              element=element, msg=str(exc_info[1]), stack=str(exc_info[2]))))


@beam.ptransform.ptransform_fn
def MapToFields(
    pcoll,
    yaml_create_transform,
    *,
    fields,
    keep=None,
    explode=(),
    cross_product=None,
    append=False,
    drop=(),
    language=None,
    error_handling=None,
    transform_name="MapToFields",
    **language_keywords):

  if isinstance(explode, str):
    explode = [explode]
  if cross_product is None:
    if len(explode) > 1:
      # TODO(robertwb): Consider if true is an OK default.
      raise ValueError(
          'cross_product must be specified true or false '
          'when exploding multiple fields')
    else:
      # Doesn't matter.
      cross_product = True

  input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  if drop and not append:
    raise ValueError("Can only drop fields if append is true.")
  for name in drop:
    if name not in input_schema:
      raise ValueError(f'Dropping unknown field "{name}"')
  for name in explode:
    if not (name in fields or (append and name in input_schema)):
      raise ValueError(f'Exploding unknown field "{name}"')
  if append:
    for name in fields:
      if name in input_schema and name not in drop:
        raise ValueError(f'Redefinition of field "{name}"')

  if append:
    fields = {
        **{name: name
           for name in input_schema.keys() if name not in drop},
        **fields
    }

  if language is None:
    for name, expr in fields.items():
      if not isinstance(expr, str) or expr not in input_schema:
        # TODO(robertw): Could consider defaulting to SQL, or another
        # lowest-common-denominator expression language.
        raise ValueError("Missing language specification.")

    # We should support this for all languages.
    language = "python"

  if language in ("sql", "calcite"):
    if error_handling:
      raise ValueError('Error handling unsupported for sql.')
    selects = [f'{expr} AS {name}' for (name, expr) in fields.items()]
    query = "SELECT " + ", ".join(selects) + " FROM PCOLLECTION"
    if keep:
      query += " WHERE " + keep

    result = pcoll | yaml_create_transform({
        'type': 'Sql',
        'config': {
            'query': query, **language_keywords
        },
    }, [pcoll])
    if explode:
      # TODO(yaml): Implement via unnest.
      result = result | _Explode(explode, cross_product)

    return result

  elif language == 'python' or language == 'javascript':
    return pcoll | yaml_create_transform({
        'type': 'PyTransform',
        'config': {
            'constructor': __name__ + '._PythonProjectionTransform',
            'kwargs': {
                'fields': fields,
                'transform_name': transform_name,
                'language': language,
                'keep': keep,
                'explode': explode,
                'cross_product': cross_product,
                'error_handling': error_handling,
            },
            **language_keywords
        },
    }, [pcoll])

  else:
    # TODO(yaml): Support javascript expressions and UDFs.
    # TODO(yaml): Support java by fully qualified name.
    # TODO(yaml): Maybe support java lambdas?
    raise ValueError(
        f'Unknown language: {language}. '
        'Supported languages are "sql" (alias calcite) and "python."')


def create_mapping_provider():
  # These are MetaInlineProviders because their expansion is in terms of other
  # YamlTransforms, but in a way that needs to be deferred until the input
  # schema is known.
  return yaml_provider.MetaInlineProvider({
      'MapToFields': MapToFields,
      'Filter': (
          lambda yaml_create_transform,
          keep,
          **kwargs: MapToFields(
              yaml_create_transform,
              keep=keep,
              fields={},
              append=True,
              transform_name='Filter',
              **kwargs)),
      'Explode': (
          lambda yaml_create_transform,
          explode,
          **kwargs: MapToFields(
              yaml_create_transform,
              explode=explode,
              fields={},
              append=True,
              transform_name='Explode',
              **kwargs)),
  })
