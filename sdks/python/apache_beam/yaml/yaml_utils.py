import functools
import inspect
from typing import NamedTuple

import apache_beam as beam
from apache_beam.transforms.core import _MaybePValueWithErrors


class ErrorHandlingConfig(NamedTuple):
  output: str


def exception_handling_args(error_handling_spec):
  if error_handling_spec:
    return {
        'dead_letter_tag' if k == 'output' else k: v
        for (k, v) in error_handling_spec.items()
    }
  else:
    return None


def _map_errors_to_standard_format():
  # TODO(https://github.com/apache/beam/issues/24755): Switch to MapTuple.
  return beam.Map(
      lambda x: beam.Row(element=x[0], msg=str(x[1][1]), stack=str(x[1][2])))


def maybe_with_exception_handling_transform_fn(transform_fn):
  @functools.wraps(transform_fn)
  def expand(pcoll, error_handling=None, **kwargs):
    wrapped_pcoll = _MaybePValueWithErrors(
        pcoll, exception_handling_args(error_handling))
    return transform_fn(wrapped_pcoll,
                        **kwargs).as_result(_map_errors_to_standard_format())

  original_signature = inspect.signature(transform_fn)
  new_parameters = list(original_signature.parameters.values())
  error_handling_param = inspect.Parameter(
      'error_handling',
      inspect.Parameter.KEYWORD_ONLY,
      default=None,
      annotation=ErrorHandlingConfig)
  if new_parameters[-1].kind == inspect.Parameter.VAR_KEYWORD:
    new_parameters.insert(-1, error_handling_param)
  else:
    new_parameters.append(error_handling_param)
  expand.__signature__ = original_signature.replace(parameters=new_parameters)

  return expand
