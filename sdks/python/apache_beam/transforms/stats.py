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

"""This module has all statistic related transforms."""

from __future__ import absolute_import
from __future__ import division

import heapq
import itertools
import math
import sys
import typing
import warnings
from builtins import round
from functools import cmp_to_key

from apache_beam import coders
from apache_beam import typehints
from apache_beam.transforms.core import *
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.ptransform import PTransform

__all__ = [
    'ApproximateQuantiles',
    'ApproximateUnique',
]

# Type variables
T = typing.TypeVar('T')
K = typing.TypeVar('K')
V = typing.TypeVar('V')


class ApproximateUnique(object):
  """
  Hashes input elements and uses those to extrapolate the size of the entire
  set of hash values by assuming the rest of the hash values are as densely
  distributed as the sample space.
  """

  _NO_VALUE_ERR_MSG = 'Either size or error should be set. Received {}.'
  _MULTI_VALUE_ERR_MSG = 'Either size or error should be set. ' \
                         'Received {size = %s, error = %s}.'
  _INPUT_SIZE_ERR_MSG = 'ApproximateUnique needs a size >= 16 for an error ' \
                        '<= 0.50. In general, the estimation error is about ' \
                        '2 / sqrt(sample_size). Received {size = %s}.'
  _INPUT_ERROR_ERR_MSG = 'ApproximateUnique needs an estimation error ' \
                         'between 0.01 and 0.50. Received {error = %s}.'

  @staticmethod
  def parse_input_params(size=None, error=None):
    """
    Check if input params are valid and return sample size.

    :param size: an int not smaller than 16, which we would use to estimate
      number of unique values.
    :param error: max estimation error, which is a float between 0.01 and 0.50.
      If error is given, sample size will be calculated from error with
      _get_sample_size_from_est_error function.
    :return: sample size
    :raises:
      ValueError: If both size and error are given, or neither is given, or
      values are out of range.
    """

    if None not in (size, error):
      raise ValueError(ApproximateUnique._MULTI_VALUE_ERR_MSG % (size, error))
    elif size is None and error is None:
      raise ValueError(ApproximateUnique._NO_VALUE_ERR_MSG)
    elif size is not None:
      if not isinstance(size, int) or size < 16:
        raise ValueError(ApproximateUnique._INPUT_SIZE_ERR_MSG % (size))
      else:
        return size
    else:
      if error < 0.01 or error > 0.5:
        raise ValueError(ApproximateUnique._INPUT_ERROR_ERR_MSG % (error))
      else:
        return ApproximateUnique._get_sample_size_from_est_error(error)

  @staticmethod
  def _get_sample_size_from_est_error(est_err):
    """
    :return: sample size

    Calculate sample size from estimation error
    """
    #math.ceil in python2.7 returns a float, while it returns an int in python3.
    return int(math.ceil(4.0 / math.pow(est_err, 2.0)))

  @typehints.with_input_types(T)
  @typehints.with_output_types(int)
  class Globally(PTransform):
    """ Approximate.Globally approximate number of unique values"""

    def __init__(self, size=None, error=None):
      self._sample_size = ApproximateUnique.parse_input_params(size, error)

    def expand(self, pcoll):
      coder = coders.registry.get_coder(pcoll)
      return pcoll \
             | 'CountGlobalUniqueValues' \
             >> (CombineGlobally(ApproximateUniqueCombineFn(self._sample_size,
                                                            coder)))

  @typehints.with_input_types(typing.Tuple[K, V])
  @typehints.with_output_types(typing.Tuple[K, int])
  class PerKey(PTransform):
    """ Approximate.PerKey approximate number of unique values per key"""

    def __init__(self, size=None, error=None):
      self._sample_size = ApproximateUnique.parse_input_params(size, error)

    def expand(self, pcoll):
      coder = coders.registry.get_coder(pcoll)
      return pcoll \
             | 'CountPerKeyUniqueValues' \
             >> (CombinePerKey(ApproximateUniqueCombineFn(self._sample_size,
                                                          coder)))


class _LargestUnique(object):
  """
  An object to keep samples and calculate sample hash space. It is an
  accumulator of a combine function.
  """
  _HASH_SPACE_SIZE = 2.0 * sys.maxsize

  def __init__(self, sample_size):
    self._sample_size = sample_size
    self._min_hash = sys.maxsize
    self._sample_heap = []
    self._sample_set = set()

  def add(self, element):
    """
    :param an element from pcoll.
    :return: boolean type whether the value is in the heap

    Adds a value to the heap, returning whether the value is (large enough to
    be) in the heap.
    """
    if len(self._sample_heap) >= self._sample_size and element < self._min_hash:
      return False

    if element not in self._sample_set:
      self._sample_set.add(element)
      heapq.heappush(self._sample_heap, element)

      if len(self._sample_heap) > self._sample_size:
        temp = heapq.heappop(self._sample_heap)
        self._sample_set.remove(temp)
        self._min_hash = self._sample_heap[0]
      elif element < self._min_hash:
        self._min_hash = element

    return True

  def get_estimate(self):
    """
    :return: estimation count of unique values

    If heap size is smaller than sample size, just return heap size.
    Otherwise, takes into account the possibility of hash collisions,
    which become more likely than not for 2^32 distinct elements.
    Note that log(1+x) ~ x for small x, so for sampleSize << maxHash
    log(1 - sample_size/sample_space) / log(1 - 1/sample_space) ~ sample_size
    and hence estimate ~ sample_size * hash_space / sample_space
    as one would expect.

    Given sample_size / sample_space = est / hash_space
    est = sample_size * hash_space / sample_space

    Given above sample_size approximate,
    est = log1p(-sample_size/sample_space) / log1p(-1/sample_space)
      * hash_space / sample_space
    """

    if len(self._sample_heap) < self._sample_size:
      return len(self._sample_heap)
    else:
      sample_space_size = sys.maxsize - 1.0 * self._min_hash
      est = (math.log1p(-self._sample_size / sample_space_size)
             / math.log1p(-1 / sample_space_size)
             * self._HASH_SPACE_SIZE
             / sample_space_size)

      return round(est)


class ApproximateUniqueCombineFn(CombineFn):
  """
  ApproximateUniqueCombineFn computes an estimate of the number of
  unique values that were combined.
  """

  def __init__(self, sample_size, coder):
    self._sample_size = sample_size
    self._coder = coder

  def create_accumulator(self, *args, **kwargs):
    return _LargestUnique(self._sample_size)

  def add_input(self, accumulator, element, *args, **kwargs):
    try:
      accumulator.add(hash(self._coder.encode(element)))
      return accumulator
    except Exception as e:
      raise RuntimeError("Runtime exception: %s", e)

  # created an issue https://issues.apache.org/jira/browse/BEAM-7285 to speep up
  # merge process.
  def merge_accumulators(self, accumulators, *args, **kwargs):
    merged_accumulator = self.create_accumulator()
    for accumulator in accumulators:
      for i in accumulator._sample_heap:
        merged_accumulator.add(i)

    return merged_accumulator

  @staticmethod
  def extract_output(accumulator):
    return accumulator.get_estimate()

  def display_data(self):
    return {'sample_size': self._sample_size}


class ApproximateQuantiles(object):
  """
  PTransfrom for getting the idea of data distribution using approximate N-tile
  (e.g. quartiles, percentiles etc.) either globally or per-key.
  """

  @staticmethod
  def _display_data(num_quantiles, compare, key, reverse):
    return {
        'num_quantiles': DisplayDataItem(num_quantiles, label="Quantile Count"),
        'compare': DisplayDataItem(compare.__class__,
                                   label='Record Comparer FN'),
        'key': DisplayDataItem(key.__class__, label='Record Comparer Key'),
        'reverse': DisplayDataItem(reverse.__class__, label='Is reversed')
    }

  @typehints.with_input_types(T)
  @typehints.with_output_types(typing.List[T])
  class Globally(PTransform):
    """
    PTransform takes PCollection and returns a list whose single value is
    approximate N-tiles of the input collection globally.

    Args:
      num_quantiles: number of elements in the resulting quantiles values list.
      compare: (optional) Comparator function which is an implementation
        of "a < b" taking at least two arguments (a and b). Which is later
        converted to key function as Python 3 does not support cmp.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
    """

    def __init__(self, num_quantiles, compare=None, key=None, reverse=False):
      self.num_quantiles = num_quantiles
      self.compare = compare
      self._key = key
      self._reverse = reverse

    def expand(self, pcoll):
      return pcoll | CombineGlobally(ApproximateQuantilesCombineFn.create(
          num_quantiles=self.num_quantiles, compare=self.compare,
          key=self._key, reverse=self._reverse))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self.num_quantiles, compare=self.compare, key=self._key,
          reverse=self._reverse)

  @typehints.with_input_types(typing.Tuple[K, V])
  @typehints.with_output_types(typing.List[typing.Tuple[K, V]])
  class PerKey(PTransform):
    """
    PTransform takes PCollection of KV and returns a list based on each key
    whose single value is list of approximate N-tiles of the input element of
    the key.

    Args:
      num_quantiles: number of elements in the resulting quantiles values list.
      compare: (optional) Comparator function which is an implementation
        of "a < b" taking at least two arguments (a and b). Which is later
        converted to key function as Python 3 does not support cmp.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
    """

    def __init__(self, num_quantiles, compare=None, key=None, reverse=False):
      self.num_quantiles = num_quantiles
      self.compare = compare
      self._key = key
      self._reverse = reverse

    def expand(self, pcoll):
      return pcoll | CombinePerKey(ApproximateQuantilesCombineFn.create(
          num_quantiles=self.num_quantiles, compare=self.compare,
          key=self._key, reverse=self._reverse))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self.num_quantiles, compare=self.compare, key=self._key,
          reverse=self._reverse)


class _QuantileBuffer(object):
  """A single buffer in the sense of the referenced algorithm."""

  def __init__(self, elements, level=0, weight=1):
    self.elements = elements
    self.level = level
    self.weight = weight

  def __lt__(self, other):
    self.elements < other.elements

  def sized_iterator(self):

    class QuantileBufferIterator(object):
      def __init__(self, elem, weight):
        self._iter = iter(elem)
        self.weight = weight

      def __iter__(self):
        return self

      def __next__(self):
        value = next(self._iter)
        return {'value': value, 'weight': self.weight}

      next = __next__  # For Python 2

    return QuantileBufferIterator(self.elements, self.weight)


class _QuantileState(object):
  """
  Compact summarization of a collection on which quantiles can be estimated.
  """
  min_val = None  # Holds smallest item in the list
  max_val = None  # Holds largest item in the list

  def __init__(self, buffer_size, num_buffers, unbuffered_elements, buffers):
    self.buffer_size = buffer_size
    self.num_buffers = num_buffers
    self.buffers = buffers

    # The algorithm requires that the manipulated buffers always be filled to
    # capacity to perform the collapse operation. This operation can be extended
    # to buffers of varying sizes by introducing the notion of fractional
    # weights, but it's easier to simply combine the remainders from all shards
    # into new, full buffers and then take them into account when computing the
    # final output.
    self.unbuffered_elements = unbuffered_elements

  def is_empty(self):
    """Check if the buffered & unbuffered elements are empty or not."""
    return not self.unbuffered_elements and not self.buffers


class ApproximateQuantilesCombineFn(CombineFn):
  """
  This combiner gives an idea of the distribution of a collection of values
  using approximate N-tiles. The output of this combiner is the list of size of
  the number of quantiles (num_quantiles), containing the input values of the
  minimum value item of the list, the intermediate values (n-tiles) and the
  maximum value item of the list, in the sort order provided via comparator
  (compare or key).

  If there are fewer values to combine than the number of quantile
  (num_quantiles), then the resulting list will contain all the values being
  combined, in sorted order.

  If no comparator (compare or key) is provided, then the results are sorted
  in the natural order.

  To evaluate the quantiles, we use the "New Algorithm" described here:

  [MRL98] Manku, Rajagopalan & Lindsay, "Approximate Medians and other
  Quantiles in One Pass and with Limited Memory", Proc. 1998 ACM SIGMOD,
  Vol 27, No 2, p 426-435, June 1998.
  http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&rep=rep1
  &type=pdf

  The default error bound is (1 / N), though in practice the accuracy
  tends to be much better.

  Args:
    num_quantiles: Number of quantiles to produce. It is the size of the final
      output list, including the mininum and maximum value items.
    buffer_size: The size of the buffers, corresponding to k in the referenced
      paper.
    num_buffers: The number of buffers, corresponding to b in the referenced
      paper.
    epsilon: (optional) The default error bound is `epsilon`, which holds as
      long as the number of elements is less than `MAX_NUM_ELEMENTS`.
      Specifically, if one considers the input as a sorted list x_1, ..., x_N,
      then the distance between each exact quantile x_c and its
      approximation x_c' is bounded by `|c - c'| < epsilon * N`. Note that
      these errors are worst-case scenarios. In practice the accuracy tends to
      be much better.
    compare: (optional) Comparator function which is an implementation
        of "a < b" taking two arguments (a and b). Which is later
        converted to key function as Python 3 does not support `cmp`.
    key: (optional) Key is a mapping of elements to a comparable key, similar
      to the key argument of Python's sorting methods.
  """

  # For alternating between biasing up and down in the above even weight
  # collapse operation.
  offset_jitter = 0

  # The cost (in time and space) to compute quantiles to a given accuracy is a
  # function of the total number of elements in the data set. If an estimate is
  # not known or specified, we use this as an upper bound. If this is too low,
  # errors may exceed the requested tolerance; if too high, efficiency may be
  # non-optimal. The impact is logarithmic with respect to this value, so this
  # default should be fine for most uses.
  MAX_NUM_ELEMENTS = 1e9
  qs = None  # Refers to the _QuantileState

  # TODO(mszb): For Python 3, remove compare and only keep key.
  def __init__(self, num_quantiles, buffer_size, num_buffers, compare=None,
               key=None, reverse=False):
    if compare:
      warnings.warn('Compare is not available in Python 3, use key instead.')

    if key:
      self._comparator = lambda a, b: (key(a) < key(b)) - (key(a) > key(b)) \
        if reverse else (key(a) > key(b)) - (key(a) < key(b))
    else:
      if not compare:
        self._comparator = lambda a, b: (a < b) - (a > b) if reverse \
          else (a > b) - (a < b)
      else:
        self._comparator = lambda a, b: compare(b, a) if reverse \
          else compare(a, b)
      key = lambda elem: cmp_to_key(self._comparator)(elem)

    self.compare = compare
    self.num_quantiles = num_quantiles
    self.buffer_size = buffer_size
    self.num_buffers = num_buffers
    self._key = key

  @classmethod
  def create(cls, num_quantiles, epsilon=None, max_num_elements=None,
             compare=None, key=None, reverse=False):
    max_num_elements = max_num_elements or cls.MAX_NUM_ELEMENTS
    if not epsilon:
      epsilon = 1.0 / num_quantiles
    b = 2
    while (b - 2) * (1 << (b - 2)) < epsilon * max_num_elements:
      b = b + 1
    b = b - 1
    k = max(2, math.ceil(max_num_elements / float(1 << (b - 1))))
    return cls(num_quantiles=num_quantiles, compare=compare, buffer_size=k,
               num_buffers=b, key=key, reverse=reverse)

  def add_unbuffered(self, elem):
    """
    Add a new buffer to the unbuffered list, creating a new buffer and
    collapsing if needed.
    """
    self.qs.unbuffered_elements.append(elem)
    if len(self.qs.unbuffered_elements) == self.qs.buffer_size:
      self.qs.unbuffered_elements.sort(key=self._key)
      heapq.heappush(self.qs.buffers,
                     _QuantileBuffer(elements=self.qs.unbuffered_elements))
      self.qs.unbuffered_elements = []
      self.collapse_if_needed()

  def offset(self, newWeight):
    """
    If the weight is even, we must round up or down. Alternate between these
    two options to avoid a bias.
    """
    if newWeight % 2 == 1:
      return (newWeight + 1) / 2
    else:
      self.offset_jitter = 2 - self.offset_jitter
      return (newWeight + self.offset_jitter) / 2

  def collapse(self, buffers):
    new_level = 0
    new_weight = 0
    for buffer_elem in buffers:
      # As presented in the paper, there should always be at least two
      # buffers of the same (minimal) level to collapse, but it is possible
      # to violate this condition when combining buffers from independently
      # computed shards.  If they differ we take the max.
      new_level = max([new_level, buffer_elem.level + 1])
      new_weight = new_weight + buffer_elem.weight
    new_elements = self.interpolate(buffers, self.buffer_size, new_weight,
                                    self.offset(new_weight))
    return _QuantileBuffer(new_elements, new_level, new_weight)

  def collapse_if_needed(self):
    while len(self.qs.buffers) > self.num_buffers:
      toCollapse = []
      toCollapse.append(heapq.heappop(self.qs.buffers))
      toCollapse.append(heapq.heappop(self.qs.buffers))
      minLevel = toCollapse[1].level

      while len(self.qs.buffers) > 0 and self.qs.buffers[0].level == minLevel:
        toCollapse.append(heapq.heappop(self.qs.buffers))

      heapq.heappush(self.qs.buffers, self.collapse(toCollapse))

  def interpolate(self, i_buffers, count, step, offset):
    """
    Emulates taking the ordered union of all elements in buffers, repeated
    according to their weight, and picking out the (k * step + offset)-th
    elements of this list for `0 <= k < count`.
    """

    iterators = []
    new_elements = []
    compare_key = lambda x: self._key(x['value'])
    for buffer_elem in i_buffers:
      iterators.append(buffer_elem.sized_iterator())

    # Python 3 `heapq.merge` support key comparison and returns an iterator and
    # does not pull the data into memory all at once. Python 2 does not
    # support comparison on its `heapq.merge` api, so we use the itertools
    # which takes the `key` function for comparison and creates an iterator
    # from it.
    if sys.version_info[0] < 3:
      sorted_elem = iter(
          sorted(itertools.chain.from_iterable(iterators), key=compare_key))
    else:
      sorted_elem = heapq.merge(*iterators, key=compare_key)

    weighted_element = next(sorted_elem)
    current = weighted_element['weight']
    j = 0
    while j < count:
      target = j * step + offset
      j = j + 1
      try:
        while current <= target:
          weighted_element = next(sorted_elem)
          current = current + weighted_element['weight']
      except StopIteration:
        pass
      new_elements.append(weighted_element['value'])
    return new_elements

  def create_accumulator(self):
    self.qs = _QuantileState(buffer_size=self.buffer_size,
                             num_buffers=self.num_buffers,
                             unbuffered_elements=[], buffers=[])
    return self.qs

  def add_input(self, quantile_state, element):
    """
    Add a new element to the collection being summarized by quantile state.
    """
    if quantile_state.is_empty():
      quantile_state.min_val = quantile_state.max_val = element
    elif self._comparator(element, quantile_state.min_val) < 0:
      quantile_state.min_val = element
    elif self._comparator(element, quantile_state.max_val) > 0:
      quantile_state.max_val = element
    self.add_unbuffered(elem=element)
    return self.qs

  def merge_accumulators(self, accumulators):
    """Merges all the accumulators (quantile state) as one."""
    self.qs = self.create_accumulator()
    for accumulator in accumulators:
      if accumulator.is_empty():
        continue
      if not self.qs.min_val or self._comparator(accumulator.min_val,
                                                 self.qs.min_val) < 0:
        self.qs.min_val = accumulator.min_val
      if not self.qs.max_val or self._comparator(accumulator.max_val,
                                                 self.qs.max_val) > 0:
        self.qs.max_val = accumulator.max_val

      for unbuffered_element in accumulator.unbuffered_elements:
        self.add_unbuffered(unbuffered_element)

      self.qs.buffers.extend(accumulator.buffers)
    self.collapse_if_needed()
    return self.qs

  def extract_output(self, accumulator):
    """
    Outputs num_quantiles elements consisting of the minimum, maximum and
    num_quantiles - 2 evenly spaced intermediate elements. Returns the empty
    list if no elements have been added.
    """
    if accumulator.is_empty():
      return []

    all_elems = accumulator.buffers
    total_count = len(accumulator.unbuffered_elements)
    for buffer_elem in all_elems:
      total_count = total_count + accumulator.buffer_size * buffer_elem.weight

    if accumulator.unbuffered_elements:
      accumulator.unbuffered_elements.sort(key=self._key)
      all_elems.append(_QuantileBuffer(accumulator.unbuffered_elements))

    step = 1.0 * total_count / (self.num_quantiles - 1)
    offset = (1.0 * total_count - 1) / (self.num_quantiles - 1)

    quantiles = [accumulator.min_val]
    quantiles.extend(
        self.interpolate(all_elems, self.num_quantiles - 2, step, offset))
    quantiles.append(accumulator.max_val)
    return quantiles
