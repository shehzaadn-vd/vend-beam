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

"""
This is experimental module for reading from Google Cloud Spanner.
https://cloud.google.com/spanner

To read from Cloud Spanner, apply ReadFromSpanner transformation.
It will return a PCollection of list, where each element represents an
individual row returned from the read operation.
Both Query and Read APIs are supported. See more information
about "https://cloud.google.com/spanner/docs/reads".

To execute a "query", specify a "ReadFromSpanner.with_query(QUERY_STRING)"
during the construction of the transform. For example:

    records = (pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME)
                  .with_query('SELECT * FROM users'))

To use the read API, specify a "ReadFromSpanner.with_table(TABLE_NAME, COLUMNS)"
during the construction of the transform. For example:

    records = (pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME)
                  .with_table("users", ["id", "name", "email"]))

"ReadFromSpanner.with_table" also support indexes by specifying the "index"
parameter. For more information, the spanner read with index documentation:
https://cloud.google.com/spanner/docs/secondary-indexes#read-with-index


It is possible to read several PCollection of ReadOperation within a single
transaction. Apply ReadFromSpanner.create_transaction() transform, that lazily
creates a transaction. The result of this transformation can be passed to
read operation using ReadFromSpanner.with_transaction(). For Example:

    transaction = ReadFromSpanner.create_transaction(
        project_id=PROJECT_ID,
        instance_id=sINSTANCE_ID,
        database_id=DB_NAME,
        exact_staleness=datetime.timedelta(seconds=100))

    spanner_read = ReadFromSpanner(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        database_id=DB_NAME)

    users = (pipeline
            | 'Get all users' >> spanner_read.with_transaction(transaction)
               .with_query("SELECT * FROM users"))
    tweets = (pipeline
            | 'Get all tweets' >> spanner_read.with_transaction(transaction)
             .with_query("SELECT * FROM tweets"))
"""
from __future__ import absolute_import

import collections

from google.cloud.spanner import Client
from google.cloud.spanner import KeySet
from google.cloud.spanner_v1.database import BatchSnapshot

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.utils.annotations import experimental

__all__ = ['ReadFromSpanner', 'ReadOperation', 'create_transaction', ]


class ReadOperation(collections.namedtuple("ReadOperation",
                                           ["read_operation", "batch_action",
                                            "transaction_action", "kwargs"])):
  """
  Encapsulates a spanner read operation.
  """

  __slots__ = ()

  @classmethod
  def query(cls, sql, params=None, param_types=None):
    return cls(
        read_operation="process_query_batch",
        batch_action="generate_query_batches", transaction_action="execute_sql",
        kwargs={'sql': sql, 'params': params, 'param_types': param_types}
    )

  @classmethod
  def table(cls, table, columns, index="", keyset=None):
    keyset = keyset or KeySet(all_=True)
    if not isinstance(keyset, KeySet):
      raise ValueError("keyset must be an instance of class "
                       "google.cloud.spanner_v1.keyset.KeySet")
    return cls(
        read_operation="process_read_batch",
        batch_action="generate_read_batches", transaction_action="read",
        kwargs={'table': table, 'columns': columns, 'index': index,
                'keyset': keyset}
    )


class _BeamSpannerConfiguration(collections.namedtuple(
    "_BeamSpannerConfiguration", ["project", "instance", "database",
                                  "credentials", "user_agent", "pool",
                                  "snapshot_read_timestamp",
                                  "snapshot_exact_staleness"])):

  @property
  def snapshot_options(self):
    snapshot_options = {}
    if self.snapshot_exact_staleness:
      snapshot_options['exact_staleness'] = self.snapshot_exact_staleness
    if self.snapshot_read_timestamp:
      snapshot_options['read_timestamp'] = self.snapshot_read_timestamp
    return snapshot_options


class _NaiveSpannerReadDoFn(beam.DoFn):

  def __init__(self, spanner_configuration):
    self._spanner_configuration = spanner_configuration
    self._snapshot = None

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)

  def process(self, element, transaction_info):
    self._snapshot = BatchSnapshot.from_dict(self._database, transaction_info)
    with self._snapshot._get_session().transaction() as transaction:
      if element.transaction_action == 'execute_sql':
        transaction_read = transaction.execute_sql
      elif element.transaction_action == 'read':
        transaction_read = transaction.read
      else:
        raise ValueError("Not implemented ")

      for row in transaction_read(**element.kwargs):
        yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


class _CreateReadPartitions(beam.DoFn):

  def __init__(self, snapshot_dict, spanner_configuration):
    self._snapshot_dict = snapshot_dict
    self._spanner_configuration = spanner_configuration

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(project=self._spanner_configuration.project,
                            credentials=self._spanner_configuration.credentials,
                            user_agent=self._spanner_configuration.user_agent)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(**self._spanner_configuration
                                                   .snapshot_options)
    self._snapshot_dict = self._snapshot.to_dict()

  def process(self, element):
    if element.batch_action == 'generate_query_batches':
      partitioning_action = self._snapshot.generate_query_batches
    elif element.batch_action == 'generate_read_batches':
      partitioning_action = self._snapshot.generate_read_batches

    for p in partitioning_action(**element.kwargs):
      yield {"read_operation": element.read_operation, "partitions": p,
             "transaction_info": self._snapshot_dict}

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


class _CreateTransactionFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, credentials,
               user_agent, pool, read_timestamp,
               exact_staleness):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._credentials = credentials
    self._user_agent = user_agent
    self._pool = pool

    self._snapshot_options = {}
    if read_timestamp:
      self._snapshot_options['read_timestamp'] = read_timestamp
    if exact_staleness:
      self._snapshot_options['exact_staleness'] = exact_staleness
    self._snapshot = None

  def setup(self):
    self._spanner_client = Client(project=self._project_id,
                                  credentials=self._credentials,
                                  user_agent=self._user_agent)
    self._instance = self._spanner_client.instance(self._instance_id)
    self._database = self._instance.database(self._database_id, pool=self._pool)

  def process(self, element, *args, **kwargs):
    self._snapshot = self._database.batch_snapshot(**self._snapshot_options)
    return [self._snapshot.to_dict()]

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


@beam.transforms.ptransform_fn
def create_transaction(pcoll, project_id, instance_id, database_id,
                       credentials=None,
                       user_agent=None, pool=None, read_timestamp=None,
                       exact_staleness=None):
  return (pcoll | beam.Create([1]) | beam.ParDo(_CreateTransactionFn(
      project_id, instance_id, database_id, credentials,
      user_agent, pool, read_timestamp,
      exact_staleness)))


class _ReadFromPartitionFn(beam.DoFn):

  def __init__(self, snapshot_dict, spanner_configuration):
    self._snapshot_dict = snapshot_dict
    self._spanner_configuration = spanner_configuration

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(**self._spanner_configuration
                                                   .snapshot_options)

  def process(self, element):
    self._snapshot = BatchSnapshot.from_dict(
        self._database,
        element['transaction_info']
    )

    if element['read_operation'] == 'process_query_batch':
      read_action = self._snapshot.process_query_batch
    elif element['read_operation'] == 'process_read_batch':
      read_action = self._snapshot.process_read_batch
    else:
      raise ValueError("Unknown read action.")

    for row in read_action(element['partitions']):
      yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


@experimental()
class ReadFromSpanner(PTransform):

  def __init__(self, project_id, instance_id, database_id, pool=None,
               read_timestamp=None, exact_staleness=None, credentials=None,
               user_agent=None,
               sql=None, params=None, param_types=None,  # with_query
               table=None, columns=None, index="", keyset=None,  # with_table
               read_operations=None,  # for read all
               transaction=None
               ):
    self._configuration = _BeamSpannerConfiguration(
        project=project_id, instance=instance_id, database=database_id,
        credentials=credentials, user_agent=user_agent, pool=pool,
        snapshot_read_timestamp=read_timestamp,
        snapshot_exact_staleness=exact_staleness
    )

    self._read_operations = read_operations
    self._transaction = transaction

    if self._read_operations is None:
      if table is not None:
        if columns is None:
          raise ValueError("Columns are required with the table name.")
        self._read_operations = [ReadOperation.table(
            table=table, columns=columns, index=index, keyset=keyset)]
      elif sql is not None:
        self._read_operations = [ReadOperation.query(
            sql=sql, params=params, param_types=param_types)]

  def expand(self, pbegin):
    if self._read_operations is not None and isinstance(pbegin,
                                                        beam.pvalue.PBegin):
      pcoll = pbegin.pipeline | beam.Create(self._read_operations)
    elif not isinstance(pbegin, beam.pvalue.PBegin):
      if self._read_operations is not None:
        raise ValueError("Read operation in the constructor only works with "
                         "the root of the pipeline.")
      pcoll = pbegin
    else:
      raise ValueError("Spanner required read operation, sql or table "
                       "with columns.")

    if self._transaction is None:
      p = (pcoll
           | 'Generate Partitions' >> beam.ParDo(
              _CreateReadPartitions(
                  snapshot_dict=None,
                  spanner_configuration=self._configuration
              )
          ).with_input_types(ReadOperation)
           | 'Reshuffle' >> beam.Reshuffle()
           | 'Read From Partitions' >> beam.ParDo(
              _ReadFromPartitionFn(
                  snapshot_dict=None,
                  spanner_configuration=self._configuration))
           )
    else:
      p = (pcoll
           | 'Reshuffle' >> beam.Reshuffle().with_input_types(ReadOperation)
           | 'Perform Read' >> beam.ParDo(
              _NaiveSpannerReadDoFn(spanner_configuration=self._configuration),
              beam.pvalue.AsSingleton(self._transaction))
           )
    return p
