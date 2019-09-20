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

from __future__ import absolute_import

import datetime
import logging
import random
import string
import unittest

import mock

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where spanner library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import spanner
  from apache_beam.io.gcp.spannerio import ReadOperation # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import ReadFromSpanner # pylint: disable=unused-import
except ImportError:
  spanner = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


MAX_DB_NAME_LENGTH = 30
TEST_PROJECT_ID = 'apache-beam-testing'
TEST_INSTANCE_ID = 'beam-test'
TEST_DATABASE_PREFIX = 'spanner-testdb-'
# TEST_TABLE = 'users'
# TEST_COLUMNS = ['Key', 'Value']
FAKE_ROWS = [[1, 'Alice'], [2, 'Bob'], [3, 'Carl'], [4, 'Dan'], [5, 'Evan'],
             [6, 'Floyd']]


def _generate_database_name():
  mask = string.ascii_lowercase + string.digits
  length = MAX_DB_NAME_LENGTH - 1 - len(TEST_DATABASE_PREFIX)
  return TEST_DATABASE_PREFIX + ''.join(random.choice(mask) for i in range(
      length))


def _generate_test_data():
  mask = string.ascii_lowercase + string.digits
  length = 100
  return [('users', ['Key', 'Value'], [(x, ''.join(
      random.choice(mask) for _ in range(length))) for x in range(1, 5)])]


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
class SpannerReadTest(unittest.TestCase):

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  @mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
  def test_read_with_query_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot = mock.MagicMock()

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.batch_snapshot.return_value = mock_snapshot
    mock_batch_snapshot_class.return_value = mock_snapshot
    mock_batch_snapshot_class.from_dict.return_value = mock_snapshot

    mock_snapshot.to_dict.return_value = dict()
    mock_snapshot.generate_query_batches.return_value = [
        {'query': {'sql': 'SELECT * FROM users'},
         'partition': 'test_partition'} for _ in range(3)]
    mock_snapshot.process_query_batch.side_effect = [
        FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    pipeline = TestPipeline()
    records = (pipeline
               | ReadFromSpanner(
                   project_id=TEST_PROJECT_ID,
                   instance_id=TEST_INSTANCE_ID,
                   database_id=_generate_database_name())
               .with_query('SELECT * FROM users'))
    pipeline.run()
    assert_that(records, equal_to(FAKE_ROWS))

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  @mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
  def test_read_with_table_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot = mock.MagicMock()

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.batch_snapshot.return_value = mock_snapshot
    mock_batch_snapshot_class.return_value = mock_snapshot
    mock_batch_snapshot_class.from_dict.return_value = mock_snapshot

    mock_snapshot.to_dict.return_value = dict()
    mock_snapshot.generate_read_batches.return_value = [{
        'read': {'table': 'users', 'keyset': {'all': True},
                 'columns': ['Key', 'Value'], 'index': ''},
        'partition': 'test_partition'} for _ in range(3)]
    mock_snapshot.process_read_batch.side_effect = [
        FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    pipeline = TestPipeline()
    records = (pipeline | ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name())
               .with_table('users', ['Key', 'Value']))
    pipeline.run()
    assert_that(records, equal_to(FAKE_ROWS))

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  @mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
  def test_read_with_query_transaction(self, mock_batch_snapshot_class,
                                       mock_client_class):

    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot = mock.MagicMock()

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.batch_snapshot.return_value = mock_snapshot
    mock_batch_snapshot_class.return_value = mock_snapshot
    mock_batch_snapshot_class.from_dict.return_value = mock_snapshot
    mock_snapshot.to_dict.return_value = dict()

    mock_session = mock.MagicMock()
    mock_transaction_ctx = mock.MagicMock()
    mock_transaction = mock.MagicMock()

    mock_snapshot._get_session.return_value = mock_session
    mock_session.transaction.return_value = mock_transaction
    mock_transaction.__enter__.return_value = mock_transaction_ctx
    mock_transaction_ctx.execute_sql.return_value = FAKE_ROWS

    transaction = ReadFromSpanner.create_transaction(
        project_id=TEST_PROJECT_ID, instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        exact_staleness=datetime.timedelta(seconds=10))

    pipeline = TestPipeline()

    records = (pipeline
               | ReadFromSpanner(project_id=TEST_PROJECT_ID,
                                 instance_id=TEST_INSTANCE_ID,
                                 database_id=_generate_database_name())
               .with_transaction(transaction)
               .with_query('SELECT * FROM users'))
    pipeline.run()
    assert_that(records, equal_to(FAKE_ROWS))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
