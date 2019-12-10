# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Implementation of "cooldown" command.
"""
import logging
from atexit import register

from google.cloud.bigquery import Row

from gcs_sa.actions import rewrite_object
from gcs_sa.bq.output import BigQueryOutput
from gcs_sa.bq.queries import compose_cooldown_query, run_query_job
from gcs_sa.bq.tables import Table, TableDefinitions, get_table
from gcs_sa.config import get_config
from gcs_sa.decisions import should_cool_down
from gcs_sa.thread import BoundedThreadPoolExecutor

LOG = logging.getLogger(__name__)


def cooldown_command() -> None:
    """
    Evaluate objects in the audit log to see if they should be moved to a
    colder storage class.
    """
    config = get_config()
    cold_storage_class = config.get('RULES', 'COLD_STORAGE_CLASS')
    moved_output = BigQueryOutput(get_table(TableDefinitions.OBJECTS_MOVED))
    excluded_output = BigQueryOutput(
        get_table(TableDefinitions.OBJECTS_EXCLUDED))
    rows_read = 0

    # Create temp table object. Doesn't need to be initialized, as the
    # query job will do that.
    temp_table = Table(
        config.get('BIGQUERY', 'TEMP_TABLE', fallback='smart_archiver_temp_cooldown'))

    # Register cleanup as shutdown hook
    def cleanup():
        # Flush any remaining output
        moved_output.flush()
        excluded_output.flush()
        # Delete temp table
        temp_table.drop()
        # Print statistics
        LOG.info("%s rows read.", rows_read)
        LOG.info(moved_output.stats())
        LOG.info(excluded_output.stats())

    register(cleanup)

    # Run query job
    job = run_query_job(compose_cooldown_query(),
                        temp_table.get_fully_qualified_name())

    # evaluate, archive and record
    def archive_worker(row: Row) -> None:
        if should_cool_down(row):
            rewrite_object(row, cold_storage_class, moved_output,
                           excluded_output)

    workers = config.getint('RUNTIME', 'WORKERS')
    with BoundedThreadPoolExecutor(max_workers=workers) as executor:
        # get total rows in result, report it
        result = job.result()
        total_rows = result.total_rows
        percentage_reported = 0
        LOG.info("Total rows: %s", total_rows)
        # Start all worker threads
        for row in result:
            rows_read += 1
            executor.submit(archive_worker, row)
            # calculate the percentage and show it if it's a new 10%ile
            percentage = int(rows_read / total_rows * 100)
            if percentage > percentage_reported and not percentage % 10:
                LOG.info("%s%% complete.", percentage)
                percentage_reported = percentage
