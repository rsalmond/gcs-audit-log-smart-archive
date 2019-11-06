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
Google Cloud Storage smart archiver main entry point.
"""
import logging
import warnings
import sys
from atexit import register
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from gcs_sa.actions import rewrite_object
from gcs_sa.args import get_args
from gcs_sa.bq.output import BigQueryOutput
from gcs_sa.bq.queries import compose_access_query, run_query_job
from gcs_sa.bq.tables import Table, TableDefinitions, get_table
from gcs_sa.config import config_to_string, get_config
from gcs_sa.decisions import should_cool_down, should_warm_up
from gcs_sa.logging import set_program_log_level

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger(__name__)

def main() -> None:
    """
    Main entry point for the program.
    """
    args = get_args()
    print("Arguments parsed: {}".format(args))
    config = get_config(args)
    print("Configuration parsed: \n{}".format(config_to_string(config)))
    set_program_log_level(args, config)
    return evaluate_objects()


def evaluate_objects() -> None:
    """
    Evaluate objects in the audit log to see if they should be moved to a
    new storage class.
    """
    config = get_config()
    cold_storage_class = config.get('RULES', 'COLD_STORAGE_CLASS')
    moved_output = BigQueryOutput(get_table(
        TableDefinitions.OBJECTS_MOVED))
    excluded_output = BigQueryOutput(get_table(
        TableDefinitions.OBJECTS_EXCLUDED))
    work_queue = Queue(maxsize=3000)

    # Create temp table object. Doesn't need to be initialized.
    temp_table = Table("smart_archiver_temp")

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

    rows_read = 0
    # Run query job
    job = run_query_job(compose_access_query(),
                        temp_table.get_fully_qualified_name())

    # evaluate, archive and record
    def archive_worker(name: str) -> None:
        LOG.debug("Worker %s started.", name)
        while True:
            row = work_queue.get()
            if not row:
                break
            if should_warm_up(row):
                rewrite_object(row, 'STANDARD', moved_output,
                               excluded_output)
            elif should_cool_down(row):
                rewrite_object(row, cold_storage_class,
                               moved_output, excluded_output)
        LOG.debug("Worker %s finished.", name)


    workers = config.getint('RUNTIME', 'WORKERS')
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Start all worker threads
        for worker_name in range(workers):
            executor.submit(archive_worker, worker_name)
        # Enqueue all work
        for row in job.result():
            rows_read += 1
            work_queue.put(row)
        # Signal completion to all workers
        for _ in range(workers):
            work_queue.put(None)


if __name__ == "__main__":
    sys.exit(main())
