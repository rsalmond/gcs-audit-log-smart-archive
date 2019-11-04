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
Definitions of BigQuery tables used by this program.
"""
import logging

from enum import Enum

from google.cloud import bigquery

from gcs_sa.bq.client import get_bq_client
from gcs_sa.config import get_config

LOG = logging.getLogger(__name__)


class Table:
    """
    BigQuery table information and common methods.
    """

    def __init__(self, name: str, schema: str = None):
        self.short_name = name
        self.schema = schema

    def drop(self) -> bigquery.table.RowIterator:
        """DROPs (deletes) the table. This cannot be undone.

        Returns:
            google.cloud.bigquery.table.RowIterator -- Result of the query.
            Since this is a DDL query, this will always be empty if
            it succeeded.

        Raises:
            google.cloud.exceptions.GoogleCloudError –- If the job failed.
            concurrent.futures.TimeoutError –- If the job did not complete
            in the default BigQuery job timeout.
        """
        bq_client = get_bq_client()

        LOG.info("Deleting table %s", self.get_fully_qualified_name())

        querytext = "DROP TABLE `{}`".format(self.get_fully_qualified_name())

        LOG.debug("Running query: \n%s", querytext)

        query_job = bq_client.query(querytext)
        return query_job.result()

    def initialize(self) -> bigquery.table.RowIterator:
        """Creates, if not found, a table.

        Returns:
            google.cloud.bigquery.table.RowIterator -- Result of the query.
            Since this is a DDL query, this will always be empty if
            it succeeded.

        Raises:
            google.cloud.exceptions.GoogleCloudError –- If the job failed.
            concurrent.futures.TimeoutError –- If the job did not complete
            in the default BigQuery job timeout.
        """
        bq_client = get_bq_client()

        LOG.info("Creating table %s if not found.",
                 self.get_fully_qualified_name())

        querytext = """
            CREATE TABLE IF NOT EXISTS `{}` (
            {}
            )""".format(self.get_fully_qualified_name(), self.schema)

        LOG.debug("Running query: \n%s", querytext)

        query_job = bq_client.query(querytext)
        return query_job.result()

    def get_fully_qualified_name(self) -> str:
        """Return a table name with project and dataset names prefixed.

        Arguments:
            name {str} -- Short name of the table.

        Returns:
            str -- Fully qualified name of the table.
        """
        config = get_config()
        return "{}.{}.{}".format(
            config.get("BIGQUERY",
                       "JOB_PROJECT",
                       fallback=config.get("GCP", "PROJECT")),
            config.get("BIGQUERY", "DATASET_NAME"), self.short_name)


class TableDefinitions(Enum):
    """
    Definitions of known tables.

    Where tables have schema = None, they are presumed to be read-only.
    """
    OBJECTS_MOVED = {
        "name":
            "objects_moved",
        "schema":
            """
                resourceName STRING,
                storageClass STRING,
                size INT64,
                blobCreatedTimeWhenMoved TIMESTAMP,
                moveTimestamp TIMESTAMP
            """
    }
    OBJECTS_EXCLUDED = {
        "name": "objects_excluded",
        "schema": "resourceName STRING"
    }
    DATA_ACCESS_LOGS = {
        "name": "cloudaudit_googleapis_com_data_access_*",
        "schema": None
    }


def get_table(table: TableDefinitions) -> Table:
    """
    Get a Table object using one of the enum definitions.

    Arguments:
        name {TableDefinitions} -- Enum name of the table.

    Returns:
        Table -- The table object representing the table.
    """
    return Table(**table.value)
