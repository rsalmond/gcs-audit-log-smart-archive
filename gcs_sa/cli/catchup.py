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
Implementation of "catchup" command.
"""

import logging
from configparser import ConfigParser

from google.cloud.storage import Bucket, Client
from google.api_core.page_iterator import Page

from gcs_sa.bq.output import BigQueryOutput
from gcs_sa.bq.tables import TableDefinitions, get_table
from gcs_sa.config import get_config
from gcs_sa.gcs.client import get_gcs_client
from gcs_sa.thread import BoundedThreadPoolExecutor

LOG = logging.getLogger(__name__)


def catchup_command(buckets: [str] = None) -> None:
    """Implementation of the catchup command.
    
    Keyword Arguments:
        buckets {[type]} -- A list of buckets to use instead of the
        project-wide bucket listing. (default: {None})
    """
    config = get_config()
    gcs = get_gcs_client()
    # Call this once to initialize.
    _ = BigQueryOutput(
        get_table(TableDefinitions.CATCHUP_TABLE,
                  config.get("BIGQUERY", "CATCHUP_TABLE")))

    # if buckets is given, get each bucket object; otherwise, list all bucket
    # objects
    if buckets:
        buckets = [gcs.get_bucket(x) for x in buckets]
    else:
        buckets = [x for x in gcs.list_buckets()]

    total_buckets = len(buckets)
    buckets_listed = 0
    bucket_blob_counts = dict()

    workers = max(config.getint('RUNTIME', 'WORKERS') / 2, 1)
    with BoundedThreadPoolExecutor(max_workers=workers) as executor:
        for bucket in buckets:
            buckets_listed += 1
            executor.submit(bucket_lister, config, gcs, bucket, buckets_listed,
                            total_buckets, bucket_blob_counts)


def page_outputter(config: ConfigParser, bucket: Bucket, page: Page,
                   stats: dict) -> None:
    """Write a page of blob listing to BigQuery.

    Arguments:
        config {ConfigParser} -- The program config.
        bucket {Bucket} -- The bucket where this list page came from.
        page {Page} -- The Page object from the listing.
        stats {dict} -- A dictionary of bucket_name (str): blob_count (int)
    """
    catchup_output = BigQueryOutput(
        get_table(TableDefinitions.CATCHUP_TABLE,
                  config.get("BIGQUERY", "CATCHUP_TABLE")), False)
    blob_count = 0

    for blob in page:
        blob_count += 1
        metadata = blob.__dict__["_properties"]
        catchup_output.put(metadata)

    catchup_output.flush()
    stats[bucket] += blob_count
    LOG.info("%s blob records written for bucket %s.", stats[bucket], bucket)


def bucket_lister(config: ConfigParser, gcs: Client, bucket: Bucket,
                  bucket_number: int, total_buckets: int, stats: dict) -> None:
    """List a bucket, sending each page of the listing into an executor pool
    for processing.
    
    Arguments:
        config {ConfigParser} -- The program config.
        gcs {Client} -- A GCS client object.
        bucket {Bucket} -- A GCS Bucket object to list.
        bucket_number {int} -- The number of this bucket (out of the total).
        total_buckets {int} -- The total number of buckets that will be listed.
        stats {dict} -- A dictionary of bucket_name (str): blob_count (int)
    """
    LOG.info("Listing %s. %s of %s total buckets", bucket.name, bucket_number,
             total_buckets)
    stats[bucket] = 0

    workers = max(config.getint('RUNTIME', 'WORKERS') / 2, 1)
    with BoundedThreadPoolExecutor(max_workers=workers) as sub_executor:
        blobs = gcs.list_blobs(bucket)
        for page in blobs.pages:
            sub_executor.submit(page_outputter, config, bucket, page, stats)
