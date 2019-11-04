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
Key actions taken during the smart archiver run. These mostly involve GCS API
calls.
"""
import logging
from datetime import datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.bigquery import Row

from gcs_sa.bq.output import BigQueryOutput
from gcs_sa.bq.utils import get_bucket_and_object
from gcs_sa.config import get_config
from gcs_sa.gcs.client import get_gcs_client

LOG = logging.getLogger(__name__)


def rewrite_object(row: Row, storage_class: str, moved_output: BigQueryOutput,
                   excluded_output: BigQueryOutput):
    """
    Rewrites an object to the archive storage class and stores record of it.

    Arguments:
        row {Row} -- Row from result set.
        storage_class {str} -- [description]
        moved_output {BigQueryOutput} -- Output for records of moved objects.
        excluded_output {BigQueryOutput} -- Output for records of excluded
        objects.
    """
    config = get_config()
    dry_run = config.getboolean("RUNTIME", "DRY_RUN", fallback=False)
    record_original_create_time = config.getboolean(
        "RULES", "RECORD_ORIGINAL_CREATE_TIME", fallback=False)

    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])

    try:
        gcs = get_gcs_client()
        bucket = storage.bucket.Bucket(gcs, name=bucket_name)
        blob = storage.blob.Blob(object_name, bucket)
        current_create_time = None
        if record_original_create_time and not dry_run:
            # Get the blob info. Skip this on a dry run, as it creates 
            # an access record.
            LOG.debug("Getting original blob info.")
            blob_info = bucket.get_blob(object_name)
            current_create_time = blob_info.time_created if blob_info else None

        LOG.info("%s%s rewriting to: %s", "DRY RUN: " if dry_run else "",
                 object_path, storage_class)
        if not dry_run:
            blob.update_storage_class(storage_class, gcs)
            LOG.debug("%s rewrite to %s complete.", object_path, storage_class)

        object_info = {
            "storageClass": storage_class,
            "resourceName": row.resourceName,
            "size": blob.size,
            "moveTimestamp": str(datetime.now(timezone.utc))
        }
        if current_create_time:
            object_info.update(
                {"blobCreatedTimeWhenMoved": str(current_create_time)})

        moved_output.put(object_info)
        LOG.info("%s object move record queued for write to BQ: \n%s",
                 object_path, object_info)

    except NotFound:
        LOG.info(
            "%s skipped! This object wasn't found. Adding to excluded objects"
            " list so it will no longer be considered.", object_path)
        excluded_output.put({"resourceName": row.resourceName})
