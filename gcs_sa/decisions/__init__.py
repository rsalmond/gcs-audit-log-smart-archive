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
Key decision functions used during the smart archiver run. These mostly
look at BigQuery job results.
"""

from datetime import datetime, timezone
import logging
from typing import Tuple

from google.cloud.bigquery import Row

from gcs_sa.config import get_config
from gcs_sa.bq.utils import get_bucket_and_object

LOG = logging.getLogger(__name__)


def should_warm_up(row: Row) -> bool:
    """Decide whether an object should be rewritten to a "warmer" storage class.

    Arguments:
        row {google.cloud.bigquery.row} -- Row from result set.

    Returns:
        bool -- True if the object should be rewritten to STANDARD.
    """
    config = get_config()
    warm_threshold_days = config.getint('RULES', 'WARM_THRESHOLD_DAYS')
    warm_threshold_accesses = config.getint('RULES', 'WARM_THRESHOLD_ACCESSES')

    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])

    # Is the object's storage class known and STANDARD? Don't warm.
    if row.storageClass and row.storageClass == "STANDARD":
        LOG.debug("%s last moved to STANDARD, cannot warm up.", object_path)
        return False

    # It's not STANDARD, or unknown. Is it warm? If so, do the rewrite.
    # It may be a no-op if it's unknown and standard, but then it is a
    # known value.
    if row.recent_access_count >= warm_threshold_accesses:
        LOG.info(
            "%s accessed %s times in the last %s days, greater than %s "
            "accesses. Warming up to Standard storage.", object_path,
            row.recent_access_count, warm_threshold_days,
            warm_threshold_accesses)
        return True

    # This data is not STANDARD, but it's also cold. Don't warm.
    LOG.debug(
        "%s accessed %s times in the last %s days, fewer than %s accesses."
        "Will not warm.", object_path, row.recent_access_count,
        warm_threshold_days, warm_threshold_accesses)
    return False


def should_cool_down(row: Row) -> bool:
    """Decide whether an object should be rewritten to a "cooler" storage class.

    Arguments:
        row {google.cloud.bigquery.row} -- Row from result set.

    Returns:
        bool -- True if the object should be archived.
    """
    config = get_config()
    cold_storage_class = config.get('RULES', 'COLD_STORAGE_CLASS')
    cold_theshold_days = config.getint('RULES', 'COLD_THRESHOLD_DAYS')

    time_since_last_access = datetime.now(tz=timezone.utc) - row.lastAccess
    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])

    # Is the object's storage class known and cold? Don't cool.
    if row.storageClass and row.storageClass == cold_storage_class:
        LOG.debug("%s last moved to %s, cannot cool down.", object_path,
                  cold_storage_class)
        return False

    # Has it been more than the threshold days since last access? Cool down.
    if time_since_last_access.days >= cold_theshold_days:
        LOG.info(
            "%s last accessed %s ago, greater than %s days(s) ago, "
            "will cool down.", object_path, time_since_last_access,
            cold_theshold_days)
        return True

    # The object was accessed recently enough that it should stay where it is.
    LOG.debug(
        "%s last accessed %s ago, less than %s days(s) ago, will "
        "not cool down.", object_path, time_since_last_access,
        cold_theshold_days)
    return False
