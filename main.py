#!/usr/bin/env python3
import argparse
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from os import getenv

from google.api_core.exceptions import NotFound
from google.cloud import storage

from helpers import (BigQueryOutput, event_is_fresh, get_bq_client,
                     get_bucket_and_object, get_gcs_client, load_config_file)

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger("smart_archiver." + __name__)


def get_moved_objects_output(config):
    """Creates, if not found, a table in which objects moved by this script to
    another storage class are stored. This table is used to exclude such items
    from future runs to keep execution time short.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since
        this is a DDL query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the
        given timeout.
    """
    moved_objects_table = "{}.{}.objects_moved_to_{}".format(
        config["BQ_JOB_PROJECT"] if "BQ_JOB_PROJECT" in config else
        config["PROJECT"], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])
    schema = """
            resourceName STRING, 
            size INT64,
            archiveTimestamp TIMESTAMP
        """
    return BigQueryOutput(config, moved_objects_table, schema)


def get_excluded_objects_output(config):
    """Creates, if not found, a table in which objects which should be ignored
    by this script are stored.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since
        this is a DDL query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the
        given timeout.
    """
    excluded_objects_table = "{}.{}.objects_excluded_from_archive".format(
        config["BQ_JOB_PROJECT"] if "BQ_JOB_PROJECT" in config else
        config["PROJECT"], config['DATASET_NAME'])
    schema = "resourceName STRING"
    return BigQueryOutput(config, excluded_objects_table, schema)


def query_access_table(config):
    """Queries the BigQuery audit log sink for the maximum access time of all
    objects which aren't in the moved objects table, and have been accessed
    since audit logging was turned on and sunk into the dataset.

    This is a wildcard table query, and can get quite large. To speed it up and lower costs, consider deleting tables older than the outer threshold for this script (e.g., 30 days, 60 days, 365 days, etc.)

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. This will be all objects which haven't been moved and have been accessed since audit logging was turned on and sunk into this table.

    Raises:
        google.cloud.exceptions.GoogleCloudError – If the job failed.
        concurrent.futures.TimeoutError – If the job did not complete in the given timeout.
    """
    LOG.info(
        "Getting last access of all objects, where last access is older than the DAYS_THRESHOLD, without already moved and excluded objects."
    )

    bqc = get_bq_client(config)

    access_log_tables = "`{}.{}.cloudaudit_googleapis_com_data_access_*`".format(
        config['PROJECT'], config['DATASET_NAME'])

    moved_objects_table = "`{}.{}.objects_moved_to_{}`".format(
        config['PROJECT'], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])

    excluded_objects_table = "`{}.{}.objects_excluded_from_archive`".format(
        config['PROJECT'], config['DATASET_NAME'])

    querytext = """
    SELECT a.resourceName, lastAccess FROM (
        SELECT REGEXP_REPLACE(protopayload_auditlog.resourceName, "gs://.*/", "") AS resourceName,
        MAX(timestamp) AS lastAccess FROM {0}
        WHERE
            _TABLE_SUFFIX BETWEEN 
            FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {3} DAY)) AND
            FORMAT_DATE("%Y%m%d", CURRENT_DATE())
        GROUP BY resourceName) 
    AS a 
    LEFT JOIN {1} as b ON a.resourceName = b.resourceName
    LEFT JOIN {2} as c ON a.resourceName = c.resourceName
    WHERE b.resourceName IS NULL AND c.resourceName IS NULL AND
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), lastAccess, DAY) >= {4}
    """.format(access_log_tables, moved_objects_table, excluded_objects_table,
               int(config["DAYS_THRESHOLD"]) + int(config["DAYS_BETWEEN_RUNS"]),
               int(config["DAYS_THRESHOLD"]))
    LOG.debug("Query: %s", querytext)
    query_job = bqc.query(querytext)
    return query_job.result()


def should_archive(timedelta, object_path, config):
    """Decide whether an object should be archived.

    Arguments:
        timedelta {datetime.timedelta} -- Time since last access of the object.
        object_path {string} -- Full gs:// path to the object
        config {dict} -- Configuration of this program.

    Returns:
        bool -- True if the object should be archived.
    """
    if 'SECONDS_THRESHOLD' in config:
        # If present, SECONDS_THRESHOLD will override. Note this only works for seconds since midnight. This is only for development use.
        if timedelta.seconds >= int(config['SECONDS_THRESHOLD']):
            LOG.info("%s last accessed %s ago, greater than %s second(s) ago",
                     object_path, timedelta, config['SECONDS_THRESHOLD'])
            return True
        LOG.info("%s last accessed %s ago, less than %s second(s) ago",
                 object_path, timedelta, config['SECONDS_THRESHOLD'])
        return False
    else:
        if timedelta.days >= int(config['DAYS_THRESHOLD']):
            LOG.info("%s last accessed %s ago, greater than %s days(s) ago",
                     object_path, timedelta, config['DAYS_THRESHOLD'])
            return True
        LOG.info("%s last accessed %s ago, less than %s days(s) ago",
                 object_path, timedelta, config['DAYS_THRESHOLD'])
        return False


def archive_object(resourceName, bucket_name, object_name, object_path, config,
                   moved_output, excluded_output):
    """Rewrites an object to the archive storage class and stores record of it.

    Arguments:
        resourceName {string} -- The resource name for the object as shown in the
        audit log (protopayload_auditlog.resourceName).
        bucket_name {string} -- The name of the bucket where the object resides.
        object_name {string} -- The name of the object within the bucket.
        object_path {string} -- The full gs:// path to the object.

    Returns:
        string -- Human-readable output describing the operations undertaken.
    """
    dry_run = config["DRY_RUN"]
    try:
        gcs = get_gcs_client(config)
        bucket = storage.bucket.Bucket(gcs, name=bucket_name)
        blob = storage.blob.Blob(object_name, bucket)
        object_info = {
            "resourceName": resourceName,
            "size": blob.size,
            "archiveTimestamp": str(datetime.now(timezone.utc))
        }
        LOG.info("%s%s rewriting to: %s", "DRY RUN: " if dry_run else "",
                 object_path, config['NEW_STORAGE_CLASS'])
        if not dry_run:
            blob.update_storage_class(config['NEW_STORAGE_CLASS'], gcs)
            LOG.info("%s rewrite to %s complete.\n%s", object_path,
                     config['NEW_STORAGE_CLASS'], object_info)
        moved_output.put(object_info)
        LOG.debug("%s object storage class status queued for write to BQ.",
                  object_path)
    except NotFound:
        LOG.info(
            "%s skipped! This object wasn't found. Adding to excluded objects list so it will no longer be considered.",
            object_path)
        excluded_output.put({"resourceName": resourceName})


def evaluate_objects(config):
    """Evaluates objects in the audit log to see if they should be moved to a
    new storage class.

    Arguments:
        audit_log {google.cloud.bigquery.table.RowIterator} -- The result set of a query of the audit log table, with the columns `resourceName` and `lastAccess`.
    """
    moved_output = get_moved_objects_output(config)
    excluded_output = get_excluded_objects_output(config)

    audit_log = query_access_table(config)

    with ThreadPoolExecutor() as executor:
        jobs = list()

        # evaluate, archive and record
        for row in audit_log:
            timedelta = datetime.now(tz=timezone.utc) - row.lastAccess
            bucket_name, object_name = get_bucket_and_object(row.resourceName)
            object_path = "/".join(["gs:/", bucket_name, object_name])
            if should_archive(timedelta, object_path, config):
                jobs.append(
                    executor.submit(archive_object, row.resourceName,
                                    bucket_name, object_name, object_path,
                                    config, moved_output, excluded_output))

        # wait for all of the row jobs to complete
        wait(jobs)
        moved_output.flush()
        LOG.info(moved_output.stats())
        excluded_output.flush()
        LOG.info(excluded_output.stats())


def find_config_file(args):
    if args.config_file:
        return args.config_file
    elif getenv("SMART_ARCHIVE_CONFIG"):
        return getenv("SMART_ARCHIVE_CONFIG")
    return "./default.cfg"


def build_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file")
    args = parser.parse_args()

    config_file = find_config_file(args)

    LOG.info("Loading config: %s", config_file)
    return load_config_file(config_file,
                            required=[
                                'PROJECT', 'DATASET_NAME', 'DAYS_THRESHOLD',
                                'NEW_STORAGE_CLASS', 'BQ_BATCH_WRITE_SIZE',
                                'DAYS_BETWEEN_RUNS'
                            ],
                            defaults={
                                'LOG_LEVEL': 'INFO',
                                'DRY_RUN': False
                            })


def archive_cold_objects(data, context):
    """Entrypoint for Google Cloud Function.

    Arguments:
        data {dict} -- Event data passed to the function in the pubsub message.
        context {dict} -- Context of function execution.
    """
    if event_is_fresh(data, context):
        config = build_config()
        # set level at root logger
        logging.getLogger("smart_archiver").setLevel(config['LOG_LEVEL'])
        LOG.debug("Configuration: \n %s", config)

        LOG.info("Evaluating accessed objects for rewriting to %s.",
                 config['NEW_STORAGE_CLASS'])
        evaluate_objects(config)

        LOG.info("Done.")


if __name__ == '__main__':
    archive_cold_objects(None, None)
