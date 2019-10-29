#!/usr/bin/env python3
import argparse
import logging
import warnings
from atexit import register
from threading import Thread
from datetime import datetime, timezone
from os import getenv
from queue import Queue

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.bigquery.job import QueryJobConfig, WriteDisposition

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
    moved_objects_table = "{}.{}.objects_moved".format(
        config["BQ_JOB_PROJECT"] if "BQ_JOB_PROJECT" in config else
        config["PROJECT"], config['DATASET_NAME'])
    schema = """
            resourceName STRING, 
            storageClass STRING,
            size INT64,
            blobCreatedTimeWhenMoved TIMESTAMP,
            moveTimestamp TIMESTAMP
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
    excluded_objects_table = "{}.{}.objects_excluded".format(
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
    LOG.info("Getting access history.")

    bqc = get_bq_client(config)

    access_log_tables = "`{}.{}.cloudaudit_googleapis_com_data_access_*`".format(
        config['PROJECT'], config['DATASET_NAME'])

    moved_objects_table = "`{}.{}.objects_moved`".format(
        config['PROJECT'], config['DATASET_NAME'])

    excluded_objects_table = "`{}.{}.objects_excluded`".format(
        config['PROJECT'], config['DATASET_NAME'])

    catch_up_union = ""
    if 'CATCHUP_TABLE' in config:
        catchup_table = "`{}.{}.{}`".format(config['PROJECT'],
                                            config['DATASET_NAME'],
                                            config['CATCHUP_TABLE'])
        catch_up_union = """
            UNION ALL
            SELECT
                REGEXP_REPLACE(url,"gs://(.*)/(.*)","projects/_/buckets/{1}1/objects/{1}2") AS resourceName,
                created AS timestamp
            FROM
                {0}
        """.format(catchup_table, "\\\\")

    querytext = """
WITH 
most_recent_moves AS (SELECT
    z.*
FROM
    {1} AS z
INNER JOIN (
    SELECT
        resourceName as object,
        MAX(moveTimestamp) as most_recent
    FROM
        {1}
    GROUP BY resourceName) as y on y.most_recent = z.moveTimestamp)
,
access_records AS (SELECT
    REGEXP_REPLACE(protopayload_auditlog.resourceName, "gs://.*/", "") AS resourceName,
    timestamp
  FROM {0}
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {3} DAY))
    AND FORMAT_DATE("%Y%m%d", CURRENT_DATE())
  {5}
)

SELECT a.resourceName, b.storageClass, lastAccess, recent_access_count FROM (
    SELECT resourceName,
    MAX(timestamp) AS lastAccess,
    COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, DAY) <= {4}) AS recent_access_count
    FROM access_records
    GROUP BY resourceName)
AS a 
LEFT JOIN most_recent_moves as b ON a.resourceName = b.resourceName
LEFT JOIN {2} as c ON a.resourceName = c.resourceName
WHERE c.resourceName IS NULL
    """.format(
        access_log_tables, moved_objects_table, excluded_objects_table,
        int(config["COLD_THRESHOLD_DAYS"]) + int(config["DAYS_BETWEEN_RUNS"]),
        int(config['WARM_THRESHOLD_DAYS']), catch_up_union)
    LOG.debug("Query: %s", querytext)

    query_job_config = QueryJobConfig()
    temp_table = "{}.{}.smart_archive_temp".format(
        config['PROJECT'], config['DATASET_NAME'])
    query_job_config.destination = temp_table
    query_job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    query_job = bqc.query(query=querytext, job_config=query_job_config)
    
    return query_job.result()


def should_warm_up(row, config):
    """Decide whether an object should be rewritten to a "warmer" storage class.

    Arguments:
        row {BigQuery.row} -- Row from result set.
        config {dict} -- Configuration of this program.

    Returns:
        bool -- True if the object should be archived.
    """
    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])
    if row.storageClass and row.storageClass == "STANDARD":
        LOG.debug("%s last moved to STANDARD, cannot warm up.", object_path)
        return False
    if row.recent_access_count >= int(config['WARM_THRESHOLD_ACCESSES']):
        LOG.info(
            "%s accessed %s times in the last %s days, greater than %s accesses. Warming up to Standard storage.",
            object_path, row.recent_access_count, config['WARM_THRESHOLD_DAYS'],
            config['WARM_THRESHOLD_ACCESSES'])
        return True
    LOG.debug(
        "%s accessed %s times in the last %s days, fewer than %s accesses. Will not warm.",
        object_path, row.recent_access_count, config['WARM_THRESHOLD_DAYS'],
        config['WARM_THRESHOLD_ACCESSES'])
    return False


def should_cool_down(row, config):
    """Decide whether an object should be rewritten to a "cooler" storage class.

    Arguments:
        row {BigQuery.row} -- Row from result set.
        config {dict} -- Configuration of this program.

    Returns:
        bool -- True if the object should be archived.
    """
    timedelta = datetime.now(tz=timezone.utc) - row.lastAccess
    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])
    if row.storageClass and row.storageClass == config['COLD_STORAGE_CLASS']:
        LOG.debug("%s last moved to %s, cannot cool down.", object_path,
                  config['COLD_STORAGE_CLASS'])
        return False
    if 'SECONDS_THRESHOLD' in config:
        # If present, SECONDS_THRESHOLD will override. Note this only works for seconds since midnight. This is only for development use.
        if timedelta.seconds >= int(config['SECONDS_THRESHOLD']):
            LOG.info(
                "%s last accessed %s ago, greater than %s second(s) ago, will cool down.",
                object_path, timedelta, config['SECONDS_THRESHOLD'])
            return True
        LOG.debug(
            "%s last accessed %s ago, less than %s second(s) ago. will not cool down.",
            object_path, timedelta, config['SECONDS_THRESHOLD'])
        return False
    else:
        if timedelta.days >= int(config['COLD_THRESHOLD_DAYS']):
            LOG.info(
                "%s last accessed %s ago, greater than %s days(s) ago, will cool down.",
                object_path, timedelta, config['COLD_THRESHOLD_DAYS'])
            return True
        LOG.debug(
            "%s last accessed %s ago, less than %s days(s) ago, will not cool down.",
            object_path, timedelta, config['COLD_THRESHOLD_DAYS'])
        return False


def rewrite_object(row, config, storage_class, moved_output, excluded_output):
    """Rewrites an object to the archive storage class and stores record of it.

    Arguments:
        row {BigQuery.row} -- Row from result set.
        config {dict} -- Configuration of this program.
        moved_output {helpers.BigQueryOutput} -- Output for records of moved objects.
        exluded_output {helpers.BigQueryOutput} -- Output for records of not found/excluded objects.

    Returns:
        string -- Human-readable output describing the operations undertaken.
    """
    bucket_name, object_name = get_bucket_and_object(row.resourceName)
    object_path = "/".join(["gs:/", bucket_name, object_name])
    dry_run = config["DRY_RUN"]
    try:
        gcs = get_gcs_client(config)
        bucket = storage.bucket.Bucket(gcs, name=bucket_name)
        blob = storage.blob.Blob(object_name, bucket)
        current_create_time = None
        if not dry_run and "RECORD_ORIGINAL_CREATE_TIME" in config:
            # Get the blob info. Skip this on a dry run, as it creates an access record.
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
        LOG.info("%s new storage class queued for write to BQ: \n%s",
                 object_path, object_info)
    except NotFound:
        LOG.info(
            "%s skipped! This object wasn't found. Adding to excluded objects list so it will no longer be considered.",
            object_path)
        excluded_output.put({"resourceName": row.resourceName})


def evaluate_objects(config):
    """Evaluates objects in the audit log to see if they should be moved to a
    new storage class.

    Arguments:
        audit_log {google.cloud.bigquery.table.RowIterator} -- The result set of a query of the audit log table, with the columns `resourceName` and `lastAccess`.
    """
    moved_output = get_moved_objects_output(config)
    excluded_output = get_excluded_objects_output(config)
    work_queue = Queue(maxsize=3000)

    # evaluate, archive and record
    def archive_worker():
        while True:
            row = work_queue.get()
            if not row:
                break
            if should_warm_up(row, config):
                rewrite_object(row, config, 'STANDARD', moved_output,
                               excluded_output)
            elif should_cool_down(row, config):
                rewrite_object(row, config, config['COLD_STORAGE_CLASS'],
                               moved_output, excluded_output)
            work_queue.task_done()

    # Start all worker threads
    worker_threads = []
    for _ in range(32):
        t = Thread(target=archive_worker)
        t.start()
        worker_threads.append(t)

    # Register cleanup as shutdown hook
    def cleanup():
        # Flush any remaining output
        moved_output.flush()
        excluded_output.flush()
        # Print statistics
        LOG.info("%s rows read.", rows_read)
        LOG.info(moved_output.stats())
        LOG.info(excluded_output.stats())

    register(cleanup)

    rows_read = 0
    # Enqueue all work
    last_accesses = query_access_table(config)
    for row in last_accesses:
        rows_read += 1
        work_queue.put(row)

    # wait for all of the row jobs to complete
    LOG.info("All work enqueued. Waiting for last jobs to complete.")
    work_queue.join()

    # shutdown workers
    for _ in range(32):
        work_queue.put(None)
    for t in worker_threads:
        t.join()


def find_config_file(args):
    if args.config_file:
        return args.config_file
    elif getenv("SMART_ARCHIVE_CONFIG"):
        return getenv("SMART_ARCHIVE_CONFIG")
    return "./default.cfg"


def build_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", required=False)
    args = parser.parse_args()

    config_file = find_config_file(args)

    LOG.info("Loading config: %s", config_file)
    return load_config_file(config_file,
                            required=[
                                'PROJECT',
                                'DATASET_NAME',
                                'COLD_STORAGE_CLASS',
                                'COLD_THRESHOLD_DAYS',
                                'WARM_THRESHOLD_DAYS',
                                'WARM_THRESHOLD_ACCESSES',
                                'DAYS_BETWEEN_RUNS',
                                'BQ_BATCH_WRITE_SIZE',
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
        if hasattr(logging, config.get('LOG_LEVEL')):
            logging.getLogger("smart_archiver").setLevel(
                getattr(logging, config.get('LOG_LEVEL')))
        else:
            print("Invalid log level specified: {}".format(
                config.get('LOG_LEVEL')))
            exit(1)
        LOG.debug("Configuration: \n %s", config)
        LOG.info("Evaluating accessed objects for cool down/warm up.")
        evaluate_objects(config)

        LOG.info("Running shutdown hooks and exiting normally.")


if __name__ == '__main__':
    archive_cold_objects(None, None)
