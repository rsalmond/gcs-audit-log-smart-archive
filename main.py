#!/usr/bin/env python3
import warnings
from atexit import register, unregister
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from os import getenv
from sys import exit

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from helpers import (IterableQueue, bq_insert_stream, event_is_fresh,
                     get_bq_client, get_bucket_and_object, get_gcs_client,
                     initialize_table, load_config_file)

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")


config_file = getenv("SMART_ARCHIVE_CONFIG") if getenv(
    "SMART_ARCHIVE_CONFIG") else "./default.cfg"
print("Loading config: {}".format(config_file))
config = load_config_file(config_file, required=[
    'PROJECT',
    'DATASET_NAME',
    'DAYS_THRESHOLD',
    'NEW_STORAGE_CLASS',
    'BQ_BATCH_WRITE_SIZE'])


def initialize_moved_objects_table():
    """Creates, if not found, a table in which objects moved by this script to another storage class are stored. This table is used to exclude such items from future runs to keep execution time short.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since this is a DDL query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the given timeout.
    """
    moved_objects_table = "`{}.{}.objects_moved_to_{}`".format(
        config['PROJECT'], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])
    schema = """
            resourceName STRING, 
            size INT64,
            archiveTimestamp TIMESTAMP
        """
    return initialize_table(moved_objects_table, schema)


def initialize_excluded_objects_table():
    """Creates, if not found, a table in which objects which should be ignored by this script are stored.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since this is a DDL query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the given timeout.
    """
    excluded_objects_table = "`{}.{}.objects_excluded_from_archive`".format(
        config['PROJECT'], config['DATASET_NAME'])
    schema = "resourceName STRING"
    return initialize_table(excluded_objects_table, schema)


moved_objects = IterableQueue(maxsize=10000)


def moved_objects_insert_stream():
    """Insert the resource name of an object into the table of moved objects for exclusion later.

    Arguments:
        resource_name {str} -- The resource name of the object, as given in the audit log.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since this is an INSERT query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the given timeout.
    """
    # TODO: configurable?
    moved_objects_table = "{}.{}.objects_moved_to_{}".format(
        config['PROJECT'], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])
    return bq_insert_stream(moved_objects_table, moved_objects, config["BQ_BATCH_WRITE_SIZE"])


excluded_objects = IterableQueue(maxsize=10000)


def excluded_objects_insert_stream():
    """Insert the resource name of an object into the table of excluded objects.

    Arguments:
        resource_name {str} -- The resource name of the object, as given in the audit log.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since this is an INSERT query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the given timeout.
    """

    # TODO: configurable?
    excluded_objects_table = "{}.{}.objects_excluded_from_archive".format(
        config['PROJECT'], config['DATASET_NAME'])

    return bq_insert_stream(excluded_objects_table, excluded_objects, config["BQ_BATCH_WRITE_SIZE"])


def query_access_table():
    """Queries the BigQuery audit log sink for the maximum access time of all objects which aren't in the moved objects table, and have been accessed since audit logging was turned on and sunk into the dataset.

    This is a wildcard table query, and can get quite large. To speed it up and lower costs, consider deleting tables older than the outer threshold for this script (e.g., 30 days, 60 days, 365 days, etc.)

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. This will be all objects which haven't been moved and have been accessed since audit logging was turned on and sunk into this table.

    Raises:
        google.cloud.exceptions.GoogleCloudError – If the job failed.
        concurrent.futures.TimeoutError – If the job did not complete in the given timeout.
    """
    bq = get_bq_client()

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
        GROUP BY resourceName) 
    AS a 
    LEFT JOIN {1} as b ON a.resourceName = b.resourceName
    LEFT JOIN {2} as c ON a.resourceName = c.resourceName
    WHERE b.resourceName IS NULL AND c.resourceName IS NULL
    """.format(access_log_tables, moved_objects_table, excluded_objects_table)
    query_job = bq.query(querytext)
    return query_job.result()


def archive_object(resourceName, bucket_name, object_name, object_path):
    """Rewrites an object to the archive storage class and stores record of it.

    Arguments:
        resourceName {string} -- The resource name for the object as shown in the audit log (protopayload_auditlog.resourceName).
        bucket_name {string} -- The name of the bucket where the object resides.
        object_name {string} -- The name of the object within the bucket.
        object_path {string} -- The full gs:// path to the object.

    Returns:
        string -- Human-readable output describing the operations undertaken.
    """
    gcs = get_gcs_client()
    bucket = storage.bucket.Bucket(gcs, name=bucket_name)
    try:
        blob = storage.blob.Blob(object_name, bucket)
        blob.update_storage_class(config['NEW_STORAGE_CLASS'])
        print("{} rewritten to: {}".format(
            object_path, config['NEW_STORAGE_CLASS']))
        moved_objects.put(
            {
                "resourceName": resourceName,
                "size": blob.size,
                "archiveTimestamp": str(datetime.now(timezone.utc))
            }, True)
        print(
            "{} object archive status streaming to BQ.".format(object_path))
    except NotFound:
        print("{} skipped! This object wasn't found. Adding to excluded objects list so it will no longer be considered.".format(
            object_path))
        excluded_objects.put(
            {
                "resourceName": resourceName
            }, True)


def should_archive(timedelta, object_path):
    if 'SECONDS_THRESHOLD' in config:
        # If present, SECONDS_THRESHOLD will override. Note this only works for seconds since midnight. This is only for development use.
        if timedelta.seconds >= int(config['SECONDS_THRESHOLD']):
            print(object_path, "last accessed {} ago, greater than {} second(s) ago".format(
                timedelta, config['SECONDS_THRESHOLD']))
            return True
        print(object_path, "last accessed {} ago, less than {} second(s) ago".format(
            timedelta, config['SECONDS_THRESHOLD']))
        return False
    else:
        if timedelta.days >= int(config['DAYS_THRESHOLD']):
            print(object_path, "last accessed {} ago, greater than {} days(s) ago".format(
                timedelta, config['DAYS_THRESHOLD']))
            return True
        print(object_path, "last accessed {} ago, less than {} days(s) ago".format(
            timedelta, config['DAYS_THRESHOLD']))
        return False


def evaluate_objects(audit_log):
    """Evaluates objects in the audit log to see if they should be moved to a new storage class.

    Arguments:
        audit_log {google.cloud.bigquery.table.RowIterator} -- The result set of a query of the audit log table, with the columns `resourceName` and `lastAccess`.
    """

    with ThreadPoolExecutor(max_workers=8) as executor:
        # start BQ stream
        executor.submit(moved_objects_insert_stream)
        executor.submit(excluded_objects_insert_stream)

        def cleanup():
            # terminate the BQ stream
            moved_objects.close()
            excluded_objects.close()
            executor.shutdown()

        # shutdown hook for cleanup in case we get a sigterm
        register(cleanup)

        # evaluate, archive and record
        for row in audit_log:
            timedelta = datetime.now(tz=timezone.utc) - row.lastAccess
            bucket_name, object_name = get_bucket_and_object(row.resourceName)
            object_path = "/".join(["gs:/", bucket_name, object_name])
            if should_archive(timedelta, object_path):
                executor.submit(archive_object, row.resourceName,
                                bucket_name, object_name, object_path)

        # normal cleanup
        unregister(cleanup)
        cleanup()


def archive_cold_objects(data, context):
    if event_is_fresh(data, context):
        print("Initializing moved objects table (if not found).")
        initialize_moved_objects_table()
        print("Initializing excluded objects table (if not found).")
        initialize_excluded_objects_table()
        print("Getting access log, except for already moved objects.")
        audit_log = query_access_table()
        print("Evaluating accessed objects for rewriting to {}.".format(
            config['NEW_STORAGE_CLASS']))
        evaluate_objects(audit_log)
        print("Done.")


if __name__ == '__main__':
    archive_cold_objects(None, None)
