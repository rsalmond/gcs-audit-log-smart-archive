#!/usr/bin/env python3
import warnings
from atexit import register
from atexit import unregister
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from queue import Queue
from sys import exit

from dateutil import parser
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")


config = {
    "CONFIG_FILE_PATH": "./config.cfg"
}


def load_config():
    """
    Loads configuration file into module variables.
    """
    config_file = open(config["CONFIG_FILE_PATH"], "r")
    for line in config_file:
        # ignore comments
        if line.startswith('#'):
            continue
        # parse the line
        tokens = line.split('=')
        if len(tokens) != 2:
            print("Error parsing config tokens: %s" % tokens)
            continue
        k, v = tokens
        config[k.strip()] = v.strip()
    # quick validation
    for required in [
        'PROJECT',
        'DATASET_NAME',
        'SECONDS_THRESHOLD',
            'NEW_STORAGE_CLASS']:
        if required not in config.keys() or config[required] is "CONFIGURE_ME":
            print('Missing required config item: {}'.format(required))
            exit(1)


def initialize_moved_objects_table():
    """Creates, if not found, a table in which objects moved by this script to another storage class are stored. This table is used to exclude such items from future runs to keep execution time short.

    Returns:
        google.cloud.bigquery.table.RowIterator -- Result of the query. Since this is a DDL query, this will always be empty if it succeeded.

    Raises:
        google.cloud.exceptions.GoogleCloudError –- If the job failed.
        concurrent.futures.TimeoutError –- If the job did not complete in the given timeout.
    """
    bq = get_bq_client()

    moved_objects_table = "`{}.{}.objects_moved_to_{}`".format(
        config['PROJECT'], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])

    querytext = """
        CREATE TABLE IF NOT EXISTS {} (
            resourceName STRING, 
            size INT64,
            archiveTimestamp TIMESTAMP
        )""".format(
        moved_objects_table)

    query_job = bq.query(querytext)
    return query_job.result()


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

    querytext = """
    SELECT a.resourceName, lastAccess FROM (
        SELECT REGEXP_REPLACE(protopayload_auditlog.resourceName, "gs://.*/", "") AS resourceName,
        MAX(timestamp) AS lastAccess FROM {0}
        GROUP BY resourceName) 
    AS a 
    LEFT JOIN {1} as b ON a.resourceName = b.resourceName
    WHERE b.resourceName IS NULL
""".format(access_log_tables, moved_objects_table)
    query_job = bq.query(querytext)
    return query_job.result()


class IterableQueue(Queue):

    _sentinel = object()

    def __iter__(self):
        return iter(self.get, self._sentinel)

    def close(self):
        self.put(self._sentinel)


moved_objects = IterableQueue()

def flatten(iterable, iter_types=(list, tuple)):
    """Flattens nested iterables into a flat iterable.
    """
    for i in iterable:
        if isinstance(i, iter_types):
            for j in flatten(i, iter_types):
                yield j
        else:
            yield i


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
    bq = get_bq_client()

    # TODO: configurable?
    moved_objects_table = "{}.{}.objects_moved_to_{}".format(
        config['PROJECT'], config['DATASET_NAME'], config['NEW_STORAGE_CLASS'])

    insert_errors = []
    batch = []
    print("Starting BQ insert stream...")
    for row in moved_objects:
        batch.append(row)
        if len(batch) > config["BQ_BATCH_WRITE_SIZE"]:
            try:
                insert_errors.append(bq.insert_rows_json(moved_objects_table, batch))
            except BadRequest as e:
                if not e.message.endswith("No rows present in the request."):
                    raise e
            finally:
                batch.clear()

    print("Finished BQ insert stream...")
    return "BQ Errors:\t{}".format([x for x in flatten(insert_errors)])

def evaluate_objects(audit_log):
    """Evaluates objects in the audit log to see if they should be moved to a new storage class.

    Arguments:
        audit_log {google.cloud.bigquery.table.RowIterator} -- The result set of a query of the audit log table, with the columns `resourceName` and `lastAccess`.
    """

    # This function will be run o(objects) times in the executor pool
    def _archive_object(row, bucket_name, object_name, object_path):
        gcs = get_gcs_client()
        bucket = storage.bucket.Bucket(gcs, name=bucket_name)
        output = []
        try:
            blob = storage.blob.Blob(object_name, bucket)
            blob.update_storage_class(config['NEW_STORAGE_CLASS'])
            output.append("\tRewrote {} to: {}".format(
                object_path, config['NEW_STORAGE_CLASS']))
            moved_objects.put(
                {
                    "resourceName": row.resourceName,
                    "size": blob.size,
                    "archiveTimestamp": str(datetime.now(timezone.utc))
                }, True)
            output.append(
                "\tStreaming {} object archive status to BQ.".format(object_path))
        except NotFound:
            output.append("Skipping {} :: this object seems to have been deleted.".format(
                object_path))
        return '\n'.join(output)

    with ThreadPoolExecutor(max_workers=8) as executor:
        # start BQ stream
        stream_future = executor.submit(moved_objects_insert_stream)
        archive_futures = []

        def cleanup():
            # terminate the BQ stream
            moved_objects.close()
            executor.shutdown()
            print(stream_future.result())
        # shutdown hook for cleanup in case we get a sigterm
        register(cleanup)

        # evaluate, archive and record
        for row in audit_log:
            timedelta = datetime.now(tz=timezone.utc) - row.lastAccess
            bucket_name, object_name = get_bucket_and_object(row.resourceName)
            object_path = "/".join(["gs:/", bucket_name, object_name])
            if timedelta.seconds >= int(config['SECONDS_THRESHOLD']):
                print(object_path, "last accessed {} ago, greater than {} seconds(s) ago".format(
                    timedelta, config['SECONDS_THRESHOLD']))
                archive_futures.append(
                    executor.submit(_archive_object, row, bucket_name,
                                    object_name, object_path))
            else:
                print(object_path, "last accessed {} ago, less than {} seconds(s) ago".format(
                    timedelta, config['SECONDS_THRESHOLD']))
        # print results as they come
        for f in as_completed(archive_futures):
            print(f.result())

        # normal cleanup
        unregister(cleanup)
        cleanup()


def get_bucket_and_object(resource_name):
    """Given an audit log resourceName, parse out the bucket name and object path within the bucket.

    Returns:
        (str, str) -- ([bucket name], [object name])
    """
    pathparts = resource_name.split("buckets/", 1)[1].split("/", 1)

    bucket_name = pathparts[0]
    object_name = pathparts[1].split("objects/", 1)[1]

    return (bucket_name, object_name)


clients = {}


def get_bq_client():
    """Get a BigQuery client. Uses a simple create-if-not-found mechanism to avoid repeatedly creating new clients.

    Returns:
        google.cloud.bigquery.Client -- A BigQuery client.
    """
    if 'bq' not in clients:
        bq = bigquery.Client()
        clients['bq'] = bq
    return clients['bq']


def get_gcs_client():
    """Get a GCS client. Uses a simple create-if-not-found mechanism to avoid repeatedly creating new clients.

    Returns:
        google.cloud.storage.Client -- A GCS client.
    """
    if 'gcs' not in clients:
        gcs = storage.Client()
        clients['gcs'] = gcs
    return clients['gcs']


def event_is_fresh(data, context):
    """Ensure a background Cloud Function only executes within a certain
    time period after the triggering event.

    Args:
        data (dict): The event payload.
        context (google.cloud.functions.Context): The event metadata.
    Returns:
        None; output is written to Stackdriver Logging
    """
    if data is None:
        # desktop run
        return True

    timestamp = context.timestamp

    event_time = parser.parse(timestamp)
    event_age = (datetime.now(timezone.utc) - event_time).total_seconds()
    event_age_ms = event_age * 1000

    # Ignore events that are too old
    # TODO: Should this be configurable?
    max_age_ms = 10000
    if event_age_ms > max_age_ms:
        print('Event timeout. Dropping {} (age {}ms)'.format(
            context.event_id, event_age_ms))
        return False
    return True


def archive_cold_objects(data, context):
    if event_is_fresh(data, context):
        print("Loading config.")
        load_config()
        print("Initializing moved objects table (if not found).")
        initialize_moved_objects_table()
        print("Getting access log, except for already moved objects.")
        audit_log = query_access_table()
        print("Evaluating accessed objects for rewriting to {}.".format(
            config['NEW_STORAGE_CLASS']))
        evaluate_objects(audit_log)
        print("Done.")


if __name__ == '__main__':
    archive_cold_objects(None, None)
