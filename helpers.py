import logging
from datetime import datetime, timezone
from queue import Queue

from dateutil import parser as dateparser
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery, storage

LOG = logging.getLogger("smart_archiver." + __name__)


def event_is_fresh(data, context):
    """Ensure a background Cloud Function only executes within a certain time
    period after the triggering event.

    Args:     data (dict): The event payload.     context
    (google.cloud.functions.Context): The event metadata. Returns:
    None; output is written to Stackdriver Logging
    """
    if data is None:
        LOG.debug("Running outside of Cloud Functions.")
        return True

    LOG.debug(context)
    timestamp = context.timestamp

    event_time = dateparser.parse(timestamp)
    event_age = (datetime.now(timezone.utc) - event_time).total_seconds()
    event_age_ms = event_age * 1000

    # Ignore events that are too old
    # TODO: Should this be configurable?
    max_age_ms = 10000
    if event_age_ms > max_age_ms:
        LOG.info('Event is too old. Dropping {} (age {}ms)'.format(
            context.event_id, event_age_ms))
        return False
    return True


def load_config_file(filepath, required=[], defaults={}):
    """Loads configuration file into module variables."""
    config = defaults
    config_file = open(filepath, "r")
    for line in config_file:
        # ignore comments
        if line.startswith('#'):
            continue
        # parse the line
        tokens = line.split('=')
        if len(tokens) != 2:
            LOG.info("Error parsing config tokens: %s" % tokens)
            continue
        k, v = tokens
        config[k.strip()] = v.strip()
    # quick validation
    for r in required:
        if r not in config.keys() or config[r] == "CONFIGURE_ME":
            LOG.info('Missing required config item: {}'.format(r))
            exit(1)
    return config


clients = {}


def get_bq_client(config):
    """Get a BigQuery client. Uses a simple create-if-not-found mechanism to
    avoid repeatedly creating new clients.

    Returns:     google.cloud.bigquery.Client -- A BigQuery client.
    """
    if 'bq' not in clients:
        bq = bigquery.Client(
            project=config["BQ_JOB_PROJECT"] if "BQ_JOB_PROJECT" in
            config else config["PROJECT"])
        LOG.debug("Created new BigQuery client.")
        clients['bq'] = bq
    return clients['bq']


def get_gcs_client(config):
    """Get a GCS client. Uses a simple create-if-not-found mechanism to avoid
    repeatedly creating new clients.

    Returns:     google.cloud.storage.Client -- A GCS client.
    """
    if 'gcs' not in clients:
        gcs = storage.Client(project=config["PROJECT"])
        LOG.debug("Created new GCS client.")
        clients['gcs'] = gcs
    return clients['gcs']


def initialize_table(config, name, schema):
    """Creates, if not found, a table.

    Arguments:     name {string} -- The fully qualified table name.
    schema {string} -- The schema portion of a BigQuery CREATE TABLE DDL
    query. For example: "resourceName STRING"  Returns:
    google.cloud.bigquery.table.RowIterator -- Result of the query. Since
    this is a DDL query, this will always be empty if it succeeded.
    Raises:     google.cloud.exceptions.GoogleCloudError –- If the job
    failed.     concurrent.futures.TimeoutError –- If the job did not
    complete in the given timeout.
    """
    bq = get_bq_client(config)

    querytext = """
        CREATE TABLE IF NOT EXISTS {} (
        {}
        )""".format(name, schema)

    LOG.debug("Query: \n{}".format(querytext))

    query_job = bq.query(querytext)
    return query_job.result()


def get_bucket_and_object(resource_name):
    """Given an audit log resourceName, parse out the bucket name and object
    path within the bucket.

    Returns:     (str, str) -- ([bucket name], [object name])
    """
    pathparts = resource_name.split("buckets/", 1)[1].split("/", 1)

    bucket_name = pathparts[0]
    object_name = pathparts[1].split("objects/", 1)[1]

    return (bucket_name, object_name)


def bq_insert_stream(config, tablename, iter_q):
    """Insert records from an IterableQueue into BigQuery.

    Arguments:     table_name {str} -- The name of the table into which to
    stream rows.     iter_q {IterableQueue} -- The IterableQueue to read
    from.  Returns:     google.cloud.bigquery.table.RowIterator -- Result
    of the query. Since this is an INSERT query, this will always be empty
    if it succeeded.  Raises:     google.cloud.exceptions.GoogleCloudError
    –- If the job failed.     concurrent.futures.TimeoutError –- If the
    job did not complete in the given timeout.
    """
    bq = get_bq_client(config)
    LOG.info("Starting BQ insert stream to {}...".format(tablename))
    batch = []
    batch_size = config["BQ_BATCH_WRITE_SIZE"]

    def flush_to_bq():
        try:
            if config["DRY_RUN"]:
                LOG.info(
                    "DRY RUN: Would flush to BQ {} the following...\n{}".format(
                        tablename, batch))
            else:
                insert_errors = bq.insert_rows_json(tablename, batch)
                if insert_errors:
                    LOG.info("Insert errors! {}".format(
                        [x for x in flatten(insert_errors)]))
        except BadRequest as e:
            if not e.message.endswith("No rows present in the request."):
                raise e
        finally:
            batch.clear()

    for row in iter_q:
        batch.append(row)
        if len(batch) > batch_size:
            flush_to_bq()
    # finally, insert the remainder
    flush_to_bq()

    LOG.info("Finished BQ insert stream to {}.".format(tablename))


def flatten(iterable, iter_types=(list, tuple)):
    """Flattens nested iterables into a flat iterable."""
    for i in iterable:
        if isinstance(i, iter_types):
            for j in flatten(i, iter_types):
                yield j
        else:
            yield i


class IterableQueue(Queue):

    _sentinel = object()

    def __iter__(self):
        return iter(self.get, self._sentinel)

    def close(self):
        self.put(self._sentinel)
