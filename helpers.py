import logging
from datetime import datetime, timezone
from queue import Queue
from threading import Lock

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
            LOG.info("Error parsing config tokens: %s", tokens)
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
    """Get a BigQuery client. 

    Returns:     google.cloud.bigquery.Client -- A BigQuery client.
    """
    if 'bq' not in clients:
        clients['bq'] = bigquery.Client(
            project=config["BQ_JOB_PROJECT"] if "BQ_JOB_PROJECT" in
            config else config["PROJECT"])
    return clients['bq']


def get_gcs_client(config):
    """Get a GCS client. 

    Returns:     google.cloud.storage.Client -- A GCS client.
    """
    if 'gcs' not in clients:
        clients['gcs'] = storage.Client(project=config["PROJECT"])
    return clients['gcs']


def get_bucket_and_object(resource_name):
    """Given an audit log resourceName, parse out the bucket name and object
    path within the bucket.

    Returns:     (str, str) -- ([bucket name], [object name])
    """
    pathparts = resource_name.split("buckets/", 1)[1].split("/", 1)

    bucket_name = pathparts[0]
    object_name = pathparts[1].split("objects/", 1)[1]

    return (bucket_name, object_name)


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

    LOG.info("Creating table %s if not found.", name)

    querytext = """
        CREATE TABLE IF NOT EXISTS `{}` (
        {}
        )""".format(name, schema)

    LOG.debug("Query: \n{}".format(querytext))

    query_job = bq.query(querytext)
    return query_job.result()


class BigQueryOutput():

    def __init__(self, config, tablename, schema):
        self.lock = Lock()
        self.client = get_bq_client(config)
        self.rows = list()
        self.tablename = tablename
        self.batch_size = int(config["BQ_BATCH_WRITE_SIZE"])
        self.insert_count = 0
        initialize_table(config, tablename, schema)

    def put(self, row):
        self.rows.append(row)
        self.lock.acquire()
        if len(self.rows) >= self.batch_size:
            self.flush()
        self.lock.release()

    def flush(self):
        LOG.debug("Flushing %s rows to %s.", len(self.rows), self.tablename)
        try:
            insert_errors = self.client.insert_rows_json(
                self.tablename, self.rows)
            if insert_errors:
                LOG.error("Insert errors! %s",
                          [x for x in flatten(insert_errors)])
        except BadRequest as error:
            if not error.message.endswith("No rows present in the request."):
                LOG.error("Insert error! %s", error.message)
                raise error
        finally:
            self.insert_count += len(self.rows)
            self.rows = list()

    def stats(self):
        return "{} rows inserted into {}".format(self.insert_count,
                                                 self.tablename)


def flatten(iterable, iter_types=(list, tuple)):
    """Flattens nested iterables into a flat iterable."""
    for i in iterable:
        if isinstance(i, iter_types):
            for j in flatten(i, iter_types):
                yield j
        else:
            yield i
