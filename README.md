# Google Cloud Storage Smart Archiver
This is a Python-based implementation of a Google Cloud Storage (GCS) Smart Archiver. A smart archiver uses object access records to make decisions about whether to move objects to "colder" storage classes, like Nearline and Coldline, based on information about how the object has been accessed.

This archiver implementation has some limitations, namely that it will not work with publicly accessible objects, and it is not advisable to use with large numbers of very small objects.

The bulk of the work is done by Cloud Audit Logging, Stackdriver Logging, and most importantly, BigQuery. Cloud Audit Logging is used to capture events when objects are created or accessed. Stackdriver Logging is used to filter these logs and export them to BigQuery. Then BigQuery is used to analyze access records, as well as records of when the archiver moved objects to other storage classes.

# Installation

This package is not yet on PyPi at the time of this writing, so you'll need to clone or download this repository. From there, it can be installed with pip. For example:

```shell
git clone THIS_REPOSITORY_URL
cd REPOSITORY_NAME
pip install .
```

For development, if you want to be able to make modifications to the repo and run them locally, install with `pip install -e .`.

# Usage

For basic usage, run with the `-h` or `--help` switch.

```
$ gcs_sa --help
usage: gcs_sa [-h] [-c CONFIG_FILE] [-l LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config_file CONFIG_FILE
                        Path to the configuration file to use.
  -l LOG_LEVEL, --log_level LOG_LEVEL
                        Set log level. Overrides configuration.
```

The bulk of control over the program is done through the configuration file. Review the comments in [`default.cfg`](/default.cfg) for more information about configuration items.

# Module Overview

## gcs_sa

Main module. Entry point for the script is here, under `gcs_sa.__main__.py:main`. Main program control and task parallelism is handled in `__main__.py` also.

### gcs_sa.args

Handles parsing and validating arguments, and making them available.

### gcs_sa.config

Handles parsing and validating configuration files, and making them available.

### gcs_sa.constants

Program-wide constants.

### gcs_sa.logging

Utility functions for validating and setting log levels. Doesn't replace the standard library logging module.

### gcs_sa.utils

Very general utilities, such as decorators.

### gcs_sa.actions

Actions that the program should take; specifically, these are actions relating to archiving or de-archiving data.

### gcs_sa.bq

A module containing BigQuery-related code for the program.

### gcs_sa.bq.client

Handles building BigQuery client objects and making them available.

### gcs_sa.bq.output

A class used to manage a thread-safe output stream to BigQuery, where rows are inserted into a table via a streaming insert.

### gcs_sa.bq.queries

Query text and functions to compose them. The rows produced by these queries are fed into decisions to take actions.

### gcs_sa.bq.tables

Table definitions and a Table class with a few common functions required by the program.

### gcs_sa.bq.utils

Utilities specific to BigQuery or the data used in BigQuery, such as converting string formats for object resource representations.

### gcs_sa.decisions

Decisions that need to be made by the program based on rows read out of BigQuery. These are a good place to insert customization.

### gcs_sa.gcs

A module containing Google Cloud Storage (GCS)-related code for the program.

### gcs_sa.gcs.client

Handles building GCS client objects and making them available.

# License
Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

# Copyright
Copyright 2019 Google LLC.