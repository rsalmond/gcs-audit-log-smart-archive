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

A runnable script `gcs_sa` should be on your path.

# Usage

For basic usage, run with the `-h` or `--help` switch.

```
$ gcs_sa --help
```

The bulk of control over the program is done through the configuration file. Review the comments in [`default.cfg`](/default.cfg) for more information about configuration items.

# License
Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

# Copyright
Copyright 2019 Google LLC.