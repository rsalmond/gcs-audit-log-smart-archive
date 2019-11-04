# Google Cloud Storage Smart Archiver
This is a Python-based implementation of a Google Cloud Storage (GCS) Smart Archiver. A smart archiver uses object access records to make decisions about whether to move objects to "colder" storage classes, like Nearline and Coldline, based on information about how the object has been accessed.

This archiver implementation has some limitations, namely that it will not work with publicly accessible objects, and it is not advisable to use with large numbers of very small objects.

The bulk of the work is done by Cloud Audit Logging, Stackdriver Logging, and most importantly, BigQuery. Cloud Audit Logging is used to capture events when objects are created or accessed. Stackdriver Logging is used to filter these logs and export them to BigQuery. Then BigQuery is used to analyze access records, as well as records of when the archiver moved objects to other storage classes.

# License
Apache 2.0 - See [the LICENSE](blob/master/LICENSE) for more information.

# Copyright
Copyright 2019 Google LLC.