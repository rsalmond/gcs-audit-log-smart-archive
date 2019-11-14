# Definitions of tables used by the Smart Archiver
Here, you can find out about the BigQuery tables used by the Smart Archiver.

## cloudaudit_googleapis_com_data_access_*
This table is created by the Stackdriver sink of Cloud Audit Logs of data access. Its schema is quite complex and detailed. Primarily, the Smart Archiver relies upon two columns in this table:


  - `timestamp`: TIMESTAMP The time of the event.
  - `protopayload_auditlog.resourceName`: STRING The resource the event is about.

## objects_excluded

The schema of this table is as follows:

  - `resourceName`: STRING The name of the resource to exclude.

You can manually append object names to this table for objects you with the Smart Archiver to ignore. Otherwise, objects will be appended to this table when they are not found during Smart Archiver processing. This arises from the case where an object appears in the create/access log and a decision is made to rewrite it, but the object has since been deleted. 

If the object is created again later, it will continue to be excluded. If this is a rare event, you can DELETE the object from this table. Otherwise, you may want to DROP this table between runs of the Smart Archiver. It serves only to optimize query and processing time.

## objects_moved

The schema of this table is as follows:

  - resourceName STRING	The name of the (object) resource rewritten.
  - storageClass STRING	The storage class the object was rewritten _to_. 
  - size INTEGER The size of the object when rewritten.
  - blobCreatedTimeWhenMoved TIMESTAMP The pre-rewrite created time of the object. Rewriting creates a new object, so other than this column this information is likely lost.
  - moveTimestamp TIMESTAMP The time of the rewrite.

This table is used to track work already done by the Smart Archiver. This is key to enable durability in the event of crashes or timeouts. Queries of the access log still include objects in this table, but also include the most recent move in this table if one is found. This enables the Archiver to quickly rule out new rewrites without accessing each object; specifically, if the "decision" logic would rewrite the object to the storage class it was most recently written to, no action will be taken.

# License
Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

# Copyright
Copyright 2019 Google LLC.