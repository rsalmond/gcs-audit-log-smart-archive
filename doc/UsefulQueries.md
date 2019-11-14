# Useful queries to monitor the Smart Archiver

You can use the `objects_moved` table, in addition to log output, to monitor the work of the Smart Archiver.

## Finding out how much data was moved

A simple aggregation of moves with a WHERE clause specifying the conditions you're looking for will suffice. For example, this looks for all objects moved to NEARLINE by the Smart Archiver over all time:

```sql
SELECT
  ROUND(SUM(size) / 1000 / 1000 / 1000 , 2) as gigabytes_moved
FROM
  `myproject.mydataset.objects_moved`
WHERE
  storageClass = "NEARLINE"
```

## Finding out processing rate by the hour

You can use timestamp truncation and grouping to gather hourly statistics about objects rewritten by the Smart Archiver:

```sql
SELECT
  TIMESTAMP_TRUNC(moveTimestamp, hour) AS hour_moved,
  COUNT(resourceName) as objects_moved,
  ROUND(SUM(size) / 1000 / 1000 / 1000 , 2) as gigabytes_moved
FROM
  `myproject.mydataset.objects_moved`
WHERE
  storageClass = "NEARLINE"
GROUP BY hour_moved
ORDER BY hour_moved DESC
```

## Seeing what the Archiver sees

In debug mode, the Smart Archiver will emit all SQL queries it makes. However, these can be complex. You can get an idea of what input went into a Smart Archiver decision by looking at (and filtering) the results of this query, which should be easier to reason about:

```sql
-- Grouped by resourceName, get the latest access and the number of accesses in a recent window.
SELECT
  protopayload_auditlog.resourceName AS resourceName,
  MAX(timestamp) AS lastAccess,
  COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, DAY) <= 3) AS recent_access_count
FROM
  `myproject.mydataset.cloudaudit_googleapis_com_data_access_*`
WHERE
  -- Limit the query to the last 30 days.
  _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND FORMAT_DATE("%Y%m%d", CURRENT_DATE())
GROUP BY
  resourceName
```

Be sure to change the `INTERVAL` value on the date range as well as the day threshold in the `COUNTIF` function to reflect your configuration.

# License
Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

# Copyright
Copyright 2019 Google LLC.