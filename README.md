# node-load-csv-to-bq
Simple script for loading csv files from GCS into BigQuery tables.

## Setup

This script requires a service account in the relevant project. Create a service account and then create a JSON key file (see the [docs](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries) for a guide on how to do this). Save the resultant JSON file in the root of this project as ```service-account-key.json```.

Run ```node main --datasetId=DATASET_ID --tableId=TABLE_ID --bucketName="gs://YOUR_BUCKET_NAME" --fileName="FILE_NAME" [--schema] [--partition=PARTITION_TYPE]```

```--datasetId```: The name of the existing dataset into which to load the data.
```--tableId```: The name of the table into which to load the data - it does not have to exist.
```--bucketName```: The Cloud Storage URI of the bucket where the data is stored, not including the filename(s). If your bucket is called mybucket, this would be ```gs://mybucket```. **This argument must be surrounded by double quotes (").**
```--fileName```: Supports wildcards as per the [BigQuery client library docs](https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards). **This argument must be surrounded by double quotes (").**
```--schema```: Optional. If the destination table does not already exist, you need to supply a table schema in the file ```schema.json``` (see [Table](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)). The table schema must be a JSON file that contains a ```fields``` entry (see [TableFieldSchema](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema)) and, if required, a ```timePartition``` or ```rangePartition``` entry (see [Time Partitioning](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning), [Range Partitioning](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#rangepartitioning)). 
```--partition```: Optional. Defines the type of partition. Valid arguments are 'time' and 'range'. If this flag is provided, you must provide a configuration; see the above point on ```--schema```.


You can generate a table schema JSON for any table using this command:
```bq show --format=prettyjson PROJECT_NAME:DATASET_NAME.TABLE_NAME > schema.json```
This may serve as an example for you to build the schema you need.