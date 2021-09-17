require('dotenv').config();
const yargs = require('yargs');
const fs = require('fs');

const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');

const bigquery = new BigQuery();
const storage = new Storage();

const args = yargs(process.argv.slice(2)).parse();
const {datasetId, tableId, bucketName, fileName, schema, partition} = args;

async function loadCSVFromGCSAppend() {
  if (!datasetId || !tableId || !bucketName || !fileName) {
    throw 'One or more arguments are missing from the command: must include --datasetId, --tableId, --bucketName, --fileName.'
  }
  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 0,
    writeDisposition: 'WRITE_APPEND',
    location: 'europe-west2',
  };
  if (schema) {
    let rawSchema = fs.readFileSync('schema.json');
    let schemaData = JSON.parse(rawSchema);
    metadata.schema = {
      fields: schemaData.schema.fields
    };
    if (partition && partition == 'time') {
      metadata.timePartitioning = schemaData.timePartitioning;
    }
    if (partition && partition == 'range') {
      metadata.rangePartitioning = schemaData.rangePartitioning;
    }
  }

  const dataset = bigquery.dataset(datasetId);
  const table = dataset.table(tableId);

  const [job] = await table.load(storage.bucket(bucketName).file(fileName), metadata);
  console.log(`Job ${job.id} completed.`);

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}
loadCSVFromGCSAppend();