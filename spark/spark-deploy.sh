#!/bin/bash
# Deploy and run PySpark crypto transformations using Dataproc Serverless

# Configuration
PROJECT_ID="data-pipelines-450717"
REGION="us-central1"
TEMP_BUCKET="${PROJECT_ID}-spark-temp"
SCRIPTS_BUCKET="${PROJECT_ID}-spark-scripts"

# Create temporary bucket if it doesn't exist
if ! gsutil ls -b "gs://${TEMP_BUCKET}" &>/dev/null; then
    echo "Creating temporary bucket: ${TEMP_BUCKET}"
    gsutil mb -l ${REGION} "gs://${TEMP_BUCKET}"
fi

# Create scripts bucket if it doesn't exist
if ! gsutil ls -b "gs://${SCRIPTS_BUCKET}" &>/dev/null; then
    echo "Creating scripts bucket: ${SCRIPTS_BUCKET}"
    gsutil mb -l ${REGION} "gs://${SCRIPTS_BUCKET}"
fi

# Upload the PySpark script to GCS
echo "Uploading PySpark script to GCS"
gsutil cp crypto-transformations.py "gs://${SCRIPTS_BUCKET}/crypto-transformations.py"

# Submit the serverless Spark batch
echo "Submitting serverless Spark batch"
gcloud dataproc batches submit pyspark "gs://${SCRIPTS_BUCKET}/crypto-transformations.py" \
    --region="${REGION}" \
    --jars="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar" \
    --project="${PROJECT_ID}" \
    --version="1.1" \
    --properties="spark.executor.memory=4g" \
    --service-account="$(gcloud iam service-accounts list --filter="email ~ bigquery" --format="value(email)" --limit=1)"

echo "Serverless job submitted successfully"
echo "Results will be written to tables in the ${PROJECT_ID}.coinbase_data dataset"
