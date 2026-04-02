markdown
# Real‑Time & Batch E‑Commerce Data Pipeline on Google Cloud Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production‑ready data pipeline that ingests e‑commerce events in real time, stores them in BigQuery, runs daily batch aggregations with Spark, orchestrates everything with Airflow, and validates data quality with Great Expectations.

---

## Table of Contents

- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [1. Infrastructure](#1-infrastructure)
  - [2. Streaming Pipeline](#2-streaming-pipeline)
  - [3. Batch Pipeline](#3-batch-pipeline)
  - [4. Orchestration (Airflow)](#4-orchestration-airflow)
  - [5. Data Quality (Great Expectations)](#5-data-quality-great-expectations)
- [Running the Pipeline](#running-the-pipeline)
- [Results](#results)
- [Cleanup](#cleanup)
- [Future Enhancements](#future-enhancements)
- [License](#license)

---

## Architecture

![Architecture](architecture.png) <!-- You can add a diagram later -->

The pipeline consists of the following components:

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Ingestion** | Pub/Sub | Collects raw JSON events from e‑commerce platform |
| **Stream Processing** | Dataflow (Apache Beam Python) | Reads Pub/Sub messages, parses JSON, writes to BigQuery |
| **Storage** | BigQuery | Raw events table (`raw.events`) and aggregated analytics table (`analytics.daily_sales`) |
| **Batch Processing** | Dataproc (Spark Scala) | Reads raw events, aggregates daily order totals and revenue |
| **Orchestration** | Cloud Composer (Airflow) | Schedules the batch job daily and runs data quality checks |
| **Data Quality** | Great Expectations | Validates raw events (e.g., non‑null columns) |
| **Infrastructure** | Terraform / gcloud CLI | Provisions all GCP resources |

---

## Technologies

- **Python 3.10** – Apache Beam streaming pipeline  
- **Scala 2.12** – Spark batch job  
- **SQL** – dbt (optional) and BigQuery queries  
- **Google Cloud Platform**  
  - Pub/Sub, Dataflow, BigQuery, Dataproc, Cloud Composer, Cloud Storage  
- **Terraform** – Infrastructure as code  
- **Great Expectations** – Data quality validation  
- **Airflow** – Workflow orchestration  
- **sbt** – Scala build tool  

---

## Project Structure
epicfuely777/
├── infra/ # Terraform configuration (optional)
├── streaming/ # Python Apache Beam pipeline
│ ├── main.py
│ ├── transforms.py
│ └── requirements.txt
├── batch/ # Scala Spark job
│ ├── build.sbt
│ └── src/main/scala/com/ecommerce/DailyAggregator.scala
├── dags/ # Airflow DAG
│ └── ecommerce_dag.py
├── great_expectations/ # Data quality suite
│ ├── expectations/
│ └── great_expectations.yml
├── .github/workflows/ # (Optional) CI/CD
└── README.md

text

---

## Prerequisites

- A Google Cloud Platform project with **billing enabled** (free trial works).  
- [Google Cloud SDK](https://cloud.google.com/sdk) installed and authenticated (`gcloud auth login`).  
- [Terraform](https://www.terraform.io/downloads) (if using IaC).  
- [sbt](https://www.scala-sbt.org/download.html) (to build Spark JAR).  
- Python 3.10+ (for local testing).  

---

## Setup

### 1. Infrastructure

You can provision resources either with Terraform or manually.

#### Option A: Terraform

```bash
cd infra
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
Option B: Manual gcloud commands
bash
# Bucket
gcloud storage buckets create gs://YOUR_PROJECT_ID-dataflow-staging --location=us-central1 --uniform-bucket-level-access

# Pub/Sub
gcloud pubsub topics create ecommerce-events
gcloud pubsub subscriptions create ecommerce-events-sub --topic ecommerce-events

# BigQuery datasets
bq mk --location=US raw
bq mk --location=US analytics

# Dataproc cluster
gcloud dataproc clusters create ecommerce-batch-cluster \
  --region=us-central1 \
  --master-machine-type=n1-standard-2 \
  --worker-machine-type=n1-standard-2 \
  --num-workers=2 \
  --image-version=2.0-debian10

# Cloud Composer environment
gcloud composer environments create ecommerce-composer \
  --location=us-central1 \
  --image-version=composer-2.16.9-airflow-2.10.5
Replace YOUR_PROJECT_ID with your actual project ID.

2. Streaming Pipeline
The streaming pipeline reads from Pub/Sub and writes to BigQuery.

Deploy the Dataflow job
bash
cd streaming
python main.py \
  --project YOUR_PROJECT_ID \
  --input_subscription projects/YOUR_PROJECT_ID/subscriptions/ecommerce-events-sub \
  --output_table YOUR_PROJECT_ID:raw.events \
  --runner DataflowRunner \
  --region us-central1 \
  --staging_location gs://YOUR_PROJECT_ID-dataflow-staging/staging \
  --temp_location gs://YOUR_PROJECT_ID-dataflow-staging/temp \
  --streaming
Once the job is running, publish a test event:

bash
gcloud pubsub topics publish ecommerce-events --message='{"event_type":"order","user_id":"test","amount":99.99,"timestamp":"2026-04-01T12:00:00Z"}'
Verify in BigQuery:

sql
SELECT * FROM `YOUR_PROJECT_ID.raw.events`;
3. Batch Pipeline
The Spark job aggregates daily order totals.

Build the JAR
bash
cd batch
sbt clean package
Upload the JAR and the BigQuery connector
bash
gsutil cp target/scala-2.12/ecommerce-batch_2.12-1.0.jar gs://YOUR_PROJECT_ID-dataflow-staging/jars/
# Download the BigQuery connector if not already present
wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.32.2/spark-bigquery-with-dependencies_2.12-0.32.2.jar
gsutil cp spark-bigquery-with-dependencies_2.12-0.32.2.jar gs://YOUR_PROJECT_ID-dataflow-staging/jars/
Submit the Spark job
bash
gcloud dataproc jobs submit spark \
  --cluster ecommerce-batch-cluster \
  --region=us-central1 \
  --class com.ecommerce.DailyAggregator \
  --jars gs://YOUR_PROJECT_ID-dataflow-staging/jars/ecommerce-batch_2.12-1.0.jar,gs://YOUR_PROJECT_ID-dataflow-staging/jars/spark-bigquery-with-dependencies_2.12-0.32.2.jar \
  --properties spark.sql.bigquery.temporaryGcsBucket=YOUR_PROJECT_ID-dataflow-staging
After completion, query the aggregated table:

sql
SELECT * FROM `YOUR_PROJECT_ID.analytics.daily_sales`;
4. Orchestration (Airflow)
The Airflow DAG (dags/ecommerce_dag.py) does the following:

Starts the Dataflow streaming job (once per run – adjust as needed).

Runs the Spark batch job daily at 2 AM.

(Bonus) Validates data quality with Great Expectations.

Upload the DAG to Composer
First, find your Composer DAG bucket:

bash
gcloud composer environments describe ecommerce-composer --location=us-central1 --format="value(config.dagGcsPrefix)"
Then copy the DAG:

bash
gsutil cp dags/ecommerce_dag.py gs://YOUR_COMPOSER_BUCKET/dags/
Trigger the DAG
Open the Airflow web UI (from Composer environment page).

Turn on the ecommerce_pipeline DAG.

Trigger a manual run.

Check the Graph View to monitor task status.

5. Data Quality (Great Expectations)
Great Expectations validates that the raw.events table meets defined expectations (e.g., event_type is not null). The validation runs after the Spark job.

Install the Great Expectations provider in Composer
bash
gcloud composer environments update ecommerce-composer \
  --location=us-central1 \
  --update-pypi-package airflow-provider-great-expectations==0.2.0
Upload the Great Expectations data context
bash
gsutil cp -r great_expectations gs://YOUR_PROJECT_ID-dataflow-staging/great_expectations/
The DAG already includes a GreatExpectationsOperator that points to the mounted GCS path.

Customize expectations
Edit great_expectations/expectations/ecommerce/suite.json and re‑upload to GCS.

Running the Pipeline
Ensure the Dataflow streaming job is running (or the DAG will start a new one).

Publish events to the Pub/Sub topic.

The DAG runs daily (or you can trigger it manually).

Check BigQuery for raw and aggregated data.

Monitor Airflow for success/failure of each task.

Results
Example output from analytics.daily_sales after processing four test orders:

text
+------------+--------------+---------------+
|    date    | total_orders | total_revenue |
+------------+--------------+---------------+
| 2026-04-01 |            4 |        399.96 |
+------------+--------------+---------------+
Cleanup
To avoid ongoing costs, delete the resources:

bash
# Cancel Dataflow job (find job ID first)
gcloud dataflow jobs list --region=us-central1
gcloud dataflow jobs cancel JOB_ID --region=us-central1

# Delete Dataproc cluster
gcloud dataproc clusters delete ecommerce-batch-cluster --region=us-central1 --quiet

# Delete Composer environment (if no longer needed)
gcloud composer environments delete ecommerce-composer --location=us-central1 --quiet

# Delete GCS bucket
gsutil rm -r gs://YOUR_PROJECT_ID-dataflow-staging

# Delete BigQuery datasets
bq rm -r -f raw
bq rm -r -f analytics

# Delete Pub/Sub topic and subscription
gcloud pubsub topics delete ecommerce-events
gcloud pubsub subscriptions delete ecommerce-events-sub
If you used Terraform, run terraform destroy in the infra folder.

Future Enhancements
ML anomaly detection – Add BigQuery ML to detect outliers in daily_sales.

CI/CD – Use GitHub Actions to test and deploy code automatically.

Dashboard – Connect Looker Studio to analytics.daily_sales for visual reporting.

Dead‑letter queue – Handle malformed messages in the streaming pipeline.

License
This project is licensed under the MIT License – see the LICENSE file for details.

Contact

email: iqaddah92@gmail.com
phone: +447424117682 (Whatsapp)
       +201030351896 (international phone)
