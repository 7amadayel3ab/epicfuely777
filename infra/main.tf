terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Dataflow staging bucket
resource "google_storage_bucket" "dataflow_staging" {
  name          = "${var.project_id}-dataflow-staging"
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}

# Pub/Sub topic for raw events
resource "google_pubsub_topic" "events" {
  name = "ecommerce-events"
}

# Subscription for streaming job
resource "google_pubsub_subscription" "events_sub" {
  name  = "ecommerce-events-sub"
  topic = google_pubsub_topic.events.id
  ack_deadline_seconds = 20
}

# BigQuery datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

# Cloud Composer environment (takes ~15-20 minutes)
resource "google_composer_environment" "composer" {
  name   = "ecommerce-composer"
  region = var.region
  config {
    node_count = var.composer_node_count
    node_config {
      zone         = "${var.region}-a"
      machine_type = "n1-standard-2"
    }
    software_config {
      image_version = "composer-2.0.0-airflow-2.2.3"
    }
  }
}

# Dataproc cluster for batch jobs
resource "google_dataproc_cluster" "batch" {
  name   = "ecommerce-batch-cluster"
  region = var.region
  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
    }
  }
}