output "dataflow_bucket" {
  value = google_storage_bucket.dataflow_staging.name
}

output "pubsub_topic" {
  value = google_pubsub_topic.events.name
}

output "composer_bucket" {
  value = google_composer_environment.composer.config[0].dag_gcs_prefix
}