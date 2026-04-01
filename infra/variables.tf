variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "composer_node_count" {
  description = "Number of nodes in Composer environment"
  type        = number
  default     = 3
}