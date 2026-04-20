variable "project" {
  description = "GCP project ID"
  type        = string
  default     = "theta-carving-486822-c0"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Project location"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name"
  type        = string
  default     = "de-dota2"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "main_metadata"
}