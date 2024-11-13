variable "project_id" {
  description = "The project ID to use"
  type        = string
}

variable "region" {
  description = "The default region to deploy resources in"
  type        = string
  default     = "eu-west-1" # Default to EU West (Ireland)
}

variable "zone" {
  description = "The GCP zone within the region to deploy resources in"
  type        = string
  default     = "europe-west1-d"
}

variable "service_account" {
  description = "Service account to impersonate"
  type        = string

}

variable "environment" {
  description = "Deployment environment (e.g., dev, prod)"
  type        = string
}
