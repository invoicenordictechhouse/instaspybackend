terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project                     = var.project_id
  region                      = var.region
  impersonate_service_account = var.service_account
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network-${var.environment}"
}

resource "google_bigquery_dataset" "login_dataset" {
  dataset_id = "instaspy_login_dataset_${var.environment}"
  project    = var.project_id
  location   = "EU"
}

resource "google_bigquery_dataset" "ads_dataset" {
  dataset_id = "instaspy_ads_dataset_${var.environment}"
  project    = var.project_id
  location   = "EU"
}

resource "google_service_account" "data_ingestion_sa" {
  account_id   = "data-ingestion-sa"
  display_name = "Service Account for Data Ingestion"
}

resource "google_service_account" "dbt_sa" {
  account_id   = "dbt-sa"
  display_name = "Service Account for dbt"
}

resource "google_service_account" "backend_sa" {
  account_id   = "backend-sa"
  display_name = "Service Account for Backend"
}
