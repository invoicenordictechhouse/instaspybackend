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
  name = "terraform-network"
}

resource "google_bigquery_dataset" "login_dataset" {
  dataset_id = "instaspy_login_dataset"
  project    = var.project_id
  location   = "EU"

}

resource "google_bigquery_dataset" "ads_dataset" {
  dataset_id = "instaspy_ads_dataset"
  project    = var.project_id
  location   = "EU"

}

