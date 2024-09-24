terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project                        = "annular-net-436607-t0"
  region                         = var.region
  impersonate_service_account    = "terraform-sa@annular-net-436607-t0.iam.gserviceaccount.com"
}

resource "google_compute_network" "vpc_network" {
    name = "terraform-network"
}

