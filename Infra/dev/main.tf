terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.78.0"
    }
  }

  backend "gcs" {
    bucket = "annular-net-436607-t0-terraform-state-dev"
    prefix = "terraform/state/dev"
  }
}

provider "google" {
  project                     = var.project_id
  region                      = var.region
  impersonate_service_account = var.service_account
}

# VPC Network
resource "google_compute_network" "vpc_network" {
  name                    = "${var.environment}-vpc-network"
  auto_create_subnetworks = false
  project                 = var.project_id
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "${var.environment}-subnet"
  ip_cidr_range = "10.0.0.0/16"
  region        = var.region
  network       = google_compute_network.vpc_network.name
  project       = var.project_id
}

# Private Service Access
# Reserve an IP range for Google-managed services
resource "google_compute_global_address" "private_ip_range" {
  name          = "${var.environment}-private-ip-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc_network.id
  project       = var.project_id
}

# Establish Private Service Access connection
resource "google_service_networking_connection" "private_service_connection" {
  network                 = google_compute_network.vpc_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_range.name]
}

# Cloud SQL Instance
resource "google_sql_database_instance" "db_instance" {
  name             = "${var.environment}-sql-instance"
  database_version = "POSTGRES_13"
  region           = var.region
  project          = var.project_id

  settings {
    tier = "db-custom-1-3840"

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc_network.id
    }
  }

  depends_on = [
    google_service_networking_connection.private_service_connection
  ]
}

# Firewall Rules
# Allow MySQL or PostgreSQL traffic (adjust port as needed)
resource "google_compute_firewall" "allow_sql" {
  name    = "${var.environment}-allow-sql"
  network = google_compute_network.vpc_network.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["5432"] # PostgreSQL default port
  }

  source_ranges = ["10.0.0.0/16"]
  direction     = "INGRESS"
}
# Compute Engine VM Instance
resource "google_compute_instance" "sql_client_vm" {
  name         = "${var.environment}-sql-client-vm"
  machine_type = "e2-micro"
  zone         = var.zone
  project      = var.project_id

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 10
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = google_compute_network.vpc_network.name
    subnetwork = google_compute_subnetwork.subnet.name

    # Assign an external IP
    access_config {
      # The presence of this block assigns an external IP to the VM
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    # Update packages and install PostgreSQL client
    apt-get update
    apt-get install -y postgresql-client

    # Optional: Install other necessary tools
  EOF

  service_account {
    email  = var.service_account
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  tags = ["sql-client"]
}

# Firewall Rule to Allow SSH from Your IP
resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.environment}-allow-ssh"
  network = google_compute_network.vpc_network.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["155.4.199.54/32"] # Replace with your public IP
  direction     = "INGRESS"

  target_tags = ["sql-client"]
}

resource "google_service_account" "data_ingestion_sa" {
  account_id   = "${var.environment}-data-ingestion-sa"
  display_name = "Data Ingestion SA (${var.environment})"
  project      = var.project_id
}

resource "google_service_account" "dbt_sa" {
  account_id   = "${var.environment}-dbt-sa"
  display_name = "dbt SA (${var.environment})"
  project      = var.project_id
}

resource "google_service_account" "backend_sa" {
  account_id   = "${var.environment}-backend-sa"
  display_name = "Backend SA (${var.environment})"
  project      = var.project_id
}

resource "google_iam_workload_identity_pool" "github_actions_pool" {
  project                   = var.project_id
  workload_identity_pool_id = "${var.environment}-github-actions-pool"
  display_name              = "GitHub Pool (${var.environment})"
}

resource "google_iam_workload_identity_pool_provider" "github_actions_provider" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_actions_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "${var.environment}-github-actions-provider"
  display_name                       = "GitHub Provider (${var.environment})"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.sub.startsWith('repo:invoicenordictechhouse/instaspybackend:ref:refs/heads/${var.environment}')"
}

# Firewall Rule to Allow SSH from IAP
resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "${var.environment}-allow-iap-ssh"
  network = google_compute_network.vpc_network.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  direction     = "INGRESS"

  target_tags = ["sql-client"]
}