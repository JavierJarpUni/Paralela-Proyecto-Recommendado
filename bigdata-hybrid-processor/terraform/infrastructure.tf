# infrastructure.tf
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

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

# Cloud Storage Buckets
resource "google_storage_bucket" "processing_results" {
  name     = "${var.project_id}-bigdata-processing-results"
  location = var.region
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# Dataproc Cluster
resource "google_dataproc_cluster" "bigdata_cluster" {
  name   = "bigdata-cluster"
  region = var.region
  
  cluster_config {
    staging_bucket = google_storage_bucket.processing_results.name
    
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }
    
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
      
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }
    
    preemptible_worker_config {
      num_instances = 2
    }
    
    software_config {
      image_version = "2.1-debian11"
      
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
    
    gce_cluster_config {
      zone = "${var.region}-a"
      
      shielded_instance_config {
        enable_secure_boot = true
      }
    }
    
    initialization_action {
      script      = "gs://${google_storage_bucket.processing_results.name}/init-scripts/setup-spark.sh"
      timeout_sec = 300
    }
  }
}

# Cloud Run Service for Actor System
resource "google_cloud_run_service" "actor_system" {
  name     = "bigdata-actor-system"
  location = var.region
  
  template {
    spec {
      containers {
        image = "gcr.io/${var.project_id}/bigdata-actor-system:latest"
        
        ports {
          container_port = 8080
        }
        
        resources {
          limits = {
            cpu    = "2000m"
            memory = "4Gi"
          }
        }
        
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        
        env {
          name  = "DATAPROC_CLUSTER"
          value = google_dataproc_cluster.bigdata_cluster.name
        }
      }
      
      container_concurrency = 10
      timeout_seconds      = 300
    }
    
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "10"
        "run.googleapis.com/cpu-throttling" = "false"
      }
    }
  }
  
  traffic {
    percent         = 100
    latest_revision = true
  }
}

# Cloud Function for Metal GPU Processing
resource "google_cloudfunctions_function" "metal_processor" {
  name        = "metal-gpu-processor"
  description = "GPU processing using Metal on macOS"
  runtime     = "python39"
  
  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.processing_results.name
  source_archive_object = "cloud-functions/metal-processor.zip"
  trigger {
    http_trigger {
      security_level = "SECURE_ALWAYS"
    }
  }
  entry_point = "metal_gpu_processor"
  
  environment_variables = {
    GOOGLE_CLOUD_PROJECT = var.project_id
  }
}

# Cloud Function for API Gateway
resource "google_cloudfunctions_function" "api_gateway" {
  name        = "bigdata-api-gateway"
  description = "Main API Gateway for BigData processing"
  runtime     = "python39"
  
  available_memory_mb   = 1024
  source_archive_bucket = google_storage_bucket.processing_results.name
  source_archive_object = "cloud-functions/api-gateway.zip"
  
  trigger {
    http_trigger {
      security_level = "SECURE_ALWAYS"
    }
  }
  
  entry_point = "bigdata_api_gateway"
  
  environment_variables = {
    GOOGLE_CLOUD_PROJECT = var.project_id
    ACTOR_SYSTEM_URL     = google_cloud_run_service.actor_system.status[0].url
  }
}

# IAM Permissions
resource "google_project_iam_member" "cloud_run_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_cloud_run_service.actor_system.template[0].spec[0].service_account_name}"
}

resource "google_project_iam_member" "cloud_run_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_cloud_run_service.actor_system.template[0].spec[0].service_account_name}"
}

# Outputs
output "api_gateway_url" {
  value = google_cloudfunctions_function.api_gateway.https_trigger_url
}

output "processing_bucket" {
  value = google_storage_bucket.processing_results.name
}

output "dataproc_cluster" {
  value = google_dataproc_cluster.bigdata_cluster.name
}