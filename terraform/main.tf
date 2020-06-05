# create source dataets for testing
provider "google" {
  project = var.project
}

resource "google_bigquery_dataset" "test_source_eu" {
  project = var.project
  dataset_id = "test_eu"
  location = "EU"
}
resource "google_bigquery_dataset" "test_europe_west2" {
  project = var.project
  dataset_id = "test_europe_west2"
  location = "europe-west2"
}
resource "google_bigquery_dataset" "test_europe_west2_dkms" {
  dataset_id = "test_europe_west2"
  location = "europe-west2"
  default_encryption_configuration {
    kms_key_name = "project/${var.project}/locations/europe-west2/keyrings/bigQuery/cryptokeys/bigQuery"
  }
}

resource "google_bigquery_table" "test_source_eu" {
  for_each        = local.tablelist
  dataset_id      = google_bigquery_dataset.test_source_eu.dataset_id
  friendly_name   = each.key
  table_id        = each.key
  labels          = each.value["labels"]
  schema          = file(each.value["schema"])
  clustering      = each.value["clustering"]
  expiration_time = each.value["expiration_time"]
  project         = var.project
  description     = each.value["description"]

  dynamic "time_partitioning" {
    for_each = each.value["time_partitioning"] != null ? [each.value["time_partitioning"]] : []
    content {
      type                     = time_partitioning.value["type"]
      # expiration_ms            = time_partitioning.value["expiration_ms"]
      field                    = time_partitioning.value["field"]
      require_partition_filter = time_partitioning.value["require_partition_filter"]
    }
  }
  dynamic "encryption_configuration" {
      for_each = each.value["encryption_configuration"] != null ?  [each.value["encryption_configuration"]] : []
      content {
        kms_key_name = encryption_configuration.value["kms_key_name"]
      }
  }
}
