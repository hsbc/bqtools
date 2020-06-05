variable "project" {
  type = string
  description="The project to create the bqsync test datasets in"
}
variable "eubqkey" {
  type = string
  description = "A crypto key that can be used for big quert in eu region"
  default = null
}

locals {
  eubqkey = var.eubqkey != null ? "project/${var.project}/locations/europe-west2/keyrings/bigQuery/cryptokeys/bigQuery" : var.eubqkey
  tableslist  = [{
      table_id          = "gcp_billing_export_v1_DUMMY_ACCOUNT1",
      schema            = "billing_export_v1.json",
      description       = null
      time_partitioning  = {
        type                     = "DAY",
        field                    = null,
        require_partition_filter = false,
        expiration_ms            = null,
      },
      expiration_time = null
      clustering      = [],
      labels          = {
      },
      encryption_configuration = null
    },
    {
      table_id          = "gcp_billing_export_DUMMY_ACCOUNT1",
      schema            = "billing_export.json",
      description       = null
      time_partitioning  = {
        type                     = "DAY",
        field                    = null,
        require_partition_filter = false,
        expiration_ms            = null,
      },
      expiration_time = null
      clustering      = [],
      labels          = {
      },
      encryption_configuration = null
    },
    {
      table_id          = "gall_data_types_dp",
      schema            = "billing_export.json",
      description       = null
      time_partitioning  = {
        type                     = "DAY",
        field                    = null,
        require_partition_filter = false,
        expiration_ms            = null,
      },
      expiration_time = null
      clustering      = [],
      labels          = {
      },
      encryption_configuration = null
    },
    {
      table_id          = "gall_data_types_dp",
      schema            = "billing_export.json",
      description       = null
      time_partitioning  = {
        type                     = "DAY",
        field                    = null,
        require_partition_filter = false,
        expiration_ms            = null,
      },
      expiration_time = null
      clustering      = [],
      labels          = {
      },
      encryption_configuration = {
        kms_key_name = ""
      }
    }
  ]
  tables = { for table in local.tablelist : table["table_id"] => table }
}