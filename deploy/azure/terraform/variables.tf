variable "location" {
  description = "Azure region for compute and blob storage."
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Azure resource group name."
  type        = string
  default     = "mbx-cache-production"
}

variable "vm_size" {
  description = "Azure VM SKU for the cache API and PostgreSQL."
  type        = string
  default     = "Standard_B2s"
}

variable "admin_username" {
  description = "Linux administrator used for bootstrap deployments."
  type        = string
  default     = "azureuser"
}

variable "public_ssh_key" {
  description = "Public SSH key installed on the VM."
  type        = string
  sensitive   = true
}

variable "admin_source_cidr" {
  description = "Temporary public SSH source CIDR used only during initial bootstrap. Remove it after Tailscale enrollment."
  type        = string
  default     = null
  nullable    = true
}

variable "storage_account_name" {
  description = "Globally unique lowercase Azure Storage account name."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name must contain 3-24 lowercase letters and digits."
  }
}

variable "storage_container_name" {
  description = "Private Blob Storage container for cache objects."
  type        = string
  default     = "cache"
}

variable "storage_replication_type" {
  description = "Azure Storage replication type. ZRS tolerates a zone failure within the region."
  type        = string
  default     = "ZRS"
}

variable "blob_retention_days" {
  description = "Days after creation before cache blobs are deleted."
  type        = number
  default     = 30
}
