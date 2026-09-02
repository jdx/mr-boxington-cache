terraform {
  required_version = ">= 1.8.0"

  backend "azurerm" {
    resource_group_name  = "mbx-cache-terraform"
    storage_account_name = "mbxcachetfstate"
    container_name       = "tfstate"
    key                  = "production.tfstate"
    use_azuread_auth     = true
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.0"
    }
  }
}

provider "azurerm" {
  features {}
}
