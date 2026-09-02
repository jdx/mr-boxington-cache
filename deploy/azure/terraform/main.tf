resource "azurerm_resource_group" "cache" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.tags
}

resource "azurerm_virtual_network" "cache" {
  name                = "mbx-cache-vnet"
  address_space       = ["10.42.0.0/16"]
  location            = azurerm_resource_group.cache.location
  resource_group_name = azurerm_resource_group.cache.name
  tags                = local.tags
}

resource "azurerm_subnet" "cache" {
  name                 = "mbx-cache"
  resource_group_name  = azurerm_resource_group.cache.name
  virtual_network_name = azurerm_virtual_network.cache.name
  address_prefixes     = ["10.42.1.0/24"]

  service_endpoint {
    service = "Microsoft.Storage"
  }
}

resource "azurerm_network_security_group" "cache" {
  name                = "mbx-cache-nsg"
  location            = azurerm_resource_group.cache.location
  resource_group_name = azurerm_resource_group.cache.name
  tags                = local.tags

  security_rule {
    name                       = "http"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "https"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "http3"
    priority                   = 120
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Udp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }

  dynamic "security_rule" {
    for_each = var.admin_source_cidr == null ? [] : [var.admin_source_cidr]
    content {
      name                       = "temporary-admin-ssh"
      priority                   = 130
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = "22"
      source_address_prefix      = security_rule.value
      destination_address_prefix = "*"
    }
  }
}

resource "azurerm_public_ip" "cache" {
  name                = "mbx-cache-ip"
  location            = azurerm_resource_group.cache.location
  resource_group_name = azurerm_resource_group.cache.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags
}

resource "azurerm_network_interface" "cache" {
  name                = "mbx-cache-nic"
  location            = azurerm_resource_group.cache.location
  resource_group_name = azurerm_resource_group.cache.name
  tags                = local.tags

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.cache.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.cache.id
  }
}

resource "azurerm_network_interface_security_group_association" "cache" {
  network_interface_id      = azurerm_network_interface.cache.id
  network_security_group_id = azurerm_network_security_group.cache.id
}

resource "azurerm_linux_virtual_machine" "cache" {
  name                            = "mbx-cache-prod"
  computer_name                   = "mbx-cache-prod"
  resource_group_name             = azurerm_resource_group.cache.name
  location                        = azurerm_resource_group.cache.location
  size                            = var.vm_size
  admin_username                  = var.admin_username
  disable_password_authentication = true
  network_interface_ids           = [azurerm_network_interface.cache.id]
  tags                            = local.tags

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.public_ssh_key
  }

  identity {
    type = "SystemAssigned"
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = 64
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  boot_diagnostics {}

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_account" "cache" {
  name                            = var.storage_account_name
  resource_group_name             = azurerm_resource_group.cache.name
  location                        = azurerm_resource_group.cache.location
  account_tier                    = "Standard"
  account_replication_type        = var.storage_replication_type
  account_kind                    = "StorageV2"
  access_tier                     = "Hot"
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  tags                            = local.tags

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = [azurerm_subnet.cache.id]
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_container" "cache" {
  name                  = var.storage_container_name
  storage_account_id    = azurerm_storage_account.cache.id
  container_access_type = "private"
}

resource "azurerm_role_assignment" "cache_blobs" {
  scope                            = azurerm_storage_account.cache.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azurerm_linux_virtual_machine.cache.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_storage_management_policy" "cache" {
  storage_account_id = azurerm_storage_account.cache.id

  rule {
    name    = "expire-cache-blobs"
    enabled = true

    filters {
      prefix_match = ["${var.storage_container_name}/v1/blobs/"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        delete_after_days_since_creation_greater_than = var.blob_retention_days
      }
    }
  }

  rule {
    name    = "remove-staged-uploads"
    enabled = true

    filters {
      prefix_match = ["${var.storage_container_name}/v1/uploads/"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        delete_after_days_since_creation_greater_than = 1
      }
    }
  }
}

locals {
  tags = {
    application = "mbx-cache"
    environment = "production"
    managed_by  = "terraform"
  }
}
