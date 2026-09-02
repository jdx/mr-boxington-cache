output "server_ipv4" {
  value = azurerm_public_ip.cache.ip_address
}

output "cache_url" {
  value = "https://cache.jdx.dev"
}

output "resource_group_name" {
  value = azurerm_resource_group.cache.name
}

output "vm_name" {
  value = azurerm_linux_virtual_machine.cache.name
}

output "azure_storage_account" {
  value = azurerm_storage_account.cache.name
}

output "azure_storage_container" {
  value = azurerm_storage_container.cache.name
}

output "managed_identity_principal_id" {
  value = azurerm_linux_virtual_machine.cache.identity[0].principal_id
}
