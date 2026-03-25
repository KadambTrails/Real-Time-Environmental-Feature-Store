# 1. Define the Azure Provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# 2. Create a Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-weather-feature-store"
  location = "East US"
}

# 3. Create an Event Hub Namespace (The "Server")
resource "azurerm_eventhub_namespace" "eh_ns" {
  name                = "evh-ns-weather-archit" # Must be unique globally
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
}

# 4. Create the actual Event Hub (The "Topic")
resource "azurerm_eventhub" "weather_eh" {
  name                = "weather-raw-stream"
  namespace_name      = azurerm_eventhub_namespace.eh_ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 2
  message_retention   = 1
}
