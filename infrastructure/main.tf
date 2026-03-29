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

  
  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 104857600
    
    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = azurerm_storage_data_lake_gen2_filesystem.bronze.name
      storage_account_id  = azurerm_storage_account.datalake.id
    }
  }
}

# 5. Create a Storage Account (The Data Lake)
resource "azurerm_storage_account" "datalake" {
  name                     = "stweatherstorearchit"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true 
}

# 6. Create a Container for the Bronze Data
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}
