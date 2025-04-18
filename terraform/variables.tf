variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

variable "kafka_broker_count" {
  description = "Number of Kafka broker instances"
  type        = number
  default     = 3
}

variable "kafka_machine_type" {
  description = "Machine type for Kafka brokers"
  type        = string
  default     = "e2-standard-2"  # 2 vCPUs / 8 GB RAM
}

variable "coinbase_api_key" {
  description = "Coinbase API Key"
  type        = string
  sensitive   = true
}

variable "coinbase_api_secret" {
  description = "Coinbase API Secret"
  type        = string
  sensitive   = true
}

variable "kafka_ui_username" {
  description = "Username for Kafka UI basic authentication"
  type        = string
  default     = "admin"
}

variable "kafka_ui_password" {
  description = "Password for Kafka UI basic authentication"
  type        = string
  sensitive   = true
}
