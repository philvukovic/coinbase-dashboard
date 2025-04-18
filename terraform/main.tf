# main.tf - Terraform configuration for Coinbase data pipeline infrastructure

# Configure GCP provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Create a GCP network for the infrastructure
resource "google_compute_network" "kafka_network" {
  name                    = "kafka-network"
  auto_create_subnetworks = false
}

# Create a subnet for the Kafka cluster
resource "google_compute_subnetwork" "kafka_subnet" {
  name          = "kafka-subnet"
  ip_cidr_range = "10.0.0.0/24"
  network       = google_compute_network.kafka_network.id
  region        = var.region
}

# Create a firewall rule to allow internal communication
resource "google_compute_firewall" "kafka_internal" {
  name    = "kafka-internal"
  network = google_compute_network.kafka_network.name

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093", "2181", "22"]
  }

  source_ranges = ["10.0.0.0/24"]
}

# Create a firewall rule to allow external SSH
resource "google_compute_firewall" "kafka_ssh" {
  name    = "kafka-ssh"
  network = google_compute_network.kafka_network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]  # For production, restrict to your IP
}

# Create a service account for the Kafka cluster
resource "google_service_account" "kafka_service_account" {
  account_id   = "kafka-service-account"
  display_name = "Kafka Service Account"
}

# Grant necessary permissions
resource "google_project_iam_member" "kafka_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.kafka_service_account.email}"
}

resource "google_project_iam_member" "kafka_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.kafka_service_account.email}"
}

# Create Kafka broker instances
resource "google_compute_instance" "kafka_broker" {
  count        = var.kafka_broker_count
  name         = "kafka-broker-${count.index}"
  machine_type = var.kafka_machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20  # GB
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y default-jdk
    
    # Download and install Kafka
    wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
    tar -xzf kafka_2.13-3.9.0.tgz
    mv kafka_2.13-3.9.0 /opt/kafka
    
    # Configure Kafka - using instance metadata to get broker ID and IP
    BROKER_ID=${count.index}
    INTERNAL_IP=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" -H "Metadata-Flavor: Google")
    
    cat > /opt/kafka/config/server.properties <<EOL
    broker.id=$BROKER_ID
    listeners=PLAINTEXT://:9092
    advertised.listeners=PLAINTEXT://$INTERNAL_IP:9092
    num.network.threads=3
    num.io.threads=8
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=104857600
    log.dirs=/var/lib/kafka
    num.partitions=3
    num.recovery.threads.per.data.dir=1
    offsets.topic.replication.factor=2
    transaction.state.log.replication.factor=2
    transaction.state.log.min.isr=1
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    zookeeper.connect=${google_compute_instance.zookeeper.network_interface.0.network_ip}:2181
    zookeeper.connection.timeout.ms=18000
    group.initial.rebalance.delay.ms=0
    EOL
    
    # Create log directory
    mkdir -p /var/lib/kafka
    chown -R root:root /var/lib/kafka
    
    # Configure Kafka as a service
    cat > /etc/systemd/system/kafka.service <<EOL
    [Unit]
    Description=Apache Kafka Server
    Documentation=http://kafka.apache.org/documentation.html
    Requires=network.target
    After=network.target

    [Service]
    Type=simple
    User=root
    Environment=JAVA_HOME=/usr/lib/jvm/default-java
    ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
    ExecStop=/opt/kafka/bin/kafka-server-stop.sh
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    EOL
    
    # Start Kafka service
    systemctl enable kafka
    systemctl start kafka
  EOF

  service_account {
    email  = google_service_account.kafka_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["kafka-broker"]

  depends_on = [google_compute_instance.zookeeper]
}

# Create ZooKeeper instance
resource "google_compute_instance" "zookeeper" {
  name         = "zookeeper"
  machine_type = "e2-small"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y default-jdk
    
    # Download and install Kafka (includes ZooKeeper)
    wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
    tar -xzf kafka_2.13-3.9.0.tgz
    mv kafka_2.13-3.9.0 /opt/kafka
    
    # Configure ZooKeeper
    cat > /opt/kafka/config/zookeeper.properties <<EOL
    dataDir=/var/lib/zookeeper
    clientPort=2181
    maxClientCnxns=0
    admin.enableServer=false
    EOL
    
    # Create data directory
    mkdir -p /var/lib/zookeeper
    chown -R root:root /var/lib/zookeeper
    
    # Configure ZooKeeper as a service
    cat > /etc/systemd/system/zookeeper.service <<EOL
    [Unit]
    Description=Apache ZooKeeper Server
    Documentation=http://zookeeper.apache.org
    Requires=network.target
    After=network.target

    [Service]
    Type=simple
    User=root
    Environment=JAVA_HOME=/usr/lib/jvm/default-java
    ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
    ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    EOL
    
    # Start ZooKeeper service
    systemctl enable zookeeper
    systemctl start zookeeper
  EOF

  service_account {
    email  = google_service_account.kafka_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["zookeeper"]
}

# Create a VM for the data ingestion service
resource "google_compute_instance" "ingestion_service" {
  name         = "coinbase-ingestion-service"
  machine_type = "e2-small"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y python3 python3-pip git
    pip3 install kafka-python requests apscheduler
    
    # Create directory and write ingestion script
    mkdir -p /opt/coinbase-ingestion
    
    # Set up environment variables
    cat > /opt/coinbase-ingestion/.env <<EOL
    KAFKA_BOOTSTRAP_SERVERS=${google_compute_instance.kafka_broker[0].network_interface.0.network_ip}:9092
    COINBASE_API_KEY=${var.coinbase_api_key}
    COINBASE_API_SECRET=${var.coinbase_api_secret}
    MARKET_DATA_TOPIC=coinbase-market-data
    QUOTE_CURRENCY=USD
    TOP_ASSETS_LIMIT=20
    BACKFILL_DAYS=365
    EOL
    
    # Set up service
    cat > /etc/systemd/system/coinbase-ingestion.service <<EOL
    [Unit]
    Description=Coinbase Data Ingestion Service
    After=network.target

    [Service]
    Type=simple
    User=root
    WorkingDirectory=/opt/coinbase-ingestion
    EnvironmentFile=/opt/coinbase-ingestion/.env
    ExecStart=/usr/bin/python3 /opt/coinbase-ingestion/coinbase_kafka_ingestion.py --schedule
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    EOL
    
    # Enable service but don't start it yet (need to upload code first)
    systemctl enable coinbase-ingestion
  EOF

  service_account {
    email  = google_service_account.kafka_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["ingestion-service"]
  
  depends_on = [google_compute_instance.kafka_broker]
}

# Cloud Storage bucket for backups
resource "google_storage_bucket" "kafka_backup_bucket" {
  name     = "${var.project_id}-kafka-backups"
  location = var.region
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 30  # days
    }
    action {
      type = "Delete"
    }
  }
}

# Create a VM for Kafka UI
resource "google_compute_instance" "kafka_ui" {
  name         = "kafka-ui"
  machine_type = "e2-small"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y docker.io docker-compose
    systemctl enable docker
    systemctl start docker
    
    # Create docker-compose file for Kafka UI
    mkdir -p /opt/kafka-ui
    cat > /opt/kafka-ui/docker-compose.yml <<EOL
    version: '3'
    services:
      kafka-ui:
        image: provectuslabs/kafka-ui:latest
        container_name: kafka-ui
        ports:
          - "8080:8080"
        environment:
          - KAFKA_CLUSTERS_0_NAME=CoinbaseDataPipeline
          - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=${join(",", [for instance in google_compute_instance.kafka_broker : "${instance.network_interface[0].network_ip}:9092"])}
          - KAFKA_CLUSTERS_0_ZOOKEEPER=${google_compute_instance.zookeeper.network_interface[0].network_ip}:2181
          # Basic authentication
          - AUTH_TYPE=LOGIN_FORM
          - SPRING_SECURITY_USER_NAME=${var.kafka_ui_username}
          - SPRING_SECURITY_USER_PASSWORD=${var.kafka_ui_password}
    EOL
    
    # Start Kafka UI
    cd /opt/kafka-ui
    docker-compose up -d
  EOF

  service_account {
    email  = google_service_account.kafka_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["kafka-ui"]
  
  depends_on = [google_compute_instance.kafka_broker, google_compute_instance.zookeeper]
}

# Create a firewall rule to allow HTTP access to Kafka UI
resource "google_compute_firewall" "kafka_ui_http" {
  name    = "kafka-ui-http"
  network = google_compute_network.kafka_network.name

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]  # For production, restrict to your IP range
  target_tags   = ["kafka-ui"]
}

# Output information
output "kafka_broker_ips" {
  value = google_compute_instance.kafka_broker[*].network_interface[0].access_config[0].nat_ip
}

output "zookeeper_ip" {
  value = google_compute_instance.zookeeper.network_interface[0].access_config[0].nat_ip
}

output "ingestion_service_ip" {
  value = google_compute_instance.ingestion_service.network_interface[0].access_config[0].nat_ip
}

output "kafka_ui_url" {
  value = "http://${google_compute_instance.kafka_ui.network_interface[0].access_config[0].nat_ip}:8080"
  description = "URL to access the Kafka UI"
}

output "kafka_bootstrap_servers" {
  value = join(",", [for instance in google_compute_instance.kafka_broker : "${instance.network_interface[0].network_ip}:9092"])
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "coinbase_dataset" {
  dataset_id    = "coinbase_data"
  friendly_name = "Coinbase Market Data"
  description   = "Dataset containing cryptocurrency market data from Coinbase"
  location      = "US"

  labels = {
    environment = "production"
  }

  access {
    role          = "OWNER"
    user_by_email = google_service_account.bigquery_service_account.email
  }

  access {
    role          = "READER"
    user_by_email = google_service_account.bigquery_service_account.email
  }
}

# Create service account for BigQuery operations
resource "google_service_account" "bigquery_service_account" {
  account_id   = "bigquery-service-account"
  display_name = "BigQuery Service Account"
  description  = "Service account for BigQuery operations"
}

# Grant BigQuery Admin role to the service account
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.bigquery_service_account.email}"
}

# Create a service account key for the BigQuery service account
resource "google_service_account_key" "bigquery_sa_key" {
  service_account_id = google_service_account.bigquery_service_account.name
}

# Create a VM for the Kafka-to-BigQuery consumer service
resource "google_compute_instance" "kafka_bigquery_consumer" {
  name         = "kafka-bigquery-consumer"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y python3 python3-pip git

    # Install required Python packages
    pip3 install kafka-python google-cloud-bigquery

    # Create directory for the consumer service
    mkdir -p /opt/kafka-bigquery-consumer

    # Create directory for GCP credentials
    mkdir -p /etc/gcp-credentials
    
    # Write service account key to file
    echo '${base64decode(google_service_account_key.bigquery_sa_key.private_key)}' > /etc/gcp-credentials/service-account.json
    chmod 600 /etc/gcp-credentials/service-account.json

    # Set up environment variables
    cat > /opt/kafka-bigquery-consumer/.env <<EOL
    KAFKA_BOOTSTRAP_SERVERS=${join(",", [for instance in google_compute_instance.kafka_broker : "${instance.network_interface[0].network_ip}:9092"])}
    MARKET_DATA_TOPIC=coinbase-market-data
    HISTORICAL_DATA_TOPIC=coinbase-market-data-historical
    GCP_PROJECT_ID=${var.project_id}
    BIGQUERY_DATASET=coinbase_data
    MARKET_DATA_TABLE=market_data
    HISTORICAL_DATA_TABLE=historical_data
    GCP_CREDENTIALS_PATH=/etc/gcp-credentials/service-account.json
    EOL

    # Set up service
    cat > /etc/systemd/system/kafka-bigquery-consumer.service <<EOL
    [Unit]
    Description=Kafka to BigQuery Consumer Service
    After=network.target

    [Service]
    Type=simple
    User=root
    WorkingDirectory=/opt/kafka-bigquery-consumer
    EnvironmentFile=/opt/kafka-bigquery-consumer/.env
    ExecStart=/usr/bin/python3 /opt/kafka-bigquery-consumer/kafka_bigquery_consumer.py --all
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    EOL

    # Enable service but don't start it yet (need to upload code first)
    systemctl enable kafka-bigquery-consumer
  EOF

  service_account {
    email  = google_service_account.bigquery_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["consumer-service"]
  
  depends_on = [
    google_compute_instance.kafka_broker,
    google_bigquery_dataset.coinbase_dataset
  ]
}

# Output information
output "bigquery_consumer_ip" {
  value = google_compute_instance.kafka_bigquery_consumer.network_interface[0].access_config[0].nat_ip
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.coinbase_dataset.dataset_id
}