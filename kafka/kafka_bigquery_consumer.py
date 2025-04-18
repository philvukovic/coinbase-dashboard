#!/usr/bin/env python3
"""
Kafka to BigQuery Consumer

This script consumes cryptocurrency market data from Kafka topics
and loads it into Google BigQuery for persistent storage and analysis.
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
import argparse
from kafka import KafkaConsumer
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka-bigquery-consumer')

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MARKET_DATA_TOPIC = os.environ.get('MARKET_DATA_TOPIC', 'coinbase-market-data')
HISTORICAL_DATA_TOPIC = os.environ.get('HISTORICAL_DATA_TOPIC', 'coinbase-market-data-historical')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'coinbase_data')
MARKET_DATA_TABLE = os.environ.get('MARKET_DATA_TABLE', 'market_data')
HISTORICAL_DATA_TABLE = os.environ.get('HISTORICAL_DATA_TABLE', 'historical_data')
GCP_CREDENTIALS_PATH = os.environ.get('GCP_CREDENTIALS_PATH', '/etc/gcp-credentials/service-account.json')

# Constants
MAX_BATCH_SIZE = 100  # Maximum records to batch before writing to BigQuery
MAX_BATCH_TIME_SECONDS = 60  # Maximum time to wait before writing batch to BigQuery

class BigQueryLoader:
    """Handles loading data into BigQuery"""
    
    def __init__(self, project_id, credentials_path=None):
        """Initialize the BigQuery client"""
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.client = bigquery.Client(
                project=project_id,
                credentials=credentials
            )
        else:
            # Use default credentials
            self.client = bigquery.Client(project=project_id)
            
        logger.info(f"Initialized BigQuery client for project {project_id}")
        
    def ensure_dataset_exists(self, dataset_id):
        """Make sure the dataset exists, create it if it doesn't"""
        logger.info(f"Checking if dataset {dataset_id} exists in project {self.client.project}")
        dataset_ref = self.client.dataset(dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} already exists")
        except NotFound:
            # Dataset doesn't exist, create it
            logger.info(f"Dataset {dataset_id} not found. Creating it.")
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Set the location
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Successfully created dataset {dataset_id}")
            except Exception as e:
                logger.error(f"Error creating dataset {dataset_id}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                raise
        except Exception as e:
            logger.error(f"Unexpected error when checking dataset {dataset_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def ensure_table_exists(self, dataset_id, table_id, schema):
        """Make sure the table exists, create it if it doesn't"""
        logger.info(f"Checking if table {dataset_id}.{table_id} exists")
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {dataset_id}.{table_id} already exists")
        except NotFound:
            # Table doesn't exist, create it
            logger.info(f"Table {dataset_id}.{table_id} not found, creating it")
            table = bigquery.Table(table_ref, schema=schema)
            
            # Enable time partitioning
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="partition_date"  # The field to use for partitioning
            )
            
            # Add clustering - only add if the fields exist in the schema
            schema_fields = [field.name for field in schema]
            clustering_fields = []
            if "base_currency" in schema_fields:
                clustering_fields.append("base_currency")
            if "quote_currency" in schema_fields:
                clustering_fields.append("quote_currency")
            if "product_id" in schema_fields:
                clustering_fields.append("product_id")
                
            if clustering_fields:
                table.clustering_fields = clustering_fields
            
            logger.info(f"Attempting to create table {dataset_id}.{table_id} with schema: {[f.name for f in schema]}")
            
            try:
                table = self.client.create_table(table)
                logger.info(f"Successfully created table {dataset_id}.{table_id}")
            except Exception as e:
                logger.error(f"Error creating table {dataset_id}.{table_id}: {str(e)}")
                # If table creation fails, let's try without partitioning and clustering
                logger.info("Trying to create a simple table without partitioning/clustering")
                simple_table = bigquery.Table(table_ref, schema=schema)
                try:
                    simple_table = self.client.create_table(simple_table)
                    logger.info(f"Created simple table {dataset_id}.{table_id} without partitioning/clustering")
                except Exception as simple_error:
                    logger.error(f"Error creating simple table: {str(simple_error)}")
                    raise
    
    def load_data_to_bigquery(self, dataset_id, table_id, rows):
        """Load data to BigQuery"""
        if not rows:
            logger.warning("No rows to load")
            return 0
        
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        logger.info(f"Attempting to load {len(rows)} rows to {dataset_id}.{table_id}")
        logger.info(f"First row sample: {json.dumps(rows[0])[:300]}...")
        
        job_config = bigquery.LoadJobConfig(
            schema = self.client.get_table(table_ref).schema,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            ignore_unknown_values=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        try:
            logger.info("Starting BigQuery load job")
            load_job = self.client.load_table_from_json(
                rows,
                table_ref,
                job_config=job_config
            )
            
            logger.info("Waiting for BigQuery load job to complete...")
            result = load_job.result()
            
            if load_job.errors:
                logger.error(f"Load job had errors: {load_job.errors}")
                return 0
                
            logger.info(f"Successfully loaded {len(rows)} rows to {dataset_id}.{table_id}")
            return len(rows)
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {str(e)}")
            import traceback
            logger.error(f"Error details: {traceback.format_exc()}")
                    
            # Write a sample of the failing data to a file for inspection
            try:
                with open(f"/tmp/bigquery_failed_rows_{time.time()}.json", "w") as f:
                    json.dump(rows[:5], f, indent=2)
                logger.info("Wrote sample of failing rows to /tmp/bigquery_failed_rows_*.json")
            except Exception as write_error:
                logger.error(f"Could not write sample rows: {str(write_error)}")
                
            return 0

def create_market_data_schema():
    """Define the schema for the market data table"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="Trading pair ID"),
        bigquery.SchemaField("base_currency", "STRING", description="Base cryptocurrency"),
        bigquery.SchemaField("quote_currency", "STRING", description="Quote currency"),
        bigquery.SchemaField("display_name", "STRING", description="Display name for the trading pair"),
        bigquery.SchemaField("status", "STRING", description="Status of the trading pair"),
        bigquery.SchemaField("volume_30d", "FLOAT64", description="30-day trading volume"),
        bigquery.SchemaField("price", "FLOAT64", description="Current price"),
        bigquery.SchemaField("rank", "INTEGER", description="Rank by volume"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", description="When the data was ingested"),
        bigquery.SchemaField("partition_date", "DATE", description="Date for partitioning"),
    ]

def create_historical_data_schema():
    """Define the schema for the historical candle data table"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="Trading pair ID"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Candle timestamp"),
        bigquery.SchemaField("close", "FLOAT64", description="Closing price for the period"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", description="When the data was ingested"),
        bigquery.SchemaField("partition_date", "DATE", description="Date for partitioning"),
    ]

def process_market_data(msg_value):
    """Process market data message and prepare for BigQuery"""
    try:
        # Parse the message
        data = msg_value.copy()

        # Convert to float
        if 'price' in data and data['price'] is not None:
            try:
                data['price'] = float(data['price'])
            except (ValueError, TypeError):
                data['price'] = None
        
        # Add fields for BigQuery
        data['partition_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Add price fields if not present
        if 'price' not in data:
            data['price'] = None
        
        # Add rank if not present
        if 'rank' not in data:
            data['rank'] = None
        
        # Ensure volume_30d is a float
        if 'volume_30d' in data and data['volume_30d'] is not None:
            try:
                data['volume_30d'] = float(data['volume_30d'])
            except (ValueError, TypeError):
                data['volume_30d'] = None
                
        # Check if base_currency and quote_currency fields exist
        if 'base_currency' not in data and 'id' in data and '-' in data['id']:
            base, quote = data['id'].split('-', 1)
            data['base_currency'] = base
            data['quote_currency'] = quote
        
        return data
    except Exception as e:
        logger.error(f"Error processing market data: {str(e)}")
        return None

def process_historical_data(msg_value):
    """Process historical candle data and prepare for BigQuery"""
    try:
        # Parse the message
        data = msg_value.copy()  # Make a copy to avoid modifying the original
        
        # Add fields for BigQuery
        if 'timestamp' in data and data['timestamp']:
            try:
                timestamp = int(data['timestamp'])
                data['partition_date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                data['partition_date'] = datetime.now().strftime('%Y-%m-%d')
        else:
            data['partition_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Ensure numeric fields are floats
        for field in ['close']:
            if field in data and data[field] is not None:
                try:
                    data[field] = float(data[field])
                except (ValueError, TypeError):
                    data[field] = None
        
        return data
    except Exception as e:
        logger.error(f"Error processing historical data: {str(e)}")
        return None

def consume_from_kafka(bootstrap_servers, topic, process_func, bigquery_loader, 
                      dataset_id, table_id, consumer_group):
    """Consume messages from Kafka and load to BigQuery"""
    # Initialize the consumer
    logger.info(f"Setting up Kafka consumer for topic {topic} with bootstrap servers {bootstrap_servers}")
    batch = []

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False,  # Manual commit for better control
            max_poll_interval_ms=300000,  # 5 minutes
            session_timeout_ms=60000,  # 1 minute
            heartbeat_interval_ms=20000,  # 20 seconds
        )
        
        logger.info(f"Started Kafka consumer for topic {topic}")
        
        # Batch and timer variables
        last_commit_time = time.time()
        last_message_time = time.time()
        
        # First, explicitly check if the topic exists and has data
        topics = consumer.topics()
        logger.info(f"Available Kafka topics: {topics}")
        
        if topic not in topics:
            logger.warning(f"Topic {topic} does not exist in Kafka. Will wait for it to be created.")
        
        # Poll for a few seconds to see if we get any messages
        poll_timeout = 5000  # 5 seconds
        poll_result = consumer.poll(timeout_ms=poll_timeout, max_records=10)
        
        if not poll_result:
            logger.warning(f"No messages received from {topic} in initial {poll_timeout/1000} second poll")
        else:
            logger.info(f"Received {sum(len(msgs) for msgs in poll_result.values())} messages in initial poll")
            # Process these messages
            for tp, messages in poll_result.items():
                for message in messages:
                    processed_data = process_func(message.value)
                    if processed_data:
                        batch.append(processed_data)
            
            # If we have enough data, process it
            if len(batch) >= 1:  # Process even a single message for testing
                logger.info(f"Processing batch of {len(batch)} messages from initial poll")
                try:
                    # Load to BigQuery
                    bigquery_loader.load_data_to_bigquery(dataset_id, table_id, batch)
                    
                    # Commit offsets
                    consumer.commit()
                    
                    # Reset batch and timer
                    batch = []
                    last_commit_time = time.time()
                    last_message_time = time.time()
                    
                except Exception as e:
                    logger.error(f"Error loading initial batch to BigQuery: {str(e)}")
        
        # Continue with normal consumer loop
        logger.info("Starting main consumer loop")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            last_message_time = time.time()
            
            if message_count % 100 == 0:
                logger.info(f"Processed {message_count} messages so far")
            
            # Process the message
            processed_data = process_func(message.value)
            
            if processed_data:
                batch.append(processed_data)
                
                # Log first few batches for debugging
                if len(batch) <= 5 or message_count <= 100:
                    logger.info(f"Sample processed message: {json.dumps(processed_data)[:200]}...")
            
            # Check if we should commit and load to BigQuery
            current_time = time.time()
            batch_full = len(batch) >= MAX_BATCH_SIZE
            time_elapsed = current_time - last_commit_time >= MAX_BATCH_TIME_SECONDS
            
            if batch and (batch_full or time_elapsed):
                logger.info(f"Processing batch of {len(batch)} messages (batch_full={batch_full}, time_elapsed={time_elapsed})")
                try:
                    # Load to BigQuery
                    bigquery_loader.load_data_to_bigquery(dataset_id, table_id, batch)
                    
                    # Commit offsets
                    consumer.commit()
                    
                    # Reset batch and timer
                    batch = []
                    last_commit_time = current_time
                    
                except Exception as e:
                    logger.error(f"Error loading to BigQuery: {str(e)}")
                    # Don't clear the batch, but still update the timer to avoid rapid retries
                    last_commit_time = current_time
            
            # Check for idle timeout (no messages for a long time)
            if current_time - last_message_time > 300:  # 5 minutes
                logger.warning(f"No messages received for 5 minutes. Consumer may be idle.")
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {str(e)}")
        raise
    
    finally:
        # Final batch load if there's anything left
        if batch:
            try:
                logger.info(f"Processing final batch of {len(batch)} messages")
                bigquery_loader.load_data_to_bigquery(dataset_id, table_id, batch)
                consumer.commit()
            except Exception as e:
                logger.error(f"Error during final BigQuery load: {str(e)}")
        
        # Close the consumer
        consumer.close()
        logger.info("Consumer closed")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Kafka to BigQuery Consumer')
    parser.add_argument('--market-data', action='store_true', help='Consume market data topic')
    parser.add_argument('--historical-data', action='store_true', help='Consume historical data topic')
    parser.add_argument('--all', action='store_true', help='Consume all topics')
    parser.add_argument('--create-tables-only', action='store_true', help='Just create tables and exit')
    parser.add_argument('--consumer-group-suffix', type=str, default='consumer', help='Suffix for consumer group name')
    args = parser.parse_args()
    
    # Validate project ID
    if not GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID environment variable is required")
        return 1
    
    logger.info(f"Starting Kafka to BigQuery consumer with project {GCP_PROJECT_ID}")
    logger.info(f"Using dataset {BIGQUERY_DATASET}")
    logger.info(f"Command line arguments: {args}")
    
    # Initialize BigQuery loader
    try:
        logger.info(f"Initializing BigQuery loader with credentials from {GCP_CREDENTIALS_PATH}")
        bigquery_loader = BigQueryLoader(GCP_PROJECT_ID, GCP_CREDENTIALS_PATH)
        
        # Ensure dataset exists
        logger.info(f"Ensuring dataset {BIGQUERY_DATASET} exists")
        bigquery_loader.ensure_dataset_exists(BIGQUERY_DATASET)
        
        # Force table creation 
        logger.info("Starting table creation process")
        
        # Create market data table
        market_schema = create_market_data_schema()
        logger.info(f"Market data schema has {len(market_schema)} fields")
        bigquery_loader.ensure_table_exists(
            BIGQUERY_DATASET, 
            MARKET_DATA_TABLE, 
            market_schema
        )
        
        # Create historical data table
        historical_schema = create_historical_data_schema()
        logger.info(f"Historical data schema has {len(historical_schema)} fields")
        bigquery_loader.ensure_table_exists(
            BIGQUERY_DATASET, 
            HISTORICAL_DATA_TABLE, 
            historical_schema
        )
        
        if args.create_tables_only:
            logger.info("Tables created. Exiting as requested.")
            return 0
            
    except Exception as e:
        logger.error(f"Error during setup: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    # Set up consumers based on arguments
    if True or args.market_data:
        logger.info(f"Setting up consumer for market data topic {MARKET_DATA_TOPIC}")
        
        # Start consumer in a separate thread
        import threading
        market_thread = threading.Thread(
            target=consume_from_kafka,
            args=(
                KAFKA_BOOTSTRAP_SERVERS,
                MARKET_DATA_TOPIC,
                process_market_data,
                bigquery_loader,
                BIGQUERY_DATASET,
                MARKET_DATA_TABLE,
                f"{MARKET_DATA_TOPIC}-{args.consumer_group_suffix}"
            )
        )
        market_thread.daemon = True
        market_thread.start()
    
    if True or args.historical_data:
        logger.info(f"Setting up consumer for historical data topic {HISTORICAL_DATA_TOPIC}")
        
        # Start consumer in a separate thread
        import threading
        historical_thread = threading.Thread(
            target=consume_from_kafka,
            args=(
                KAFKA_BOOTSTRAP_SERVERS,
                HISTORICAL_DATA_TOPIC,
                process_historical_data,
                bigquery_loader,
                BIGQUERY_DATASET,
                HISTORICAL_DATA_TABLE,
                f"{HISTORICAL_DATA_TOPIC}-{args.consumer_group_suffix}"
            )
        )
        historical_thread.daemon = True
        historical_thread.start()
    
    # Keep main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    
    return 0

if __name__ == "__main__":
    exit(main())