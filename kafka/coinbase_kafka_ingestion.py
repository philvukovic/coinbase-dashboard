# coinbase_kafka_ingestion.py
import os
import json
import time
import logging
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
from apscheduler.schedulers.blocking import BlockingScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('coinbase-kafka-ingestion')

# Configuration
COINBASE_API_KEY = os.environ.get('COINBASE_API_KEY')
COINBASE_API_SECRET = os.environ.get('COINBASE_API_SECRET')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MARKET_DATA_TOPIC = os.environ.get('MARKET_DATA_TOPIC', 'coinbase-market-data')
QUOTE_CURRENCY = os.environ.get('QUOTE_CURRENCY', 'USD')
TOP_ASSETS_LIMIT = int(os.environ.get('TOP_ASSETS_LIMIT', '20'))
BACKFILL_DAYS = int(os.environ.get('BACKFILL_DAYS', '365'))

def generate_coinbase_signature(api_secret, timestamp, method, request_path, body=''):
    """
    Generate a signature for Coinbase API authentication
    
    Args:
        api_secret (str): Your Coinbase API secret
        timestamp (str): Current timestamp as string
        method (str): HTTP method (GET, POST, etc.)
        request_path (str): The request path and query string
        body (str): Request body for POST/PUT requests, or empty string
        
    Returns:
        str: Base64 encoded signature
    """
    import hmac
    import hashlib
    import base64
    
    # Decode the API secret from base64
    secret = base64.b64decode(api_secret)
    
    # Create the message string to sign
    message = timestamp + method.upper() + request_path + (body or '')
    
    # Create the HMAC-SHA256 signature
    signature = hmac.new(
        secret,
        message.encode('utf-8'),
        hashlib.sha256
    )
    
    # Return the signature as a base64 encoded string
    return base64.b64encode(signature.digest()).decode('utf-8')

class CoinbaseAPIClient:
    """Client for interacting with Coinbase API"""
    
    # Using the public API endpoint instead of the brokerage API
    BASE_URL = 'https://api.exchange.coinbase.com'
    
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
    
    def _make_request(self, method, endpoint, params=None, data=None):
        """Make a request to the Coinbase API"""
        # Construct the full URL
        url = f"{self.BASE_URL}{endpoint}"
        
        # Make the request - for public endpoints we don't need authentication
        if method.upper() == 'GET':
            response = self.session.get(url, params=params)
        elif method.upper() == 'POST':
            response = self.session.post(url, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        return response.json()
    
    def get_top_products_by_volume(self, limit=20, quote_currency='USD'):
        """Fetch all products and filter by volume"""
        try:
            # First, get all products
            all_products = self._make_request('GET', '/products')
            
            # Filter for products with the specified quote currency
            if quote_currency:
                filtered_products = [p for p in all_products if p.get('quote_currency') == quote_currency]
            else:
                filtered_products = all_products
            
            # Sort by volume (if available)
            # Note: we need to fetch stats for each product to get volume
            products_with_stats = []
            for product in filtered_products[:min(limit*10, len(filtered_products))]:  # Fetch more than needed then filter
                # sleep to avoid rate limiting
                time.sleep(0.2)

                product_id = product.get('id')
                if not product_id:
                    continue
                
                # Get 24h stats for this product
                try:
                    stats = self._make_request('GET', f'/products/{product_id}/stats')
                    product['volume_30d'] = float(stats.get('volume_30day', '0'))
                    
                    # Get current ticker data (price information)
                    try:
                        ticker = self._make_request('GET', f'/products/{product_id}/ticker')
                        if ticker:
                            product['price'] = float(ticker.get('price', '0'))
                    except Exception as ticker_error:
                        logger.warning(f"Could not get ticker for {product_id}: {str(ticker_error)}")
                    
                    products_with_stats.append(product)
                except Exception as e:
                    logger.warning(f"Could not get stats for {product_id}: {str(e)}")
            
            # Sort by volume and limit
            sorted_products = sorted(
                products_with_stats, 
                key=lambda x: float(x.get('volume_30d', 0)) * float(x.get('price', 0)),
                reverse=True
            )[:limit]
            
            # Add ranks
            for i, product in enumerate(sorted_products):
                product['rank'] = i + 1
            
            # Format response to match expected structure
            return {'products': sorted_products}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_product_candles(self, product_id, start_time, end_time, granularity='3600'):
        """Fetch historical candle data for a product"""
        try:
            # Convert granularity from string to seconds
            granularity_map = {
                'ONE_MINUTE': '60',
                'FIVE_MINUTE': '300',
                'FIFTEEN_MINUTE': '900',
                'ONE_HOUR': '3600',
                'SIX_HOUR': '21600',
                'ONE_DAY': '86400'
            }
            
            if granularity in granularity_map:
                granularity = granularity_map[granularity]
            
            params = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'granularity': granularity
            }
            
            candles_data = self._make_request('GET', f'/products/{product_id}/candles', params=params)
            
            # Transform the response to match expected format
            # The API returns: [timestamp, low, high, open, close, volume]
            formatted_candles = []
            for candle in candles_data:
                formatted_candles.append({
                    'timestamp': candle[0],
                    'close': candle[4],
                })
            
            return {'candles': formatted_candles}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {product_id}: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise

class KafkaProducerWrapper:
    """Wrapper for Kafka producer with retry logic"""
    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5,
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip'
        )
    
    def send_message(self, topic, key, value):
        """Send message to Kafka topic with retry logic"""
        future = self.producer.send(
            topic, 
            key=key.encode('utf-8') if key else None, 
            value=value
        )
        try:
            # Block until the message is sent (or fails)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to {record_metadata.topic}[{record_metadata.partition}] at offset {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            return False
    
    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        self.producer.close()

class DataIngestionService:
    """Service to handle data ingestion from Coinbase to Kafka"""
    
    def __init__(self, api_client, kafka_producer, market_data_topic):
        self.api_client = api_client
        self.kafka_producer = kafka_producer
        self.market_data_topic = market_data_topic
    
    def ingest_top_products(self, limit=20, quote_currency='USD'):
        """Ingest top products by volume data into Kafka"""
        try:
            logger.info(f"Fetching top {limit} products by volume for {quote_currency}")
            products_data = self.api_client.get_top_products_by_volume(
                limit=limit,
                quote_currency=quote_currency
            )
            
            if not products_data or 'products' not in products_data:
                logger.warning("No product data returned from API")
                return 0
            
            success_count = 0
            for product in products_data['products']:
                # Add timestamp for when we ingested this data
                product['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                # Use product_id as the key for proper partitioning
                product_id = product.get('id', 'unknown')
                
                if self.kafka_producer.send_message(
                    self.market_data_topic,
                    key=product_id,
                    value=product
                ):
                    success_count += 1
            
            logger.info(f"Successfully ingested {success_count}/{len(products_data['products'])} products")
            return success_count
            
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
            raise
    
    def backfill_historical_data(self, days, limit=20, quote_currency='USD'):
        """Backfill historical data for top cryptocurrencies"""
        logger.info(f"Starting backfill for past {days} days")
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        # First get the current top products
        products_data = self.api_client.get_top_products_by_volume(
            limit=limit,
            quote_currency=quote_currency
        )
        
        if not products_data or 'products' not in products_data:
            logger.warning("No product data returned for backfill")
            return 0
        
        total_candles = 0
        historical_topic = f"{self.market_data_topic}-historical"
        
        for product in products_data['products']:
            product_id = product.get('id')
            if not product_id:
                continue
                
            logger.info(f"Backfilling data for {product_id} from {start_time} to {end_time}")
            
            # Break the request into smaller chunks to handle API limitations
            current_start = start_time
            chunk_size = timedelta(days=7)  # Process a week at a time
            
            while current_start < end_time:
                current_end = min(current_start + chunk_size, end_time)
                
                try:
                    candles = self.api_client.get_product_candles(
                        product_id=product_id,
                        start_time=current_start,
                        end_time=current_end
                    )
                    
                    if candles and 'candles' in candles:
                        # Add metadata to each candle
                        for candle in candles['candles']:
                            candle['id'] = product_id
                            candle['ingestion_timestamp'] = datetime.utcnow().isoformat()
                            
                            self.kafka_producer.send_message(
                                historical_topic,
                                key=product_id,
                                value=candle
                            )
                            total_candles += 1
                        
                        logger.info(f"Backfilled {len(candles['candles'])} candles for {product_id} from {current_start} to {current_end}")
                    
                except Exception as e:
                    logger.error(f"Error backfilling {product_id} from {current_start} to {current_end}: {str(e)}")
                    # Continue with next chunk despite errors
                
                # Increment to next chunk
                current_start = current_end
                
                # Sleep to avoid rate limiting
                time.sleep(0.7)
            
        logger.info(f"Backfill completed. Total {total_candles} candles ingested.")
        return total_candles

def daily_ingestion_job():
    """Daily job to fetch latest market data"""
    try:
        logger.info("Starting daily ingestion job")
        
        # Create required objects
        api_client = CoinbaseAPIClient(COINBASE_API_KEY, COINBASE_API_SECRET)
        kafka_producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
        ingestion_service = DataIngestionService(
            api_client, 
            kafka_producer, 
            MARKET_DATA_TOPIC
        )
        
        # Run ingestion
        count = ingestion_service.ingest_top_products(
            limit=TOP_ASSETS_LIMIT,
            quote_currency=QUOTE_CURRENCY
        )
        
        logger.info(f"Daily ingestion completed. Processed {count} assets.")
        
        # Clean up
        kafka_producer.close()
        
    except Exception as e:
        logger.error(f"Error in daily ingestion job: {str(e)}")
        # Job will run again next time, so we just log the error

def run_backfill():
    """Run one-time backfill operation"""
    try:
        logger.info("Starting backfill operation")
        
        # Create required objects
        api_client = CoinbaseAPIClient(COINBASE_API_KEY, COINBASE_API_SECRET)
        kafka_producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
        ingestion_service = DataIngestionService(
            api_client, 
            kafka_producer, 
            MARKET_DATA_TOPIC
        )
        
        # Run backfill
        count = ingestion_service.backfill_historical_data(
            days=BACKFILL_DAYS,
            limit=TOP_ASSETS_LIMIT,
            quote_currency=QUOTE_CURRENCY
        )
        
        logger.info(f"Backfill completed. Processed {count} candles.")
        
        # Clean up
        kafka_producer.close()
        
    except Exception as e:
        logger.error(f"Error in backfill operation: {str(e)}")

def main():
    """Main entry point"""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Coinbase data ingestion service')
    parser.add_argument('--backfill', action='store_true', help='Run backfill operation')
    parser.add_argument('--schedule', action='store_true', help='Run scheduled daily ingestion')
    args = parser.parse_args()
    
    if True:
        run_backfill()
    
    if args.schedule:
        # Run once immediately
        daily_ingestion_job()
        
        # Then schedule for daily runs
        scheduler = BlockingScheduler()
        scheduler.add_job(
            daily_ingestion_job, 
            'cron', 
            hour=0,  # Run at midnight UTC
            minute=5  # 5 minutes past midnight
        )
        
        try:
            logger.info("Starting scheduler for daily ingestion")
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("Shutting down scheduler")
            scheduler.shutdown()

if __name__ == "__main__":
    main()