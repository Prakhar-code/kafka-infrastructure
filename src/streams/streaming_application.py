from confluent_kafka import Consumer, Producer
import json
import logging
import copy
import os
import time
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


load_dotenv()
KAFKA_BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVERS")
INPUT_TOPICS = os.environ.get("INPUT_TOPICS", "").split(",")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "dead-letter-queue")


if not KAFKA_BOOTSTRAP_SERVER or not INPUT_TOPICS or not OUTPUT_TOPIC:
    logger.error("Required environment variables are missing!")
    exit(1)


def parse_percentage(value):
    """Convert percentage string to numeric value."""
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.endswith('%'):
            return float(value.strip('%'))
        return float(value) 
    except Exception as e:
        logger.error(f"Failed to parse percentage value '{value}': {e}")
        return None  

# Transform Function
def transform_data(stock_data):
    """Transform incoming stock data by converting strings to numeric values." "89.10%"-> 89.10"""
    try:
        transformed_data = copy.deepcopy(stock_data)
        
        
        if 'Profitability' in transformed_data:
            profitability = transformed_data['Profitability']
            for field in ['GrossMargin', 'NetMargin', 'OperatingMargin']:
                if field in profitability:
                    profitability[field] = parse_percentage(profitability[field])

       
        if 'Growth' in transformed_data:
            growth = transformed_data['Growth']
            for field in ['Revenue_Growth_YOY', 'EPS_Growth_YOY', 'Net_Profit_Growth_YOY']:
                if field in growth:
                    growth[field] = parse_percentage(growth[field])

       
        if 'Valuation' in transformed_data:
            valuation = transformed_data['Valuation']
            if 'Dividend_Yield' in valuation:
                valuation['Dividend_Yield'] = parse_percentage(valuation['Dividend_Yield'])

        return transformed_data

    except Exception as e:
        logger.error(f"Error transforming stock data: {e}")
        return None 

def format_for_logs(stock_data):
    """
    Format stock data for logs by converting numeric values to unquoted percentage values.
    
    Args:
        stock_data (dict): Original stock data dictionary(takes only one decimal point value 89.90->89.9)
    
    Returns:
        dict: Stock data with numeric percentage fields formatted
    """
    log_data = copy.deepcopy(stock_data)
    
   
    if 'Profitability' in log_data:
        profitability = log_data['Profitability']
        for field in ['GrossMargin', 'NetMargin', 'OperatingMargin']:
            if field in profitability and isinstance(profitability[field], (int, float)):

                profitability[field] = float(f"{round(profitability[field], 1)}")

    
    if 'Growth' in log_data:
        growth = log_data['Growth']
        for field in ['Revenue_Growth_YOY', 'EPS_Growth_YOY', 'Net_Profit_Growth_YOY']:
            if field in growth and isinstance(growth[field], (int, float)):
                
                growth[field] = float(f"{round(growth[field], 1)}")

    
    if 'Valuation' in log_data:
        valuation = log_data['Valuation']
        if 'Dividend_Yield' in valuation and isinstance(valuation['Dividend_Yield'], (int, float)):
            
            valuation['Dividend_Yield'] = float(f"{round(valuation['Dividend_Yield'], 1)}")
    
    return log_data

def log_well_formatted(stock_data, category):
    """Log stock data in a well-formatted JSON structure with custom formatting.(replace quotation mark and add % sign)"""
    
    formatted_log_data = format_for_logs(stock_data)
    
    
    display_data = copy.deepcopy(formatted_log_data)
    
    
    if 'Profitability' in display_data:
        profitability = display_data['Profitability']
        for field in ['GrossMargin', 'NetMargin', 'OperatingMargin']:
            if field in profitability and profitability[field] is not None:
               
                profitability[field] = f"{profitability[field]}%" if isinstance(profitability[field], (int, float)) else profitability[field]

    if 'Growth' in display_data:
        growth = display_data['Growth']
        for field in ['Revenue_Growth_YOY', 'EPS_Growth_YOY', 'Net_Profit_Growth_YOY']:
            if field in growth and growth[field] is not None:
               
                growth[field] = f"{growth[field]}%" if isinstance(growth[field], (int, float)) else growth[field]

    if 'Valuation' in display_data:
        valuation = display_data['Valuation']
        if 'Dividend_Yield' in valuation and valuation['Dividend_Yield'] is not None:
            
            valuation['Dividend_Yield'] = f"{valuation['Dividend_Yield']}%" if isinstance(valuation['Dividend_Yield'], (int, float)) else valuation['Dividend_Yield']
    
    logger.info(f"Processed stock data for {stock_data.get('scripName', 'Unknown')} from {category} cap:")

    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, str) and obj.endswith('%'):
                return obj  
            return super().default(obj)
    
    formatted_json = json.dumps(display_data, indent=4, cls=CustomEncoder)
    
    formatted_json = formatted_json.replace('"', '')
    logger.info(formatted_json)

def validate_stock_data(stock_data):
    """Validate that stock data contains required fields."""
    required_fields = ['scripName', 'basicInfo']
    
    for field in required_fields:
        if field not in stock_data or not stock_data[field]:  
            logger.warning(f"Invalid stock data: Missing or empty field `{field}` for record: {stock_data}")
            return False
    
    return True


def create_consumer():
    """Create Kafka consumer with robust configuration."""
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': 'stream-transform-group',
        
        # Coordinator and Connection Settings
        'session.timeout.ms': 45000,                   # Increased session timeout
        'max.poll.interval.ms': 300000,                # Prevent unnecessary rebalances
        'heartbeat.interval.ms': 3000,                 # Frequent heartbeats
        'reconnect.backoff.ms': 1000,                  # Retry interval
        'coordinator.query.interval.ms': 60000,         # Query coordinator every 60 seconds               
        'reconnect.backoff.max.ms': 10000,             # Maximum backoff time
        
        # Error Handling and Offset Management
        'auto.offset.reset': 'latest',                 # Start from latest offset
        'enable.auto.commit': True,                    # Automatic offset commits
        'auto.commit.interval.ms': 5000,               # Commit offsets every 5 seconds
        
        # Network and Performance Tuning
        'socket.timeout.ms': 60000,                    # Socket connection timeout
        'fetch.min.bytes': 1,                          # Fetch even a single byte
        'fetch.wait.max.ms': 500,                      # Wait up to 500ms for data
        
        # Additional Stability Configurations
        'client.id': 'stream-transform-consumer',       # Unique client identifier
        'group.instance.id': 'consumer-1',             # Stable group instance
        
        # Optional: Debugging
        #'debug': 'broker,topic,msg'
    }
    
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe(INPUT_TOPICS)
        
        return consumer
    
    except Exception as e:
        logger.error(f"Kafka Consumer Creation Failed: {e}")
        # Optional: Implement retry mechanism or fallback strategy
        raise

def validate_consumer_connectivity(consumer):
    """
    Validate and log Kafka consumer connectivity details.
    """
    try:
        # Check cluster metadata
        cluster_metadata = consumer.list_topics(timeout=10)
        logger.info("Kafka Cluster Metadata:")
        for topic, topic_metadata in cluster_metadata.topics.items():
            logger.info(f"Topic: {topic}")
            logger.info(f"Partitions: {len(topic_metadata.partitions)}")
        
        # Perform a quick poll to trigger connection mechanisms
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            logger.warning("No immediate messages available during connectivity check")
    
    except Exception as e:
        logger.error(f"Connectivity Validation Failed: {e}")


def create_producer():
    try:
        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 5,
            'request.timeout.ms': 20000,
            'socket.timeout.ms': 120000,
            'max.in.flight.requests.per.connection': 5,
            'retry.backoff.ms': 1000,
            #'debug': 'broker,topic,msg'
        }
        producer = Producer(producer_config)
        return producer
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        raise

def stream_data():
    consumer = create_consumer()
    producer = create_producer()
    logger.info("Kafka streaming application started...")

   
    INACTIVITY_TIMEOUT = 300  
    last_message_time = time.time()
    producer_failure_count = 0
    MAX_PRODUCER_FAILURES = 5  

    try:
        while True:
            message = consumer.poll(timeout=1.0)  
            if message is None:
                # Check for inactivity
                if time.time() - last_message_time > INACTIVITY_TIMEOUT:
                    logger.info(f"No new messages received for {INACTIVITY_TIMEOUT} seconds. Shutting down.")
                    break
                continue

            if message.error():
                logger.error(f"Consumer error: {message.error()}")
                continue
 
            last_message_time = time.time()  # Update on each message
            try:
                original_data = json.loads(message.value().decode('utf-8'))
                
                if not validate_stock_data(original_data):
                    logger.warning(f"Invalid stock data. Sending to DLQ: {original_data}")
                    producer.produce(DLQ_TOPIC, key=original_data.get('scripName', 'unknown').encode('utf-8'), value=json.dumps(original_data).encode('utf-8'))
                    producer.flush()
                    consumer.commit(message)
                    continue
                
                transformed_data = transform_data(original_data)
                if transformed_data is None:
                    logger.error(f"Failed to transform stock data for {original_data.get('scripName', 'unknown')}. Sending to DLQ.")
                    producer.produce(DLQ_TOPIC, key=original_data.get('scripName', 'unknown').encode('utf-8'), value=json.dumps(original_data).encode('utf-8'))
                    producer.flush()
                    consumer.commit(message)
                    continue
                
                category = transformed_data.get("category", "unknown")
                log_well_formatted(transformed_data, category)
                
                try:
                    producer.produce(OUTPUT_TOPIC, key=original_data.get('scripName', 'unknown').encode('utf-8'), value=json.dumps(transformed_data).encode('utf-8'))
                    producer.flush()
                    producer_failure_count = 0  # Reset on success
                except Exception as e:
                    logger.error(f"Producer failed: {e}")
                    producer_failure_count += 1
                    if producer_failure_count >= MAX_PRODUCER_FAILURES:
                        logger.error(f"Producer failed {MAX_PRODUCER_FAILURES} times consecutively. Shutting down.")
                        break
                    producer.produce(DLQ_TOPIC, key=original_data.get('scripName', 'unknown').encode('utf-8'), value=json.dumps(original_data).encode('utf-8'))
                    producer.flush()
                
                consumer.commit(message)
            
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                producer.produce(DLQ_TOPIC, key=original_data.get('scripName', 'unknown').encode('utf-8'), value=json.dumps(original_data).encode('utf-8'))
                producer.flush()
                consumer.commit(message)

    except KeyboardInterrupt:
        logger.info("Kafka streaming application stopped by user.")
    finally:
        consumer.close()
        producer.flush()
        logger.info("Kafka resources cleaned up.")

if __name__ == "__main__":
    stream_data()


