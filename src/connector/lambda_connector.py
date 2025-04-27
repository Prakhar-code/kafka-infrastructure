from decimal import Decimal
import json
import time
import logging
import boto3
import signal
import sys
from kafka import KafkaConsumer
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

# Load environment variables from .env file
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC")
GROUP_ID = os.environ.get("GROUP_ID")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
LAMBDA_FUNCTION = os.environ.get("LAMBDA_FUNCTION")

logger.info(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")
logger.info(f"OUTPUT_TOPIC: {OUTPUT_TOPIC}")
logger.info(f"GROUP_ID: {GROUP_ID}")
logger.info(f"DYNAMODB_TABLE: {DYNAMODB_TABLE}")
logger.info(f"LAMBDA_FUNCTION: {LAMBDA_FUNCTION}")

# Configuration settings
CONFIG = {
    "kafka": {
        "bootstrap_servers": BOOTSTRAP_SERVERS,  # Replace with your Kafka broker address
        "topic": OUTPUT_TOPIC,  # The topic to consume from
        "group_id": GROUP_ID,  # Consumer group ID
        "auto_offset_reset": "earliest",  # Start reading from the beginning if no offset found
        "enable_auto_commit": False,  # Manual commit for better control
    },
    "aws": {
        "region_name": "us-east-2",  # Replace with your AWS region
        "dynamodb_table": DYNAMODB_TABLE,  # Replace with your DynamoDB table name
        "lambda_function": LAMBDA_FUNCTION,  # Replace with your Lambda function name
        "batch_size": 25,  # Number of messages to process in a batch
        "max_retry": 3,  # Maximum number of retries on failure

    },
}


class KafkaToDynamoDBLambdaConnector:
    def __init__(self, config):
        self.config = config
        self.consumer = None
        self.dynamodb = None
        self.lambda_client = None
        self.running = False
        self.setup_signal_handlers()
        
    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received, closing connector gracefully...")
        self.running = False

    def connect(self):
        """Establish connections to Kafka, DynamoDB, and Lambda"""
        try:
            # Set up Kafka consumer
            self.consumer = KafkaConsumer(
                self.config["kafka"]["topic"],
                bootstrap_servers=self.config["kafka"]["bootstrap_servers"],
                group_id=self.config["kafka"]["group_id"],
                auto_offset_reset=self.config["kafka"]["auto_offset_reset"],
                enable_auto_commit=self.config["kafka"]["enable_auto_commit"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            # Set up AWS clients
            session = boto3.Session(region_name=self.config["aws"]["region_name"])
            self.dynamodb = session.resource("dynamodb")
            self.lambda_client = session.client("lambda")
            
            logger.info("Successfully connected to Kafka and AWS services")
            return True
        except Exception as e:
            logger.error(f"Error connecting to services: {str(e)}")
            return False

    def write_to_dynamodb(self, items):
        """Write a batch of items to DynamoDB table"""
        table = self.dynamodb.Table(self.config["aws"]["dynamodb_table"])

        # Debug: Print the first item to see its structure

        if items:
            logger.info(f"Sample item structure: {json.dumps(items[0])}")
            logger.info(f"Keys present: {list(items[0].keys())}")
            logger.info(f"sample decimal conversion: {json.loads(json.dumps(items[0]), parse_float=Decimal)}")

        # Use batch_writer to handle retries and rate limiting
        with table.batch_writer() as batch:
            for item in items:
                try:
                    # item_to_put: dict = json.loads(json.dumps(item), parse_float=Decimal)
                    item = json.loads(json.dumps(item), parse_float=Decimal)
                    batch.put_item(Item=item)

                    logger.info(f"Successfully wrote {len(items)} items to DynamoDB")

                except Exception as e:
                    logger.error(f"Error writing item to DynamoDB: {str(e)}")
                    # Optionally, you can implement retry logic here

    def invoke_lambda(self, payload):
        """Invoke the UpdateLambda function with the given payload"""
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.config["aws"]["lambda_function"],
                InvocationType="Event",  # Asynchronous invocation
                Payload=json.dumps(payload),
            )

            status_code = response.get("StatusCode")
            if status_code == 202:  # Expected status code for async invocation
                logger.info(f"Successfully invoked Lambda function")
                return True
            else:
                logger.error(
                    f"Lambda invocation failed with status code: {status_code}"
                )
                
                return False

        except ClientError as e:
            logger.error(f"Error invoking Lambda: {str(e)}")
            return False

    def process_messages(self):
        """Process messages from Kafka in batches"""
        batch = []
        last_commit = time.time()

        logger.info(
            f"Starting to consume messages from {self.config['kafka']['topic']}"
        )

        for message in self.consumer:
            if not self.running:
                break

            try:
                # Extract message value (already deserialized)
                record = message.value
                # print(record)

                # Add message to batch
                batch.append(record)

                # Process batch if it reaches the batch size
                if len(batch) >= self.config["aws"]["batch_size"]:
                    self.write_to_dynamodb(batch)
                    self.invoke_lambda({"records": batch})

                    # Commit offsets after successful processing
                    self.consumer.commit()
                    last_commit = time.time()

                    # Clear the batch
                    batch = []

            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                # Continue processing other messages

        # Process any remaining messages in the batch
        if batch:
            try:
                self.write_to_dynamodb(batch)
                self.invoke_lambda({"records": batch})
                self.consumer.commit()
            except Exception as e:
                logger.error(f"Error processing final batch: {str(e)}")

        logger.info("Message processing stopped")

    def run(self):
        """Main method to run the connector"""
        if not self.connect():
            logger.error("Failed to connect to required services. Exiting.")
            return False

        self.running = True

        try:
            self.process_messages()
        except Exception as e:
            logger.error(f"Unexpected error in connector: {str(e)}")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Connector shut down")

        return True


def main():
    connector = KafkaToDynamoDBLambdaConnector(CONFIG)
    connector.run()
    

if __name__ == "__main__":
    main()