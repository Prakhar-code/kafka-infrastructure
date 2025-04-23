from dotenv import load_dotenv
import os

# Load the .env file from the specific path
load_dotenv(dotenv_path="config/kafka/.env")
print(os.environ.get("BOOTSTRAP_SERVERS"))
# Access the environment variable
print( os.environ.get("INPUT_TOPICS").split(","))
