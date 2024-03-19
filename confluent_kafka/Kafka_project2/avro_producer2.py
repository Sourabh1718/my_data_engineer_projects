import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import json

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import mysql.connector
import pandas as pd

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '47DX5QDUDUGMZW5Q',
    'sasl.password': 'hC6L+FWDFtdgADd5uvOVMWwYtKoR+Mq/vs9urGCaiXa15BqF02DZzoSPxWT3nXJK'
}

# MySQL database configuration
mysql_config = {
    'host': 'DESKTOP-98U992I',
    'user': 'root',
    'password': 'welcome1',
    'database': 'noob_db'
}

# Schema Registry configuration
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-81qy1r.us-west1.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('56G5YLCXARJWAYOB', 'Yy3pBijulMuWT5/aFNiLMba5XMUuVYk4wU5jR9o4p74vHjP+UJ+SFMCOlusmd7wd')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print(schema_str)

# Connect to MySQL database
connection = mysql.connector.connect(**mysql_config)
cursor = connection.cursor()

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanism'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': StringSerializer('utf_8'),  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# Load the last read timestamp from the config file
config_data = {}

try:
    with open('config.json') as f:
        config_data = json.load(f)
        last_read_timestamp = config_data.get('last_read_timestamp')
except FileNotFoundError:
    pass

# Set a default value for last_read_timestamp
if last_read_timestamp is None:
    last_read_timestamp = '1900-01-01 00:00:00'

# Use the last_read_timestamp in the SQL query
query = "SELECT * FROM product WHERE last_updated > '{}'".format(last_read_timestamp)
print(query)
# Execute the SQL query
cursor.execute(query)

# Fetch the result
rows = cursor.fetchall()

# Produce messages to Kafka
for row in rows:
    key = str(row['ID'])  # Use product ID as the key
    value = {
        'ID': row['ID'],
        'name': row['name'],
        'category': row['category'],
        'price': float(row['price']),
        'last_updated': row['last_updated'].isoformat()  # Convert datetime to ISO format
    }
    producer.produce(topic='product_updates', key=key, value=value)
    producer.flush()

# Update the last read timestamp
new_last_read_timestamp = max(row['last_updated'] for row in rows)
with open('last_read_timestamp.json', 'w') as f:
    json.dump({'last_read_timestamp': new_last_read_timestamp}, f)

# Close cursor and connection
cursor.close()
connection.close()
