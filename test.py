from cassandra.cluster import Cluster
from datetime import datetime
import random
import uuid

# Connect to Cassandra cluster
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect('stock_competition')

# Insert data
def insert_data():
    for i in range(100000):  # Adjust number of records as needed
        stock_id = uuid.uuid4()
        stock_symbol = f'SYM{i}'
        stock_name = f'Stock Name {i}'
        price = random.uniform(100, 1000)
        timestamp = datetime.utcnow()
        volume = random.randint(100, 10000)
        session.execute("""
            INSERT INTO stock_data (stock_id, stock_symbol, stock_name, price, timestamp, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (stock_id, stock_symbol, stock_name, price, timestamp, volume))

insert_data()