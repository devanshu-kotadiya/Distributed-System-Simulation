import csv
from cassandra.cluster import Cluster
from datetime import datetime

# Connect to the Cassandra cluster
cluster = Cluster()
session = cluster.connect('my_keyspace')  # Use your keyspace

# Open the CSV file and read it
with open('./train1.csv', 'r') as f:
    reader = csv.DictReader(f, delimiter='\t')  # Use '\t' for tab-separated values
    columns = reader.fieldnames  # Get the actual column names
    
    # Strip any leading/trailing whitespace from column names
    columns = [col.strip() for col in columns]
    print(f"CSV Columns: {columns}")  # Print column names to verify

    # Iterate over each row in the CSV
    for row in reader:  
        try:
            # Handle empty trade_min values
            if not row['trade_min']:  # If trade_min is empty
                print(f"Skipping row with empty trade_min: {row}")
                continue  # Skip this row

            # Convert trade_min to timestamp format (ensure it matches the format in the CSV file)
            trade_min = datetime.strptime(row['trade_min'], '%d/%m/%Y %H:%M')  # Adjust date format if needed

            # Prepare the query
            query = """
            INSERT INTO stock_data_test (
                row_id, rank, trade_min, id_stock, open, close, high, low, volume, 
                a1_v, a2_v, a3_v, a4_v, a5_v, b1_v, b2_v, b3_v, b4_v, b5_v, 
                a1_p, a2_p, a3_p, a4_p, a5_p, b1_p, b2_p, b3_p, b4_p, b5_p, 
                total_ask, total_bid, average_ask, average_bid, cjcs, target)
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s
            );
            """
            
            # Define the values to be inserted (use appropriate types)
            values = (
                row['row_id'],
                int(row['rank']),
                trade_min,
                row['id_stock'],
                float(row['open']),
                float(row['close']),
                float(row['high']),
                float(row['low']),
                int(row['volume']),
                float(row['a1_v']),
                float(row['a2_v']),
                float(row['a3_v']),
                float(row['a4_v']),
                float(row['a5_v']),
                float(row['b1_v']),
                float(row['b2_v']),
                float(row['b3_v']),
                float(row['b4_v']),
                float(row['b5_v']),
                float(row['a1_p']),
                float(row['a2_p']),
                float(row['a3_p']),
                float(row['a4_p']),
                float(row['a5_p']),
                float(row['b1_p']),
                float(row['b2_p']),
                float(row['b3_p']),
                float(row['b4_p']),
                float(row['b5_p']),
                float(row['total_ask']),
                float(row['total_bid']),
                float(row['average_ask']),
                float(row['average_bid']),
                float(row['cjcs']),
                float(row['target'])
            )

            # Execute the query with the parameters
            session.execute(query, values)

        except Exception as e:
            print(f"Error inserting row {row.get('row_id', 'N/A')}: {e}")

# Shutdown the session after completing the inserts
cluster.shutdown()