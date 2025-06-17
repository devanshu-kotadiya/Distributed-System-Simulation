CREATE KEYSPACE mykeyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE stock_data (
    row_id TEXT PRIMARY KEY,
    rank FLOAT,
    trade_min TEXT,
    id_stock TEXT,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT,
    a1_v FLOAT,
    a2_v FLOAT,
    a3_v FLOAT,
    a4_v FLOAT,
    a5_v FLOAT,
    b1_v FLOAT,
    b2_v FLOAT,
    b3_v FLOAT,
    b4_v FLOAT,
    b5_v FLOAT,
    a1_p FLOAT,
    a2_p FLOAT,
    a3_p FLOAT,
    a4_p FLOAT,
    a5_p FLOAT,
    b1_p FLOAT,
    b2_p FLOAT,
    b3_p FLOAT,
    b4_p FLOAT,
    b5_p FLOAT,
    total_ask FLOAT,
    total_bid FLOAT,
    average_ask FLOAT,
    average_bid FLOAT,
    cjcs FLOAT,
    target FLOAT
);
kubectl cp cleaned_dataset.csv cassandra-0:/data.csv
kubectl exec -it cassandra-0 -- cqlsh -e "COPY mykeyspace.stock_data (row_id, rank, trade_min, id_stock, open, close, high, low, volume, a1_v, a2_v, a3_v, a4_v, a5_v, b1_v, b2_v, b3_v, b4_v, b5_v, a1_p, a2_p, a3_p, a4_p, a5_p, b1_p, b2_p, b3_p, b4_p, b5_p, total_ask, total_bid, average_ask, average_bid, cjcs, target) FROM '/data.csv' WITH HEADER = TRUE;"
kubectl exec -it cassandra-0 -- cqlsh -e "COPY mykeyspace.stock_data (row_id, rank, trade_min, id_stock, open, close, high, low, volume, a1_v, a2_v, a3_v, a4_v, a5_v, b1_v, b2_v, b3_v, b4_v, b5_v, a1_p, a2_p, a3_p, a4_p, a5_p, b1_p, b2_p, b3_p, b4_p, b5_p, total_ask, total_bid, average_ask, average_bid, cjcs, target) FROM '/data.csv' WITH HEADER = TRUE AND DELIMITER = ',';"
DROP TABLE mykeyspace.stock_data;



kubectl exec -it cassandra-0 -- cqlsh -e "CONSISTENCY QUORUM; COPY mykeyspace.stock_data (row_id, rank, trade_min, id_stock, open, close, high, low, volume, a1_v, a2_v, a3_v, a4_v, a5_v, b1_v, b2_v, b3_v, b4_v, b5_v, a1_p, a2_p, a3_p, a4_p, a5_p, b1_p, b2_p, b3_p, b4_p, b5_p, total_ask, total_bid, average_ask, average_bid, cjcs, target) FROM '/data.csv' WITH HEADER = TRUE AND DELIMITER = ',';"