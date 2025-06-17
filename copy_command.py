import subprocess

delimiter = '\t'
command = f"""
kubectl exec -it cassandra-0 -- cqlsh -e "COPY mykeyspace.stock_data (
    row_id, rank, trade_min, id_stock, open, close, high, low, volume,
    a1_v, a2_v, a3_v, a4_v, a5_v, b1_v, b2_v, b3_v, b4_v, b5_v,
    a1_p, a2_p, a3_p, a4_p, a5_p, b1_p, b2_p, b3_p, b4_p, b5_p,
    total_ask, total_bid, average_ask, average_bid, cjcs, target
) FROM '/data.csv' WITH HEADER = TRUE AND DELIMITER = '{delimiter}';"
"""
subprocess.run(command, shell=True)