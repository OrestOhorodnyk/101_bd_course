import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import datetime
import os

file_in_path = f"{os.getcwd()}/train.csv.gz"
print(file_in_path)
i = 1
t = datetime.datetime.now()
data = pd.read_csv(file_in_path, nrows=5, compression='gzip', error_bad_lines=False)
stat_info = os.stat(file_in_path)
table = pa.Table.from_pandas(data)
print(type(table.schema))
s = table.schema
# pq.write_table(table,  f'{os.getcwd()}/example.parquet')

with pq.ParquetWriter(f'{os.getcwd()}/example.parquet', s) as writer:
    for df in pd.read_csv(file_in_path, compression='gzip', chunksize=1000000):
        table = pa.Table.from_pandas(df)
        writer.write_table(table)
        print(i)
        i += 1
print(f'time to convert {datetime.datetime.now() -t }')
# table2 = pq.read_table(f'{os.getcwd()}/example.parquet')
# print(table2.to_pandas())
