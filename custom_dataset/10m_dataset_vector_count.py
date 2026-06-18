import pyarrow.parquet as pq

# Read only metadata
parquet_file = pq.ParquetFile('/home/debian/vectordataset/custom_512d_10m/custom_512d_10m/train.parquet')

# Get total rows
num_rows = parquet_file.metadata.num_rows
print(f"Total rows: {num_rows}")

# More details
print(f"Number of row groups: {parquet_file.num_row_groups}")
print(f"Schema: {parquet_file.schema}")
