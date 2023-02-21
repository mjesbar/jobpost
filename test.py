import pandas

partitions = 

parquet = pandas.read_parquet('data/data2023-02-21.parquet.gz',
                              'pyarrow')

print(parquet['id'])
print(parquet['id'].drop_duplicates().count())
print(parquet['id'].count())

