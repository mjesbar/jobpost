import pandas, os, datetime
print()


today = datetime.date.today()
old_partitions = list() 
new_partition : str

data_dir = os.listdir('data/')

for file in data_dir:
    if ('csv' in file) & (f'{today}' in file):
        new_partition = file
        print("new partition:\n\t", new_partition)
        print("==============================================\nold_partitions:")

for file in data_dir:
    if ('csv' in file) & (f'{today}' not in file):
        old_partitions.append(file)
        print("\t",old_partitions[-1])

