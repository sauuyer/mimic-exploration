import os
import pandas as pd
import sqlite3

csv_dict = {}

#  sqlite database
conn = sqlite3.connect('mimiciii.db')  # establish and connect to the database
cursor = conn.cursor()

#  establish the locations of the mimic iii database files, which are natively in csv format
mimic_path = './mimic-iii-clinical-database-demo-1.4/'
for f in os.listdir(mimic_path):
    if f.endswith('.csv'):
        full_path = mimic_path + f
        csv_dict[f[:-4]] = pd.read_csv(full_path)

# make each one of the csvs stored in the csv_dict a table in the mimiciii.db, calling each table
# the key dict value, which is based on the original csv file name

for key in csv_dict:
    # print(key)
    # print(csv_dict[key])
    csv_dict[key].to_sql(key, conn, if_exists='replace', index=False)

# confirm that all tables are in the db
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())