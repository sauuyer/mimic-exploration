import sqlite3
import os
import pandas as pd
import sqlite3

csv_dict = {}

#  sqlite database
conn = sqlite3.connect('lecture.db')  # establish and connect to the database
cursor = conn.cursor()

#  establish the locations of the mimic iii database files, which are natively in csv format
mimic_path = './mimic-iii-clinical-database-demo-1.4/'
for f in os.listdir(mimic_path):
    if f.endswith('.csv'):
        full_path = mimic_path + f
        csv_dict[f[:-4]] = pd.read_csv(full_path)

for key in csv_dict:
    # print(key)
    # print(csv_dict[key])
    csv_dict[key].to_sql(key, conn, if_exists='replace', index=False)




