import sqlite3
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

# return subject_id, insurance, diagnosis, and admission_location for admits who where diagnosed with sepsis
cursor.execute('SELECT subject_id, insurance, diagnosis, admission_location FROM admissions WHERE diagnosis LIKE'
               '"%SEPSIS%";')
#print(cursor.fetchall())

# using the pandas read_sql_query function in order to get a dataframe read-out
# returning microbiology events where the specimen type is blood culture and the dilution value is greater than or
# equal to 6
pd.read_sql_query('SELECT row_id, spec_itemid, dilution_value, org_name FROM MICROBIOLOGYEVENTS WHERE spec_type_desc = "BLOOD CULTURE" AND dilution_value >= 6;', conn)

# pull together subject date of birth (from the patients table) with insurance and marital statuses (from admissions table)
# only including subjects who are either single or widowed
pd.read_sql_query('''SELECT patients.subject_id, marital_status, dob, insurance 
FROM patients INNER JOIN admissions 
ON patients.subject_id = admissions.subject_id 
WHERE marital_status = "WIDOWED" OR marital_status = "SINGLE";''', conn)

# perform a left join to link proceedure names in the proceedure_icds to the icd codes present
# in the proceedure_events table, and present these proceedure names with specific admission instances.
pd.read_sql_query('''SELECT admissions.subject_id, discharge_location, proceedure_icd.short_title 
FROM admissions 
LEFT JOIN proceedure_events ON admissions.subject_id = proceedure_events.subject_id
LEFT JOIN proceedure_icd ON proceedure_events.icd9_code = proceedure_icd.icd9_code;''', conn)

# pull the proceedures that have occured more than 10 times and show the number of occurances for each event
pd.read_sql_query('''SELECT proceedure_icd.short_title, proceedure_events.icd9_code, COUNT(proceedure_events.icd9_code) 
FROM proceedure_events
LEFT JOIN proceedure_icd on proceedure_events.icd9_code = proceedure_icd.icd9_code
GROUP BY proceedure_events.icd9_code
HAVING COUNT(proceedure_events.icd9_code) > 10;''', conn)

print('*****done!*****')


