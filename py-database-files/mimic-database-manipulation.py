import sqlite3
import pandas as pd

conn = sqlite3.connect('../mimiciii.db')
cursor = conn.cursor()

# return subject_id, insurance, diagnosis, and admission_location for admits who where diagnosed with sepsis
cursor.execute('SELECT subject_id, insurance, diagnosis, admission_location FROM ADMISSIONS WHERE diagnosis LIKE'
               '"%SEPSIS%";')
print(cursor.fetchall())

# using the pandas read_sql_query function in order to get a dataframe read-out
# returning microbiology events where the specimen type is blood culture and the dilution value is greater than or
# equal to 6
pd.read_sql_query('SELECT row_id, spec_itemid, dilution_value, org_name FROM MICROBIOLOGYEVENTS WHERE spec_type_desc = "BLOOD CULTURE" AND dilution_value >= 6;', conn)

# pull together subject date of birth (from the patients table) with insurance and marital statuses (from admissions table)
# only including subjects who are either single or widowed
pd.read_sql_query('''SELECT PATIENTS.subject_id, marital_status, dob, insurance 
FROM PATIENTS INNER JOIN ADMISSIONS 
ON PATIENTS.subject_id = ADMISSIONS.subject_id 
WHERE marital_status = "WIDOWED" OR marital_status = "SINGLE";''', conn)


# perform a left join to link procedure names in the procedure_icds to the icd codes present
# in the procedure_events table, and present these procedure names with specific admission instances.
pd.read_sql_query('''SELECT ADMISSIONS.subject_id, discharge_location, D_ICD_PROCEDURES.short_title 
FROM ADMISSIONS 
LEFT JOIN PROCEDURES_ICD ON ADMISSIONS.subject_id = PROCEDURES_ICD.subject_id
LEFT JOIN D_ICD_PROCEDURES ON PROCEDURES_ICD.icd9_code = D_ICD_PROCEDURES.icd9_code;''', conn)

# pull the procedures that have occured more than 10 times during emergency admissions and show the number of occurrences for each event
procedures_occuring_more_than_10_times = pd.read_sql_query('''SELECT D_ICD_PROCEDURES.short_title, PROCEDURES_ICD.icd9_code, COUNT(PROCEDURES_ICD.icd9_code) 
FROM PROCEDURES_ICD
LEFT JOIN D_ICD_PROCEDURES ON PROCEDURES_ICD.icd9_code = D_ICD_PROCEDURES.icd9_code
LEFT JOIN ADMISSIONS ON PROCEDURES_ICD.hadm_id = ADMISSIONS.hadm_id 
GROUP BY PROCEDURES_ICD.icd9_code
HAVING COUNT(PROCEDURES_ICD.icd9_code) > 10;''', conn)

print(procedures_occuring_more_than_10_times)

# how does the distribution of admit times differ for emergency vs non emergency admissions?

# 1) seperate emergency from non emergency admit types
emergencies = pd.read_sql_query('SELECT admission_type, admittime FROM ADMISSIONS WHERE admission_type = "EMERGENCY";', conn)
non_emergencies = pd.read_sql_query('SELECT admission_type, admittime FROM ADMISSIONS WHERE admission_type != "EMERGENCY";', conn)

print(emergencies)
print(non_emergencies)

# Which proceedures have associated lengths of time?

# Per insurance group, for patients within each group (all receiving medicare health insurance), how many providers are interacting with them and what types?
pd.read_sql_query('SELECT

;', conn)

print('*****done!*****')

# Data quality checks

# Per each table, which are the columns that have 100% filled out cells?

# Which table has the largest number of missing content per row?

# Which columns are most frequently not containing data?

# Columns show the highest degree of variance?


