import pandas as pd

# how does the distribution of admit times differ for emergency vs non emergency admissions?

# seperate emergency from non emergency admit types
emergencies = pd.read_sql_query("select all", conn)
non_emergencies = pd.read_sql_query