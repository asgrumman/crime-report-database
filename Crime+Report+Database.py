#!/usr/bin/env python
# coding: utf-8

# ## Crime Report Database
# 
# This project involves the creation of a database, schema, tables, and user management privileges with proper datatypes in Postgres from a CSV file on crime data.

# In[356]:


"""Import packages and create crime database"""

import psycopg2
import csv
conn = psycopg2.connect(dbname="dq",user="dq")
cur = conn.cursor()
conn.autocommit = True
cur.execute("CREATE DATABASE crime_db;")
conn.close()


# In[334]:


"""Connect to Crime database and create schema"""

conn = psycopg2.connect(dbname="crime_db",user="dq")
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SCHEMA crimes;")


# In[335]:


"""Read CSV file"""

with open("boston.csv") as file:
    reader = csv.reader(file)
    col_headers = next(file)
    first_row = next(file)


# In[336]:


"""Create function that takes in csv file and column
   index and returns set of distinct values in that 
   column"""

def get_col_value_set(csv_filename, col_index):
    with open(csv_filename) as f:
        next(f)
        reader = csv.reader(f)
        unique_col_values = set()
        for row in reader:
            col_val = row[col_index]
            if col_val not in unique_col_values:
                unique_col_values.add(col_val)
        return unique_col_values


# In[337]:


"""Print number of distinct values in each column in 
   boston crime CSV"""

for i in range(7):
    lengths = len(get_col_value_set("boston.csv",i))
    print("column",i,":",lengths)


# In[338]:


"""Look at column headers to find indices"""

print(col_headers)


# In[339]:


"""Find max character length in description column"""

#description index = 2
max_len = 0
for word in get_col_value_set("boston.csv",2):
    max_len = len(word) if len(word) > max_len else max_len
print(max_len)


# In[340]:


print(col_headers)
print(first_row)


# In[341]:


"""Let's look at the data in the first couple of rows 
   to help determine datatypes"""

with open("boston.csv") as file:
    reader = csv.reader(file)
    col_headers = next(file)
    first_row = next(file)
    second = next(file)
    third = next(file)
    fourth = next(file)
print(second)
print(third)
print(fourth)


# ### Determining Datatypes
# 
# Feel good about all the existing column names, they're short but accurate and descriptive
# 
# 1. incident_number: bigint: always going to be an     integer, but could have a very high number of incidents
# 
# 2. offense_code: smallint -- will always be an integer 4 characters long
# 
# 3. description: varchar(200) -- we know that the max length is 58, but descriptions can vary, so let's give some wiggle room for a max # of characters at 200
# 
# 4. date: date
# 
# 5. day_of_the_week: enum -- will always be one of the 7 days of the week
# 
# 6. lat: decimal(10,8) -- always 2 digits before the decimal point and 8 digits after
# 
# 7. long: decimal(10,8) -- always 2 digits before the decimal point and 8 digits after

# In[342]:


"""Create table for crime data with given columns
   and assign datatypes from above accordingly"""

cur.execute("""CREATE TYPE enum_day AS ENUM(
            'Monday','Tuesday','Wednesday',
            'Thursday','Friday','Saturday','Sunday');""")
cur.execute("""CREATE TABLE crimes.boston_crimes (
           incident_number bigint PRIMARY KEY,
           offense_code smallint,
           description varchar(200), date date,
           day_of_the_week enum_day,
           lat decimal(10,8),
           long decimal(10,8));""")


# In[343]:


"""Copy data from CSV file into table in database"""

with open("boston.csv") as crimefile:
    cur.copy_expert("""COPY crimes.boston_crimes 
                        FROM STDIN WITH CSV HEADER;""",
                        crimefile)


# In[344]:


"""Revoke all privileges from public schema in database"""

cur.execute("REVOKE ALL ON SCHEMA public FROM public;")
cur.execute("""REVOKE ALL ON DATABASE crime_db FROM public;""")


# In[345]:


"""Create readonly and readwrite groups and grant
privileges to each group"""

cur.execute("""CREATE GROUP readonly NOLOGIN;""")
cur.execute("""CREATE GROUP readwrite NOLOGIN;""")
cur.execute("""GRANT CONNECT ON DATABASE crime_db TO 
               readonly,readwrite;""")
cur.execute("""GRANT USAGE ON SCHEMA crimes TO readonly,
               readwrite;""")
cur.execute("""GRANT SELECT ON ALL TABLES IN SCHEMA
               crimes TO readonly;""")
cur.execute("""GRANT SELECT,INSERT,DELETE,UPDATE ON
               ALL TABLES IN SCHEMA crimes TO
               readwrite;""")


# In[346]:


"""Create data analyst and data scientist roles and 
   assign them group privileges"""

cur.execute("""CREATE USER data_analyst WITH
               PASSWORD 'secret1';""")
cur.execute("""GRANT readonly TO data_analyst;""")
cur.execute("""CREATE USER data_scientist WITH
               PASSWORD 'secret2';""")
cur.execute("""GRANT readwrite TO data_scientist;""")


# In[360]:


"""Close connection"""

conn.close()


# In[384]:


"""Reconnect to test that privileges have been assigned correctly"""

conn = psycopg2.connect(dbname="crime_db",user="dq")
cur = conn.cursor()

# Test 1: Readwrite/DS privilege check
readwrite_priv = cur.execute("""SELECT grantee, privilege_type
               FROM information_schema.table_privileges
               WHERE grantee IN ('readwrite');""")
for user in cur:
    print(user)
conn.close()


# In[378]:


# Test 2: Check privileges of Readonly/Analyst by
# connecting to database and trying to insert data

conn = psycopg2.connect("""dbname=crime_db 
                           user=data_analyst
                           password='secret1'""")
cur = conn.cursor()
cur.execute("""INSERT INTO crimes.boston_crimes VALUES
               (2,
               1402,
               'VANDALISM',
               '2018-08-21',
               'Tuesday',
               42.30682138,
               -71.06030035)""")
conn.close()


# In[383]:


# Test 3: Finally, just check to make sure that data
# is stored in the tables the way it should be

conn = psycopg2.connect("dbname=crime_db user=dq")
cur = conn.cursor()

cur.execute("""SELECT * FROM crimes.boston_crimes
               LIMIT 10;""")
for row in cur:
    print(row)
conn.close()


# In[385]:


"""Yay! Commit changes and close the connection"""

conn = psycopg2.connect("dbname=crime_db user=dq")
conn.commit()
conn.close()


# In[ ]:




