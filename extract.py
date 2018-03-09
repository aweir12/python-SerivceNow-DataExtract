import requests
import pandas as pd
import mysql.connector
import snparams as param # See snparams.py for configuration
from sqlalchemy import create_engine


# Import parameters from snparams.py
user = param.username
pwd = param.password
headers = param.headers
url = param.baseURL
engine = create_engine(param.mysqlConn)


# Read snTables.csv to get a list of target table names, and ServiceNow table names
tables = pd.read_csv('snTables.csv')


# Accept ServiceNow tablename and return a dataframe containing all data in that table 
def getSNData(url):
    urlParams = '?sysparm_exclude_reference_link=true&sysparm_limit=1000&sysparm_offset='
    offset = 0
    check = 1
    baseUrl = url + urlParams + str(offset)
    
    r =  requests.get(baseUrl, auth=(user, pwd), headers=headers)
    offset = len(r.json()['result'])
    df= pd.DataFrame(r.json()['result'])
    
    while offset % 1000 == 0:
        r =  requests.get(url + urlParams + str(offset), auth=(user, pwd), headers=headers)
        df = df.append(pd.DataFrame(r.json()['result']))
        offset = offset + len(r.json()['result'])
    r =  requests.get(url + urlParams + str(offset), auth=(user, pwd), headers=headers)
    df = df.append(pd.DataFrame(r.json()['result']))
    return df


# Iterate through list of tables
for index, row in tables.iterrows():
    df = getSNData(url + row['url']) # Return a dataframe containing ServiceNow data
    df.to_sql(row['tableName'],engine,chunksize=2000) # Output dataframe to MYSQL database

