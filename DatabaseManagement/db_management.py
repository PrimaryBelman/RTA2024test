# -*- coding: utf-8 -*-
"""
Created on Fri Sept  27 16:20:00 2024

@author: Pranav Belmannu
"""

import pandas as pd
import pymysql
from elasticsearch_serverless import Elasticsearch, helpers

# MySQL connection details to my local instance
mySQLuser = 'root'
mySQLpasswd = 'Belman@30'
mySQLhost = '127.0.0.1'
mySQLport = 3306
mySQLdatabase = 'company_address'

# =============================================================================
# I had to do create the new JSON files because they threw an error when I
# manually imported them. I wrote the main code and beautified it using ChatGPT.
# =============================================================================

# Read and transform company data
company = pd.read_json('company_table.json')
company.to_json('company_table_new.json', orient="records")

# Read and transform address data
address = pd.read_json('address_table.json')
address_transposed = address.transpose().reset_index(drop=True).drop('index',
                                                                     axis=1)
address_transposed.to_json('address_table_new.json', orient="records")

# Function to import JSON data into MySQL
def import_json_to_mysql(json_file, table_name):
    try:
        # Establish MySQL connection
        connection = pymysql.connect(
            user=mySQLuser,
            passwd=mySQLpasswd,
            host=mySQLhost,
            port=mySQLport,
            db=mySQLdatabase
        )
        cursor = connection.cursor()

        # Read JSON data into a DataFrame
        data = pd.read_json(json_file)

        # Insert each row into the MySQL table based on specific columns
        for _, row in data.iterrows():
            if table_name == 'company_table':
                insert_query = """
INSERT INTO company_table (corp_id, companyName, category, categoryCode, 
                           license1StartDate)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
companyName = VALUES(companyName),
category = VALUES(category),
categoryCode = VALUES(categoryCode),
license1StartDate = VALUES(license1StartDate);
"""
                cursor.execute(insert_query, (row['corp_id'], 
                                              row['companyName'],
                                              row['category'], 
                                              row['categoryCode'], 
                                              row['license1StartDate']))

            elif table_name == 'address_table':
                insert_query = """
        INSERT INTO address_table (corp_id, addressLine, addressCity, 
                                   addressRegion, addressPostal, addressCountry)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        addressLine = VALUES(addressLine),
        addressCity = VALUES(addressCity),
        addressRegion = VALUES(addressRegion),
        addressPostal = VALUES(addressPostal),
        addressCountry = VALUES(addressCountry);
        """
                cursor.execute(insert_query, (row['corp_id'], 
                                              row['addressLine'],
                                              row['addressCity'], 
                                              row['addressRegion'], 
                                              row['addressPostal'], 
                                              row['addressCountry']))

        # Commit the transaction
        connection.commit()

    except pymysql.MySQLError as e:
        print(f"Error while connecting to MySQL: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Import the data into MySQL
import_json_to_mysql('company_table_new.json', 'company_table')
import_json_to_mysql('address_table_new.json', 'address_table')

# =============================================================================
# This code creates a pipeline from mysql to Elasticsearch. What I do here 
# is select query for each table and 
# =============================================================================
# Task 2: Data Pipelining
# Establish MySQL connection again for further operations
try:
    connection = pymysql.connect(
        user=mySQLuser,
        passwd=mySQLpasswd,
        host=mySQLhost,
        port=mySQLport,
        db=mySQLdatabase
    )
    cursor = connection.cursor()

    # Fetch company data
    query_1 = 'SELECT * FROM company_address.company_table_new'
    company_data = pd.read_sql(query_1, con=connection)

    # Convert license date to datetime
    company_data['license1StartDate'] = pd.to_datetime(company_data
                                                       ['license1StartDate'],
                                                       format="%Y-%m-%d",
                                                       errors='coerce')
    company_data['license1StartDate'].fillna(pd.Timestamp('1900-01-01'), 
                                             inplace=True)

    # Fetch address data
    query_2 = 'SELECT * FROM company_address.address_table_new'
    address_data = pd.read_sql(query_2, con=connection)

finally:
    cursor.close()
    connection.close()

# Elasticsearch setup
client = Elasticsearch(
    "https://e2004a85c7a24f8f91993ffa72615cee.es.us-west-2.aws.elastic.cloud:443",
    api_key="WG5ZeGZaRUJFU1V6NUJMdC1RVk06UGFQVVdyeU1SWEtzNzRoZGJTMmtSUQ=="
)

# Check if Elasticsearch is connected
try:
    client.info()
except Exception as e:
    print(f"Error connecting to Elasticsearch: {e}")

# Create indices if they do not exist
index_name1 = 'company'
if not client.indices.exists(index=index_name1):
    client.indices.create(index=index_name1)

index_name2 = 'address'
if not client.indices.exists(index=index_name2):
    client.indices.create(index=index_name2)

# Bulk index address data
def generate_address_data():
    for i, row in address_data.iterrows():
        yield {
            "_index": index_name2,
            "_id": row['id'],  # Assuming 'id' is the primary key
            "_source": row.to_dict(),
        }

# Bulk index company data
def generate_company_data():
    for i, row in company_data.iterrows():
        yield {
            "_index": index_name1,
            "_id": row['id'],  # Assuming 'id' is the primary key
            "_source": row.to_dict(),
        }

# Index the data in bulk
helpers.bulk(client, generate_address_data())
helpers.bulk(client, generate_company_data())

# Task 3: Synchronization changes code
def sync_with_elasticsearch():
    try:
        # Establish MySQL connection
        connection = pymysql.connector.connect(host='localhost',
                                             database='company_address',
                                             user='root',
                                             password='yourpassword')
        cursor = connection.cursor(dictionary=True)
        
        # Fetch unsynchronized changes from the unified sync log table
        cursor.execute("SELECT * FROM sync_log WHERE sync_status IS NULL")
        sync_log_entries = cursor.fetchall()

        # Process each log entry to synchronize with Elasticsearch
        for log_entry in sync_log_entries:
            table_name = log_entry['table_name']
            record_id = log_entry['record_id']
            operation_type = log_entry['operation_type']

            # Fetch the full record from the affected table
            if table_name == 'company_table_new':
                cursor.execute("SELECT * FROM company_table_new WHERE corp_id = %s",
                               (record_id,))
            elif table_name == 'address_table_new':
                cursor.execute("SELECT * FROM address_table_new WHERE corp_id = %s", 
                               (record_id,))
            
            record = cursor.fetchone()

            # Prepare Elasticsearch data based on the table
            if table_name == 'company_table_new':
                client.index(index='company_table', id=record_id, document=record)
            elif table_name == 'address_table_new':
                client.index(index='address_table', id=record_id, document=record)

            # Optionally update the sync status to avoid reprocessing
            cursor.execute("UPDATE sync_log SET sync_status = 'SYNCHRONIZED' WHERE id = %s", (log_entry['id'],))
        connection.commit()

    except Exception as e:
        print(f"Error synchronizing with Elasticsearch: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

