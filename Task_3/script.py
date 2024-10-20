import os
import mysql.connector
from dotenv import load_dotenv
import logging
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Loading environment variables from the .env file
load_dotenv()

# Access the Production Database credentials
Pro_db_username = os.getenv('PROD_DB_USER')
Pro_db_password = os.getenv('PROD_DB_PASSWORD')
Pro_db_host = os.getenv('PROD_DB_HOST')
Pro_db_name = os.getenv('PROD_DB_NAME')

# Access the Development Database credentials
Dev_db_username = os.getenv('DEV_DB_USER')
Dev_db_password = os.getenv('DEV_DB_PASSWORD')
Dev_db_host = os.getenv('DEV_DB_HOST')
Dev_db_name = os.getenv('DEV_DB_NAME')

# Database configuration for production and development
PROD_DB = {
    'user': Pro_db_username,
    'password': Pro_db_password,
    'host': Pro_db_host,
    'database': Pro_db_name
}

DEV_DB = {
    'user': Dev_db_username,
    'password': Dev_db_password,
    'host': Dev_db_host,
    'database': Dev_db_name
}

# Database Connection
def connect_to_db(db_config):
    try:
        conn = mysql.connector.connect(**db_config)
        logging.info(f"Connected to database: {db_config['database']}")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        raise

# Creating table if it does not exist in the development database
def create_table_if_not_exists(conn, schema, table, headers):
    try:
        cursor = conn.cursor()
        columns = ", ".join([f"`{header}` VARCHAR(255)" for header in headers]) 
        query = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})"
        cursor.execute(query)
        conn.commit()
        logging.info(f"Table {schema}.{table} created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create table {schema}.{table}: {e}")
        raise

# Exporting data from production database to a CSV file
def export_to_csv(conn, schema, table, file_path):
    try:
        cursor = conn.cursor()
        query = f"SELECT * FROM {schema}.{table}"
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [i[0] for i in cursor.description]  
        
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  
            writer.writerows(rows)    

        logging.info(f"Data from {schema}.{table} exported to {file_path}")
    except Exception as e:
        logging.error(f"Failed to export data from {schema}.{table}: {e}")
        raise

# Importing data from CSV file into the development database
def import_from_csv(conn, schema, table, file_path):
    try:
        cursor = conn.cursor()

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  

            create_table_if_not_exists(conn, schema, table, headers)

            for row in reader:
                placeholders = ', '.join(['%s'] * len(row))
                query = f"INSERT IGNORE INTO {schema}.{table} VALUES ({placeholders})"
                cursor.execute(query, row)
            conn.commit()

        logging.info(f"Data from {file_path} imported into {schema}.{table}")
    except Exception as e:
        logging.error(f"Failed to import data into {schema}.{table}: {e}")
        raise

# Migrating data from production to development database
def migrate_data(migration_dict):
    prod_conn = None
    dev_conn = None
    try:
        prod_conn = connect_to_db(PROD_DB)
        dev_conn = connect_to_db(DEV_DB)

        for entry in migration_dict:
            for schema, table in entry.items():
                
                csv_file = f"./data/{schema}_{table}.csv"
                
                export_to_csv(prod_conn, schema, table, csv_file)
                
                import_from_csv(dev_conn, schema, table, csv_file)

    except Exception as e:
        logging.error(f"Migration process failed: {e}")
    finally:
        if prod_conn:
            prod_conn.close()
        if dev_conn:
            dev_conn.close()
        logging.info("Migration process completed")

# Defining the Input Dictionary to Run the Script
tables_to_migrate = [{"data_engineering": "customer_table"}, {"app_development": "timesheet_data"}]

# Executing the migration
migrate_data(tables_to_migrate)
