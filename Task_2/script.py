import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import time
import numpy as np

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 10  # seconds

# 1. Reading Data from CSV
def read_csv(file_path):
    try:
        data = pd.read_csv(file_path)
        print("Data successfully read from CSV.")
        return data
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None

# 2. Transforming the Data with refined logic
def transform_data(data):
    try:
        data.columns = data.columns.str.strip()

        required_columns = ['BirthDate', 'FirstName', 'LastName', 'Salary', 'Department']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing column in data: {col}")

        data['FirstName'] = data['FirstName'].str.replace(r'[^a-zA-Z\s]', '', regex=True).str.strip()

        expected_columns_count = len(data.columns)
        for idx, row in data.iterrows():
            row_data = row.values
            non_nan_count = np.count_nonzero(~pd.isna(row_data))

            if non_nan_count != expected_columns_count:
                first_name_parts = row_data[1].split(' ')

                for col_idx in range(3, expected_columns_count):
                    if col_idx < len(row_data):
                        data.iat[idx, col_idx] = row_data[col_idx - 1]

                if len(first_name_parts) > 1:
                    data.at[idx, 'FirstName'] = first_name_parts[0]
                    data.at[idx, 'LastName'] = ' '.join(first_name_parts[1:])
                else:
                    data.at[idx, 'FirstName'] = first_name_parts[0]
                    data.at[idx, 'LastName'] = ''
            else:
                data['LastName'] = data['LastName'].str.replace(r'[^a-zA-Z\s]', '', regex=True).str.strip()

        data['BirthDate'] = pd.to_datetime(data['BirthDate'], format='%Y-%m-%d', errors='coerce')
        data['BirthDate'] = data['BirthDate'].dt.strftime('%d/%m/%Y')

        data['FullName'] = data['FirstName'] + ' ' + data['LastName']

        reference_date = datetime(2023, 1, 1)
        data['Age'] = pd.to_datetime(data['BirthDate'], format='%d/%m/%Y', errors='coerce').apply(
            lambda x: reference_date.year - x.year if pd.notna(x) else None)

        data['Age'] = data['Age'].fillna(0).astype(int)

        data['Salary'] = pd.to_numeric(data['Salary'], errors='coerce')
        data['Salary'] = data['Salary'].fillna(0).astype(int)
        data['SalaryBucket'] = pd.cut(data['Salary'], 
                                    bins=[-float('inf'), 50000, 100000, float('inf')], 
                                    labels=['A', 'B', 'C'], 
                                    right=False)

        data['Department'] = data['Department'].fillna('Unknown').replace('-', 'Unknown')

        data.drop(columns=['FirstName', 'LastName', 'BirthDate'], inplace=True)

        final_columns = ['EmployeeID', 'Department', 'Salary', 'FullName', 'Age', 'SalaryBucket']
        data = data[final_columns]

        print("Data successfully transformed.")
        return data
    except Exception as e:
        print(f"Error transforming data: {e}")
        return None


# 3. Loading the Data into MySQL with Retry Mechanism
def load_data(data):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            connection = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            cursor = connection.cursor()

            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    FullName VARCHAR(255),
                    Age INT,
                    Salary DECIMAL(10, 2),
                    SalaryBucket CHAR(1),
                    INDEX (FullName),
                    INDEX (SalaryBucket)
                )
            """)

            for _, row in data.iterrows():
                cursor.execute("""
                    INSERT INTO employees (FullName, Age, Salary, SalaryBucket)
                    VALUES (%s, %s, %s, %s)
                """, (row['FullName'], row['Age'], row['Salary'], row['SalaryBucket']))

            connection.commit()
            cursor.close()
            connection.close()

            print("Data successfully loaded into MySQL.")
            break

        except Error as e:
            print(f"Error loading data into MySQL: {e}")
            retries += 1
            if retries < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds... ({retries}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached. Could not connect to MySQL.")

# Running the ETL Pipeline
file_path = '/TASK_2/Dataset/employee_details.csv'
csv_data = read_csv(file_path)

if csv_data is not None:
    transformed_data = transform_data(csv_data)
    if transformed_data is not None:
        load_data(transformed_data)