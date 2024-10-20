
import pandas as pd
import re
from datetime import datetime

# Task-1- Loading the Dataset
file_Path = '/TASK_1/Dataset/CustomerData.csv'
df = pd.read_csv(file_Path)

# Task-2- Handle Missing Values:
df['Phone'].fillna('Unknown', inplace=True)

df['Email'].fillna('myuser@example.com', inplace=True)

# Task-3- Standardize Formatting:
def clean_phone_number(x):
    return re.sub(r'\D', '', str(x))

df['Phone'] = df['Phone'].apply(clean_phone_number)

df['Email'] = df['Email'].str.lower()

#Task-4- Extract Information:
current_year = 2023
df['Age'] = current_year - df['BirthYear']

#Task-5- Categorize Data:
def customer_category(age):
    if age<30:
        return "Young"
    elif age >= 30 and age <= 50:
        return "Middle-aged"
    else:
        return "Senior"

df['CustomerType'] = df['Age'].apply(customer_category)
      
#Task-6 - Saving the new Dataset
new_file_path = '/TASK_1/Dataset/Cleaned_CustomerData.csv'
df.to_csv(new_file_path, index=False)

#Task-7 - Displaying the Summary statistics
summary_stats = df.describe(include='all')
print("Summary statistics: ")
print(summary_stats)

print("\nAdditional Metrics: ")
print(f"Mean Age of Customer: {df['Age'].mean()}")
print(f"Median Age of Customer: {df['Age'].median()}")
print(f"Standard Deviation Age of Customer: {df['Age'].std()}")





