# 1.1 to read/extract the following JSON files according to the specifications found in the mapping document.

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("CreditCardETL") \
    .getOrCreate()

# Read JSON files
customer_df = spark.read.option("multiline", True).json("cdw_sapp_customer.json")
creditcard_df = spark.read.option("multiline", True).json("cdw_sapp_credit.json")
branch_df = spark.read.option("multiline", True).json("cdw_sapp_branch.json")

# Show schema and preview data
print("branch_df")
branch_df.printSchema()
branch_df.show(5)
print("creditcard_df")
creditcard_df.printSchema()
creditcard_df.show(5)
print("customer_df")
customer_df.printSchema()
customer_df.show(5)

from pyspark.sql.functions import col, lit, when, format_string, substring, lpad
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Load the JSON file into a DataFrame
branch_df = spark.read.option("multiline", True).json("cdw_sapp_branch.json")

# Data transformation according to the mapping document
branch_df_transformed = branch_df \
    .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast("int")) \
    .withColumn("BRANCH_NAME", col("BRANCH_NAME").cast("string")) \
    .withColumn("BRANCH_STREET", col("BRANCH_STREET").cast("string")) \
    .withColumn("BRANCH_CITY", col("BRANCH_CITY").cast("string")) \
    .withColumn("BRANCH_STATE", col("BRANCH_STATE").cast("string")) \
    .withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(), lit("999999")).otherwise(lpad(col("BRANCH_ZIP"), 5, '0')).cast("string")) \
    .withColumn(
        "BRANCH_PHONE",
        format_string(
            "(%s)%s-%s",
            substring(col("BRANCH_PHONE").cast("string"), 1, 3),
            substring(col("BRANCH_PHONE").cast("string"), 4, 3),
            substring(col("BRANCH_PHONE").cast("string"), 7, 4)
        ).cast("string")
    ) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast("timestamp"))

# Show transformed DataFrame
branch_df_transformed.show()

print("branch_df_transformed")
branch_df_transformed.printSchema()





# Explanation:
# Column Casting:

# Used .cast("string") instead of .cast("varchar(64)") since Spark uses string for text data type.
# Phone Number Formatting:

# Ensured phone number formatting remains consistent and readable.
# Null Handling:

# Kept the when condition for handling NULL in BRANCH_ZIP and replaced varchar(64) with string.
# This version maintains the functionality of the original while improving readability and adhering to common practices in PySpark data frame manipulations.






from pyspark.sql.functions import col, concat, lpad
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lpad

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Load the JSON file into a DataFrame
creditcard_df = spark.read.option("multiline", True).json("cdw_sapp_credit.json")

# Transform the DataFrame
creditcard_df_transformed = creditcard_df \
    .withColumn("DAY", lpad(col("DAY").cast("string"), 2, '0')) \
    .withColumn("MONTH", lpad(col("MONTH").cast("string"), 2, '0')) \
    .withColumn("CUST_CC_NO", col("CREDIT_CARD_NO").cast("string")) \
    .withColumn("TIMEID", concat(
        col("YEAR"),
        col("MONTH"),
        col("DAY")
    ).cast("string")) \
    .withColumn("CUST_SSN", col("CUST_SSN").cast("int")) \
    .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast("int")) \
    .withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast("string")) \
    .withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast("double")) \
    .withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast("int"))

# Show transformed DataFrame
creditcard_df_transformed.show()

# Print schema of the transformed DataFrame
creditcard_df_transformed.printSchema()





# Column Casting:

# Changed .cast("varchar(64)") to .cast("string") since Spark uses string for text data types.
# Time ID Column:

# Used concat and lpad to create the TIMEID column by concatenating the YEAR, MONTH, and DAY columns and padding MONTH and DAY with leading zeros to ensure two digits.




from pyspark.sql.functions import col, initcap, lower, concat, lit, format_string, substring



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower, concat, lit, format_string, substring

# # Initialize Spark session
# spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Load the JSON file into a DataFrame
# customer_df = spark.read.option("multiline", True).json("cdw_sapp_customer.json")


# customer table("dw_sapp_customer")
customer_df_transformed = customer_df \
    .withColumn("SSN", col("SSN").cast("int")) \
    .withColumn("FIRST_NAME", initcap(col("FIRST_NAME")).cast("varchar(64)")) \
    .withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")).cast("varchar(64)")) \
    .withColumn("LAST_NAME", initcap(col("LAST_NAME")).cast("varchar(64)")) \
    .withColumn("CREDIT_CARD_NO", col("CREDIT_CARD_NO").cast("varchar(64)")) \
    .withColumn("FULL_STREET_ADDRESS", concat(col("STREET_NAME"), lit(", "), col("APT_NO")).cast("varchar(64)")) \
    .withColumn("CUST_CITY", col("CUST_CITY").cast("varchar(64)")) \
    .withColumn("CUST_STATE", col("CUST_STATE").cast("varchar(64)")) \
    .withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast("varchar(64)")) \
    .withColumn("CUST_ZIP", col("CUST_ZIP").cast("varchar(64)")) \
    .withColumn("CUST_PHONE", format_string("(%s)%s-%s", 
                                             substring(col("CUST_PHONE").cast("varchar(64)"), 1, 3), 
                                             substring(col("CUST_PHONE").cast("varchar(64)"), 4, 3), 
                                             substring(col("CUST_PHONE").cast("varchar(64)"), 4, 4)).cast("varchar(64)")) \
    .withColumn("CUST_EMAIL", col("CUST_EMAIL").cast("varchar(64)")) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast("timestamp"))

# Show transformed DataFrame
customer_df_transformed.show()

# Print schema of the transformed DataFrame
customer_df_transformed.printSchema()


# Column Casting:

# Changed .cast("varchar(64)") to .cast("string") since Spark uses string for text data types.
# String Manipulations:

# Used initcap for capitalizing the first letter of FIRST_NAME and LAST_NAME.
# Used lower for converting MIDDLE_NAME to lowercase.
# Concatenated STREET_NAME and APT_NO with a comma and space in between for FULL_STREET_ADDRESS.
# Phone Number Formatting:

# Used format_string and substring to format CUST_PHONE into the desired format.





# 1.2
# Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL), perform the following
# Load Data into SQL:

# Write Python and PySpark code to load transformed data into the database.
# How to write DataFrame to MySQL:

# MySQL configurations
mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
mysql_properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Write data to MySQL
# branch_df_transformed.write.jdbc(url=mysql_url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=mysql_properties)
creditcard_df_transformed.write.jdbc(url=mysql_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=mysql_properties)
# customer_df_transformed.write.jdbc(url=mysql_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=mysql_properties)




# # 2. Functional Requirements - Application Front-End
# # Once data is loaded into the database, we need a front-end (console/text menu) to see/display data. For that, create a console-based Menu (Python program) to satisfy Functional Requirements 2 (2.1 and 2.2). 
# # Here is a good walkthrough on what a console based program looks like: https://www.geeksforgeeks.org/how-to-make-a-todo-list-cli-application-using-python/
# # You must be able to run it from a console.
# 2.1
import mysql.connector
import re

# Connect to MySQL Database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="creditcard_capstone"
)
cursor = db.cursor()

def main_menu():

    def zipcode():
        zip_code = input("Enter ZIP code (5 digits): ")
        if re.match(r'^\d{5}$', zip_code):
            return zip_code
        else:
            print("Invalid ZIP code format. Please enter a 5-digit ZIP code.")
            return None
    
    def month_year():
        try:
            month = input("Enter month (MM): ")
            year = input("Enter year (YYYY): ")
            if (month.isdigit() and 1 <= int(month) <= 12 and len(month) == 2 and 
                year.isdigit() and len(year) == 4):
                return int(month), int(year)
            else:
                print("Invalid input. Please enter a valid month (MM) and year (YYYY).")
                return None, None       
        except ValueError:
            print("Invalid month/year format. Please enter as MM-YYYY.")
            return None, None
    
    def customer_transaction_zipcode_month_year():
        customer_zipcode = zipcode()
        if customer_zipcode is None:
            return
        month, year = month_year()
        if month is None or year is None:
            return
        
        query = f"""
        SELECT CUST_ZIP, TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE, YEAR, MONTH, DAY
        FROM CDW_SAPP_CUSTOMER
        JOIN CDW_SAPP_CREDIT_CARD ON CDW_SAPP_CUSTOMER.SSN = CDW_SAPP_CREDIT_CARD.CUST_SSN
        WHERE CDW_SAPP_CUSTOMER.CUST_ZIP = '{customer_zipcode}'
        AND CDW_SAPP_CREDIT_CARD.MONTH = {month}
        AND CDW_SAPP_CREDIT_CARD.YEAR = {year}
        ORDER BY CDW_SAPP_CREDIT_CARD.DAY DESC;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            print("\nTransactions:")
            for row in results:
                print(f"ZIP: {row[0]}, Transaction ID: {row[1]}, Type: {row[2]}, Value: ${row[3]}, Year: {row[4]}, Month: {row[5]}, Day: {row[6]}")
        else:
            print("\nNo transactions found for the specified criteria.")

    customer_transaction_zipcode_month_year()
    cursor.close()
    db.close()

if __name__ == "__main__":
    main_menu()




        # 2.2

import mysql.connector
import re
from datetime import datetime

# Connect to MySQL Database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="creditcard_capstone"
)
cursor = db.cursor()

def check_account_details():
    customer_id = input("Please enter the customer ID: ")
    
    query = "SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s"
    cursor.execute(query, (customer_id,))
    result = cursor.fetchone()
    
    if result:
        print("Customer Account Details:")
        print(result)
    else:
        print("No account found for the given customer ID.")

def modify_account_details():
    customer_id = input("Please enter the customer ID: ")
    
    print("What would you like to update?")
    print("1. First Name")
    print("2. Last Name")
    print("3. Street Address")
    print("4. Phone Number")
    print("5. Zip Code")
    print("6. Middle Name")
    print("7. Email")
    print("8. Country")
    print("9. State")
    print("10. City")
    print("11. SSN")
    print("12. Full street address")
    
    choice = input("Please enter the number corresponding to the field you want to update: ")
    new_value = input("Please enter the new value: ")

    field_map = {
        '1': 'FIRST_NAME',
        '2': 'LAST_NAME',
        '3': 'CUST_STREET',
        '4': 'CUST_PHONE',
        '5': 'CUST_ZIP',
        '6': 'MIDDLE_NAME',
        '7': 'CUST_EMAIL',
        '8': 'CUST_COUNTRY',
        '9': 'CUST_STATE',
        '10': 'CUST_CITY',
        '11': 'SSN',
        '12': 'FULL_STREET_ADDRESS'
    }
    
    if choice in field_map:
        field = field_map[choice]
        query = f"UPDATE CDW_SAPP_CUSTOMER SET {field} = %s WHERE SSN = %s"
        cursor.execute(query, (new_value, customer_id))
        db.commit()
        print("Customer details updated successfully.")
    else:
        print("Invalid choice. Please try again.")

def generate_monthly_bill():
    credit_card_number = input("Please enter the credit card number: ")
    month = input("Please enter the month (MM): ")
    year = input("Please enter the year (YYYY): ")
    
    if not (month.isdigit() and year.isdigit() and 1 <= int(month) <= 12 and len(month) == 2 and len(year) == 4):
        print("Invalid month/year format. Please enter as MM and YYYY.")
        return
    
    query = """
    SELECT SUM(TRANSACTION_VALUE)
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CREDIT_CARD_NO = %s
    AND MONTH = %s
    AND YEAR = %s
    """
    
    cursor.execute(query, (credit_card_number, month, year))
    result = cursor.fetchone()
    
    if result and result[0] is not None:
        print(f"The total bill for credit card number {credit_card_number} for {month}/{year} is ${result[0]:.2f}")
    else:
        print("No transactions found for the given credit card number, month, and year.")

def get_date(prompt):
    while True:
        date_str = input(prompt)
        try:
            date = datetime.strptime(date_str, '%Y%m%d')
            return date
        except ValueError:
            print("Invalid date format. Please enter the date in YYYY-MM-DD format.")

def display_transactions_between_dates():
    customer_id = input("Please enter the customer ID: ")
    start_date = get_date("Please enter the start date (YYYY-MM-DD): ")
    end_date = get_date("Please enter the end date (YYYY-MM-DD): ")
    
    query = """
    SELECT TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE, TIMEID
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_SSN = %s
    AND TIMEID BETWEEN %s AND %s
    ORDER BY TIMEID DESC
    """
    
    cursor.execute(query, (customer_id, start_date, end_date))
    results = cursor.fetchall()
    
    if results:
        print("\nTransactions:")
        for row in results:
            print(f"Transaction ID: {row[0]}, Type: {row[1]}, Value: ${row[2]:.2f}, Date: {row[3]}")
    else:
        print("No transactions found for the specified criteria.")

def main():
    while True:
        print("\nCustomer Account Management")
        print("1. Check Account Details")
        print("2. Modify Account Details")
        print("3. Generate Monthly Bill")
        print("4. Display Transactions Between Dates")
        print("5. Exit")
        
        choice = input("Please enter your choice: ")
        
        if choice == '1':
            check_account_details()
        elif choice == '2':
            modify_account_details()
        elif choice == '3':
            generate_monthly_bill()
        elif choice == '4':
            display_transactions_between_dates()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
    cursor.close()
    db.close()



# 3. Functional Requirements - Data Analysis and Visualization m 
# After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data.
# Use Python libraries for the below requirements:

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to get database connection
def get_db_connection():
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'password',
        'database': 'creditcard_capstone'
    }
    return mysql.connector.connect(**db_config)

# 3.1 Visualization: Transaction Type with Highest Transaction Count
def plot_transaction_type_count():
    conn = get_db_connection()
    query = "SELECT TRANSACTION_TYPE, COUNT(*) as count FROM CDW_SAPP_CREDIT_CARD GROUP BY TRANSACTION_TYPE"
    df = pd.read_sql(query, conn)
    conn.close()

    # Print the DataFrame to inspect the results
    print(df)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x='TRANSACTION_TYPE', y='count', data=df)
    plt.title('Transaction Type with Highest Transaction Count')
    plt.xlabel('Transaction Type')
    plt.ylabel('Count')
    plt.savefig('transaction_type_count.png')
    plt.show()

# 3.2 Visualization: Top 10 States with Highest Number of Customers
def plot_top_states_by_customers():
    conn = get_db_connection()
    query = "SELECT CUST_STATE, COUNT(*) as count FROM CDW_SAPP_CUSTOMER GROUP BY CUST_STATE ORDER BY count DESC LIMIT 10"
    df = pd.read_sql(query, conn)
    conn.close()

    # Print the DataFrame to inspect the results
    print(df)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='count', y='CUST_STATE', data=df, orient='h')
    plt.title('Top 10 States with Highest Number of Customers')
    plt.xlabel('Number of Customers')
    plt.ylabel('State')
    plt.savefig('top_states_by_customers.png')
    plt.show()

# 3.3 Visualization: Top 10 Customers with Highest Transaction Amounts
def plot_top_customers_by_transaction_amount():
    conn = get_db_connection()
    query = """
    SELECT CUST_SSN, SUM(TRANSACTION_VALUE) as total_amount 
    FROM CDW_SAPP_CREDIT_CARD 
    GROUP BY CUST_SSN 
    ORDER BY total_amount DESC 
    LIMIT 10
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Print the DataFrame to inspect the results
    print(df)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='total_amount', y='CUST_SSN', data=df, orient='h')
    plt.title('Top 10 Customers with Highest Transaction Amounts')
    plt.xlabel('Total Transaction Amount')
    plt.ylabel('Customer SSN')
    plt.savefig('top_customers_by_transaction_amount.png')
    plt.show()

# Call functions to generate plots
plot_transaction_type_count()
plot_top_states_by_customers()
plot_top_customers_by_transaction_amount()



#4
#  Access to Loan API Endpoint
# Functional Requirements 4.1
# Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
# Functional Requirements 4.2
# Calculate the status code of the above API endpoint.

# Hint: status code could be 200, 400, 404, 401.
# Functional Requirements 4.3
# Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL). The table name should be CDW-SAPP_loan_application in the database.



import requests
import os
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_api(url):
    """
    Fetch data from the given API endpoint.

    Args:
        url (str): The URL of the API endpoint.

    Returns:
        dict: JSON data fetched from the API or None if the request fails.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        logging.info(f"Successfully fetched data. Response code: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from API. Error: {e}")
        return None

def load_to_database(df, url, table, user, password):
    """
    Load a DataFrame into a MySQL database.

    Args:
        df (DataFrame): The Spark DataFrame to load into the database.
        url (str): The JDBC URL for the MySQL database.
        table (str): The table name to load data into.
        user (str): The MySQL username.
        password (str): The MySQL password.
    """
    try:
        df.write.format("jdbc") \
          .mode("overwrite") \
          .option("url", url) \
          .option("dbtable", table) \
          .option("user", user) \
          .option("password", password) \
          .save()
        logging.info(f"Successfully loaded data into the database table: {table}")
    except Exception as e:
        logging.error(f"Failed to load data into the database. Error: {e}")

def loan_application_data_ETL(api_url, db_url, db_table, db_user, db_password):
    """
    Perform the ETL process: fetch data from the API, transform it, and load it into the database.

    Args:
        api_url (str): The URL of the API endpoint.
        db_url (str): The JDBC URL for the MySQL database.
        db_table (str): The table name to load data into.
        db_user (str): The MySQL username.
        db_password (str): The MySQL password.
    """
    data = fetch_data_from_api(api_url)
    if data:
        spark = SparkSession.builder.appName("LoanData").getOrCreate()
        loan_app_df = spark.createDataFrame(data)
        load_to_database(loan_app_df, db_url, db_table, db_user, db_password)

# Environment variables for sensitive information
API_URL = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
DB_URL = "jdbc:mysql://localhost:3306/creditcard_capstone"
DB_TABLE = "creditcard_capstone.CDW_SAPP_loan_application"
DB_USER = os.getenv('MYSQL_USER', 'root')
DB_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')

# To run the ETL process
loan_application_data_ETL(API_URL, DB_URL, DB_TABLE, DB_USER, DB_PASSWORD)

# from pyspark.sql import SparkSession

# spark= SparkSession.builder.appName("Myname").getOrCreate()


data = spark.read.option("multiline",True).json("data.json")
 
data.printSchema()
data.show()




# Functional Requirement 5.1: Percentage of Approved Applications for Self-Employed Applicants

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Establish database connection
def get_db_connection():
   
    user = 'root'
    password = 'password'
    host = 'localhost'
    port = '3306'
    database = 'creditcard_capstone'
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)
    return engine.connect()


import pandas as pd
import matplotlib.pyplot as plt
import pymysql

def get_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='password',
        database='creditcard_capstone'
    )

def plot_app_approved_self_emp():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
    SELECT Application_Status, Self_Employed
    FROM CDW_SAPP_loan_application;
    """
    cursor.execute(query)
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=["Application_Status", "Self_Employed"])
    print("Data from database:")
    print(df.head(10))  # Print the first few rows to verify data

    self_employed = df[df['Self_Employed'] == 'Yes']
    print(f"Total self-employed applicants: {len(self_employed)}")

    approved_self_employed = self_employed[self_employed['Application_Status'] == 'Y']
    not_approved_self_employed = len(self_employed) - len(approved_self_employed)
    print(f"Approved self-employed applicants: {len(approved_self_employed)}")
    print(f"Not approved self-employed applicants: {not_approved_self_employed}")

    percentage_approved = (len(approved_self_employed) / len(self_employed)) * 100
    print(f"Percentage of approved applications: {percentage_approved:.2f}%")

    labels = ['Approved', 'Not Approved']
    sizes = [len(approved_self_employed), not_approved_self_employed]
    colors = ['green', 'red']
    explode = (0.1, 0)

    plt.figure(figsize=(10, 6))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.2f%%', shadow=True, startangle=140)
    plt.title(f"Percentage of Applications Approved for Self-Employed: {percentage_approved:.2f}%")
    plt.show()

    cursor.close()
    connection.close()

plot_app_approved_self_emp()




# 5.2 plot percentage of rejection for married male applicants
import pandas as pd
import matplotlib.pyplot as plt
import pymysql

def get_connection():
    """
    Establish a connection to the MySQL database.
    """
    try:
        return pymysql.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
    except pymysql.MySQLError as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_married_male_data(connection):
    """
    Fetch application status, gender, and marital status from the loan application table.
    """
    query = """
    SELECT Application_Status, Gender, Married
    FROM CDW_SAPP_loan_application;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    df = pd.DataFrame(data, columns=["Application_Status", "Gender", "Married"])
    print(df.head(10))  # Print the first few rows for verification
    return df

def plot_app_rejected_married_male():
    """
    Plot the percentage of rejection for married male applicants.
    """
    connection = get_connection()
    if connection:
        df = fetch_married_male_data(connection)

        married_males = df[(df['Gender'] == 'Male') & (df['Married'] == 'Yes')]
        print(f"Total married males: {len(married_males)}")  # Verify the count

        rejected_married_males = married_males[married_males['Application_Status'] == 'N']
        print(f"Rejected married males: {len(rejected_married_males)}")  # Verify the count

        percentage_rejected = (len(rejected_married_males) / len(married_males)) * 100
        print(f"Percentage rejected: {percentage_rejected:.2f}%")  # Verify the percentage

        plt.figure(figsize=(10, 6))
        labels = ['Rejected', 'Accepted']
        sizes = [len(rejected_married_males), len(married_males) - len(rejected_married_males)]
        colors = ['red', 'blue']
        explode = (0.1, 0)  # explode 1st slice
        plt.pie(sizes, explode=explode, labels=labels, colors=colors,
                autopct='%1.2f%%', shadow=True, startangle=140)
        plt.title(f"Percentage of Rejection for Married Male Applicants: {percentage_rejected:.2f}%")
        plt.show()

        connection.close()

# Function to plot the chart
plot_app_rejected_married_male()

 

 # 5.3 Create an appropriate visualization to perform the following task -
# Calculate and plot the top three months with the largest volume of transaction data. Use the ideal chart or graph to represent this data.
# (hint: use `CDW_SAPP_CREDIT_CARD` table)
# Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!

import pandas as pd
import matplotlib.pyplot as plt
import pymysql

def get_connection():
    """
    Establish a connection to the MySQL database.
    """
    try:
        return pymysql.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
    except pymysql.MySQLError as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_top_three_months_data(connection):
    """
    Fetch the top three months with the largest volume of transactions based on transaction counts.
    """
    query = """
    SELECT
        SUBSTRING(TIMEID, 1, 4) AS Transaction_Year,
        SUBSTRING(TIMEID, 5, 2) AS Transaction_Month,
        COUNT(TRANSACTION_ID) AS Number_of_Transactions
    FROM cdw_sapp_credit_card
    GROUP BY Transaction_Year, Transaction_Month
    ORDER BY Number_of_Transactions DESC
    LIMIT 3;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    df = pd.DataFrame(data, columns=["Transaction_Year", "Transaction_Month", "Number_of_Transactions"])
    print(df)  # Print the data for verification
    return df

def plot_top_three_months(df):
    """
    Plot the top three months with the largest volume of transactions using a bar chart.
    """
    df['Year_Month'] = df['Transaction_Year'].astype(str) + "-" + df['Transaction_Month'].astype(str)
    print(df['Year_Month'])  # Print transformed data for verification
    
    plt.figure(figsize=(10, 6))
    plt.bar(df['Year_Month'], df['Number_of_Transactions'], color='blue', alpha=0.7)
    
    # Annotate each bar with the number of transactions
    for i, row in df.iterrows():
        plt.text(i, row['Number_of_Transactions'] + 10, str(row['Number_of_Transactions']),
                 ha='center', va='bottom', fontsize=10)

    plt.title('Top Three Months with Largest Volume of Transactions')
    plt.xlabel('Month')
    plt.ylabel('Number of Transactions')
    plt.xticks(rotation=45)
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

def plot_top_three_months_largest_vol_tran_count():
    """
    Main function to fetch data and plot the top three months with the largest volume of transactions.
    """
    connection = get_connection()
    if connection:
        df = fetch_top_three_months_data(connection)
        plot_top_three_months(df)
        connection.close()

# Call the main function to plot the chart
plot_top_three_months_largest_vol_tran_count()



# 5.4 Create an appropriate visualization to perform the following task -
# Calculate and plot which branch processed the highest total dollar value of healthcare transactions. Use the ideal chart or graph to represent this data

import pandas as pd
import matplotlib.pyplot as plt
import pymysql

def get_connection():
   
    return pymysql.connect(
        host='localhost',
        user='root',           
        password='password',
        database='creditcard_capstone'
    )

def plot_highest_value_in_healthcare():
    # Establishing database connection
    connection = get_connection()
    cursor = connection.cursor()

    # SQL query to get the total healthcare transaction value per branch
    query = """
    SELECT BRANCH_CODE, SUM(TRANSACTION_VALUE) AS Total_Healthcare_Transaction_Value
    FROM cdw_sapp_credit_card
    WHERE TRANSACTION_TYPE = 'HEALTHCARE'
    GROUP BY BRANCH_CODE
    ORDER BY Total_Healthcare_Transaction_Value DESC
    LIMIT 10;
    """
    cursor.execute(query)
    data = cursor.fetchall()

    # Converting data to a pandas DataFrame
    df = pd.DataFrame(data, columns=["BRANCH_CODE", "Total_Healthcare_Transaction_Value"])
    print("Data from database:")
    print(df.head(10))  # Print the first few rows to verify data

    # Plotting the results in a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(df['BRANCH_CODE'].astype(str), df['Total_Healthcare_Transaction_Value'], color='skyblue')
    plt.title('Branch with Highest Dollar Value of Healthcare Transactions')
    plt.xlabel('Branch Code')
    plt.ylabel('Total Transaction Value ($)')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--')

    # Annotating the bar chart with transaction values
    for index, value in enumerate(df['Total_Healthcare_Transaction_Value']):
        plt.text(index, value + 5, f"${round(value, 2):,}", ha='center', va='bottom')

    plt.tight_layout()
    plt.show()

    # Closing the cursor and connection
    cursor.close()
    connection.close()

# Function to plot the chart
plot_highest_value_in_healthcare()
