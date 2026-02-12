#Step 3: Purpose of this script is to run 3 tests
    #Test 1: Test database connection
    #Test 2: Test database access
    #Test 3: Test ability to create a table and insert data

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

#Load environment variables
load_dotenv()

#Credentials
HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_ROOT")
PASSWORD = os.getenv("DB_PASSWORD")
DATABASE = os.getenv("DB_NAME")

def test_connection():

    print("Test 1: Connecting to the MySQL")

    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD
        )

        if connection.is_connected():
            print("Connection to MySQL successful")
        connection.close()

    except Error as e:
        print(f"Connection Failed: {e}")


def test_database_access():
    
    print("Test 2: Accessing the database")

    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )

        if connection.is_connected():
            print(f"Accessed database: {DATABASE} successfully")
        connection.close()

    except Error as e:
        print(f"Database Access Failed: {e}")


def test_temp_tables():
    print("Test 3: Creating a table and inserting data")

    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )

        cursor = connection.cursor()

        #Create a temporary table
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name VARCHAR(50));")

        #Insert sample data
        cursor.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test Name');")
        connection.commit()
        print("Table created and data inserted successfully")

        #Clean up
        cursor.execute("DROP TABLE test_table;")
        connection.close()

    except Error as e:
        print(f"Table Creation/Insertion Failed: {e}")

if __name__ == "__main__":
    print("Starting MySQL Database Tests")
    test_connection()
    test_database_access()
    test_temp_tables()
    print("MySQL Database Tests Completed")

            

