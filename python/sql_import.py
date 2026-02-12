#Step 4: Import cleaned data into SQL database

import pandas as pd
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
DATA_FOLDER = "../data/cleaned/"

#MySQL Table Schema
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS stocks (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10),
    Date DATE,
    Open FLOAT,
    High FLOAT,
    Low FLOAT,
    Close FLOAT,
    Volume BIGINT,
    Daily_Return FLOAT
);
"""

INSERT_SQL = """
INSERT INTO stocks (ticker, Date, Open, High, Low, Close, Volume, Daily_Return)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

def import_data_to_sql():

    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )

        cursor = connection.cursor()
        print("Connected to the database successfully.")
        
        #Create table if not exists
        cursor.execute(TABLE_SCHEMA)
        connection.commit()
        print("Table schema ensured.")

        #List of stock tickers
        tickers = [
            "AAPL",
            "TSLA",
            "NVDA",
            "MSFT",
            "GOOGL",
            "AMZN",
            "META",
            "JPM",
            "DIS",
            "NFLX",
            "PYPL",
        ]

        total_rows_inserted = 0

        #Import cleaned data for each ticker
        for ticker in tickers:
            file_path = os.path.join(DATA_FOLDER, f'{ticker}_cleaned_data.csv')
            
            #Check if file exists
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
            
            try:
                #Read CSV file
                df = pd.read_csv(file_path)
                
                print(f"\nProcessing {ticker}:")
                print(f"  - Columns: {list(df.columns)}")
                print(f"  - Rows to insert: {len(df)}")
                
                #Prepare data for insertion
                rows_inserted = 0
                
                for index, row in df.iterrows():
                    try:
                        #Convert NaN values to None for MySQL NULL
                        data = (
                            ticker,
                            pd.to_datetime(row['Date']).date(),  
                            float(row['Open']) if pd.notna(row['Open']) else None,
                            float(row['High']) if pd.notna(row['High']) else None,
                            float(row['Low']) if pd.notna(row['Low']) else None,
                            float(row['Close']) if pd.notna(row['Close']) else None,
                            int(row['Volume']) if pd.notna(row['Volume']) else None,
                            float(row['Daily Return']) if pd.notna(row['Daily Return']) else None
                        )
                        
                        #Execute insert
                        cursor.execute(INSERT_SQL, data)
                        rows_inserted += 1
                        
                    except Exception as e:
                        print(f"Error on row {index}: {str(e)}")
                        continue
                
                #Commit after processing ticker
                connection.commit()
                total_rows_inserted += rows_inserted
                print(f"Inserted {rows_inserted} rows for {ticker}")
                
            except Exception as e:
                print(f"Error processing {ticker}: {str(e)}")
                continue
        
        print(f"\n{'='*60}")
        print(f"All cleaned stock data imported successfully!")
        print(f"Total rows inserted: {total_rows_inserted}")
        print(f"{'='*60}")
        
        cursor.close()
        connection.close()
        
    except Error as e:
        print(f"MySQL Import Error: {str(e)}")
        return False
    
    return True
            

if __name__ == "__main__":
    import_data_to_sql()
    


