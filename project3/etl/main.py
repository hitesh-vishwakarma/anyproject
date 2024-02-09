import pandas as pd
import logging
# from sqlalchemy import create_engine
import phonenumbers
from datetime import datetime
import shutil
# import psycopg2
import mysql.connector
import os

conn = mysql.connector.connect(host='localhost', user='root', password='root', database='practice')

# Configure logging
# logging.basicConfig(filename='logs/etl.log', level=logging.INFO)

# Read input_data data
src_folder = os.listdir('../input_data')
file_count = len(src_folder)
if file_count:
    for file in os.listdir('../input_data'):
        print(file)
        df = pd.read_csv(f'../input_data/{file}')

    shutil.move(f"../input_data/{file}",'../archive')

    # ETL Manipulations

    # 1. Removing null records
    df[df.isnull().any(axis=1)]
    df = df.dropna()

    # 2. Validate email addresses and drop invalid records
    df = df[df['Mail'].apply(lambda x: pd.notna(x) and isinstance(x, str))]

    # 3. Formatting the name
    df['Full_Name'] = df['First Name'] + ' ' + df['Last Name']

    # 4. Formatting phone numbers
    def format_phone_number(phone):
        try:
            parsed_number = phonenumbers.parse(phone, "IN")
            formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
            if formatted_number.startswith("+"):
                return formatted_number
            else:
                return f"+{formatted_number}"
        except phonenumbers.phonenumberutil.NumberFormatError:
            return ""

    df['Formatted_phone'] = df['Phone_Number'].apply(lambda x: format_phone_number(x))

    # 5. Removing cities other than indian cities
    valid_cities = [
        "Mumbai",
        "Gurugram",
        "Lucknow",
        "Chandigarh",
        "Hyderabad",
        "Bangalore"
    ]
    df['Valid_City'] = df['City'].apply(lambda x: x.strip().title() if x.strip().title() in valid_cities else None)
    df = df.dropna(subset=['Valid_City'])

    # 6. Adding current timestamp
    df['ingest_timestamp'] = current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = df[['Id', 'Full_Name', 'Formatted_phone', 'Mail', 'Valid_City', 'DOB', 'ingest_timestamp']]

    # Log manipulated records
    # logging.info(f"Ignoring Null record...")
    # logging.info(f"Valid email records: {df['Mail'].tolist()}")
    # logging.info(f"Concatenating of First Name and Last Name...")
    # logging.info(f"Records after formatting: {df['Formatted_phone'].tolist()}")
    # logging.info(f"Valid city records: {df['Valid_City'].tolist()}")
    # logging.info(f"Adding Current timestamp: {df['ingest_timestamp'].tolist()}")

    def table_exists(table_name, conn):
        with conn.cursor() as cursor:
            # Execute the SHOW TABLES query
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

            # Fetch the result
            result = cursor.fetchone()

            # Check if the result is not None, indicating that the table exists
            return result is not None

    # Connect to the database
    cursor = conn.cursor()
    # engine = create_engine('postgresql://myuser:mypassword@db/mydb')

    cursor.execute('''
                CREATE TABLE IF NOT EXISTS mytable (
                                        id INT,
                                        Full_Name VARCHAR(50),
                                        Formatted_phone VARCHAR(15),
                                        Mail VARCHAR(50),
                                        Valid_City VARCHAR(30),
                                        DOB Date,
                                        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                        flag CHAR default 'U'
                                    );
                    ''')

    for i,r in df.iterrows():
        column_names = i
        break

    # Check if the table exists
    # if not engine.has_table('mytable') :
    if table_exists('mytable', conn):
        # If the table doesn't exist, create it
        # df.to_sql('mytable', conn, index=False)
        for i,row in df.iterrows():
            query = f"insert into mytable {column_names} values (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            logging.info(f'Records got inserted!!!')
            print('Data Got inserted..')
            conn.commit()
    else:
        # If the table exists, implement SCD-2 logic
        with conn.cursor() as connection:

            # Mark new data with a flag  based on duplicate IDs
            connection.execute("""
                            UPDATE mytable
                            SET flag = 'N'
                            WHERE id IN (
                                SELECT id
                                FROM (
                                    SELECT id, COUNT(*) AS cnt
                                    FROM mytable
                                    WHERE id IS NOT NULL
                                    GROUP BY id
                                    HAVING COUNT(*) > 1
                                ) AS duplicate_ids
                            )
                        """)
else:
    print('No file into the folder.')

# Close the database connection
# engine.dispose()
conn.close()
