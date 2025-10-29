import pandas as pd
import sqlite3 as db
import os
import event_logging as log

DB_FILE = 'database.db'
CSV_FILE = os.path.join("data_import", 'customers.csv')

def parse_csv():
    if not os.path.exists(CSV_FILE):
        log.log_import_event('CSV', 'ERROR', 'File not found')
        return None
    
    df = pd.read_csv(CSV_FILE, sep=';')
    data = []
    for _, row in df.iterrows():
        if pd.isna(row['name']):
            continue # Name is mandatory information
        item = {
            'name': row['name'],
            'location': row['location'] if not pd.isna(row['location']) else '',
            'currency': row['currency'] if not pd.isna(row['currency']) else ''
        }
        data.append(item)
    return data

def insert_data(connection, data):
    cursor = connection.cursor()
    for row in data:
        cursor.execute( # ID is auto-incremented in DB
            """
            INSERT INTO Customers (name, location, currency) VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                location = excluded.location,
                currency = excluded.currency
            """,
            (row['name'], row['location'], row['currency'])
        )
        log.log_import_event('CSV', 'INFO', f"Inserted/Updated row: {row['name']}")
    connection.commit()

def do_csv_update():
    try:
        data = parse_csv()
        if data is None:
            return f"CSV data import failed: file not found."
        
        with db.connect(DB_FILE) as conn:
            insert_data(conn, data)
        
        return "CSV data import successful."
    
    except Exception as e:
        log.log_import_event('CSV', 'ERROR', f"CSV data import failed: {e}")
        return f"CSV data import failed: {e}"