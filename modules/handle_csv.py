import pandas as pd
import sqlite3 as db
import os
from . import event_logging as log

DB_FILE = 'database.db'
CSV_FILE = os.path.join("data_import", 'customers.csv')
UPDATED_ROWS = 0

def parse_csv():
    if not os.path.exists(CSV_FILE):
        log.add_to_log('CSV', 'ERROR', 'File not found')
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
    global UPDATED_ROWS
    cursor = connection.cursor()

    # Compare for logging
    for row in data:
        cursor.execute("SELECT location, currency FROM Customers WHERE name = ?", (row['name'],))
        existing = cursor.fetchone()

        if existing:
            if existing != (row['location'], row['currency']):
                cursor.execute(
                    """
                    UPDATE Customers
                    SET location = ?, currency = ?
                    WHERE name = ?
                    """,
                    (row['location'], row['currency'], row['name'])
                )
                UPDATED_ROWS += 1
                log.add_to_log('CSV', 'INFO', f"Updated row: {row['name']}")
            # Else no changes, nothing to log
        else:
            cursor.execute(
                "INSERT INTO Customers (name, location, currency) VALUES (?, ?, ?)",
                (row['name'], row['location'], row['currency'])
            )
            UPDATED_ROWS += 1
            log.add_to_log('CSV', 'INFO', f"Inserted row: {row['name']}")

    connection.commit()

def do_csv_update():
    try:
        data = parse_csv()
        if data is None:
            return f"CSV data import failed: file not found."
        
        with db.connect(DB_FILE) as conn:
            insert_data(conn, data)

        if UPDATED_ROWS == 0:
            log.log_no__changes("CSV")
        else:
            log.add_to_log('CSV', 'INFO', f"Total rows inserted/updated: {UPDATED_ROWS}")

        return "CSV data import successful."
    
    except Exception as e:
        log.add_to_log('CSV', 'ERROR', f"CSV data import failed: {e}")
        return f"CSV data import failed: {e}"