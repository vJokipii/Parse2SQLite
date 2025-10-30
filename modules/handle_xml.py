import pandas as pd
import sqlite3 as db
import xml.etree.ElementTree as ET
import os
from . import event_logging as log

DB_FILE = 'database.db'
XML_FILE = os.path.join("data_import", 'products.xml')
UPDATED_ROWS = 0

def parse_xml():
    if not os.path.exists(XML_FILE):
        log.add_to_log('XML', 'ERROR', 'File not found')
        return None

    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data = []
    for product in root.findall('product'):
        if product.find('name') is None:
            continue # Name is mandatory information
        item = {
            'name': product.find('name').text,
            'price': float(product.find('price').text) if product.find('price') is not None else 0.0,
            'amount': int(product.find('amount').text) if product.find('amount') is not None else 0,
            'description': product.find('description').text if product.find('description') is not None else ''
        }
        data.append(item)
    return data


def insert_data(connection, data):
    global UPDATED_ROWS
    cursor = connection.cursor()

    # Compare current with new data and update only if there are changes
    for row in data:
        cursor.execute("SELECT price, amount, description FROM Products WHERE name = ?", (row['name'],))
        existing = cursor.fetchone()

        if existing:
            if existing != (row['price'], row['amount'], row['description']):
                cursor.execute(
                    """
                    UPDATE Products
                    SET price = ?, amount = ?, description = ?
                    WHERE name = ?
                    """,
                    (row['price'], row['amount'], row['description'], row['name'])
                )
                UPDATED_ROWS += 1
                log.add_to_log('XML', 'INFO', f"Updated row: {row['name']}")
            # Else no changes, nothing to log
        else:
            cursor.execute(
                "INSERT INTO Products (name, price, amount, description) VALUES (?, ?, ?, ?)",
                (row['name'], row['price'], row['amount'], row['description'])
            )
            UPDATED_ROWS += 1
            log.add_to_log('XML', 'INFO', f"Inserted row: {row['name']}")

    connection.commit()

def do_xml_update():
    try:
        data = parse_xml()
        if data is None:
            return f"XML data import failed: file not found."
        
        with db.connect(DB_FILE) as conn:
            insert_data(conn, data)
        
        if UPDATED_ROWS == 0:
            log.log_no__changes("XML")
        else:
            log.add_to_log('XML', 'INFO', f"Total rows inserted/updated: {UPDATED_ROWS}")

        return "XML data import successful."
    
    except Exception as e:
        log.add_to_log('XML', 'ERROR', f"XML data import failed: {e}")
        return f"XML data import failed: {e}"