import pandas as pd
import sqlite3 as db
import xml.etree.ElementTree as ET
import os
from . import event_logging as log

DB_FILE = 'database.db'
XML_FILE = os.path.join("data_import", 'products.xml')

def parse_xml():
    if not os.path.exists(XML_FILE):
        log.log_import_event('XML', 'ERROR', 'File not found')
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
    cursor = connection.cursor()
    for row in data:
        cursor.execute( # ID is auto-incremented in DB
            """
            INSERT INTO Products (name, price, amount, description) VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                price = excluded.price,
                amount = excluded.amount,
                description = excluded.description
            """,
            (row['name'], row['price'], row['amount'], row['description'])
        )
        log.log_import_event('XML', 'INFO', f"Inserted/Updated row: {row['name']}")
    connection.commit()

def do_xml_update():
    try:
        data = parse_xml()
        if data is None:
            return f"XML data import failed: file not found."
        
        with db.connect(DB_FILE) as conn:
            insert_data(conn, data)
        
        return "XML data import successful."
    
    except Exception as e:
        log.log_import_event('XML', 'ERROR', f"XML data import failed: {e}")
        return f"XML data import failed: {e}"