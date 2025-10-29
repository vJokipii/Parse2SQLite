import pandas as pd
import sqlite3 as db
import xml.etree.ElementTree as ET
import os

DB_FILE = 'database.db'
XML_FILE = os.path.join("data_import", 'data.xml')

def parse_xml():
    if not os.path.exists(XML_FILE):
        return f"Error: XML file '{XML_FILE}' not found!"
    
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data = []
    for product in root.findall('product'):
        item = {
            'id': product.find('id').text,
            'name': product.find('name').text,
            'price': product.find('price').text,
            'amount': product.find('amount').text,
            'description': product.find('description').text
        }
        data.append(item)
    return data


def insert_data(connection, data):
    cursor = connection.cursor()
    try:
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
        connection.commit()
    except db.Error as e:
        return f"Error: {e}"

def do_xml_update():
    data = parse_xml()
    if data.startswith("Error"):
        return f"XML data import failed. {data}"
    
    conn = db.connect(DB_FILE)
    result = insert_data(conn, data)
    conn.close()

    if result.startswith("Error"):
        return f"XML data import failed. {result}"
    
    return "XML data import successful."