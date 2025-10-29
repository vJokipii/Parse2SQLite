import pandas as pd
import sqlite3 as db
import xml.etree.ElementTree as ET
import os

DB_FILE = 'database.db'
XML_FILE = 'data.xml'

def parse_xml():
    if not os.path.exists(XML_FILE):
        print(f"XML file '{XML_FILE}' not found!")
        exit(1)
    
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

def do_xml_update():
    data = parse_xml()
    conn = db.connect(DB_FILE)
    insert_data(conn, data)
    conn.close()