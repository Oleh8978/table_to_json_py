import mysql.connector
import json
from dotenv import load_dotenv
import os

load_dotenv()

def fetch_data_from_db():
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = conn.cursor()
    query = 'SELECT * FROM ' + os.getenv('DB_NAME') + ';' 
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    conn.close()
    
    return rows, column_names

def convert_to_json(rows, column_names, key_column):
    key_column_index = column_names.index(key_column)
    data_dict = {}
    for row in rows:
        key = row[key_column_index]
        data_dict[key] = {column_names[i]: row[i] for i in range(len(column_names))}
    
    return json.dumps(data_dict, indent=4)

def write_json_to_file(json_data, output_file):
    with open(output_file, 'w') as file:
        file.write(json_data)

def main():
    output_file = os.getenv('OUTPUT_FILE_NAME') + '.json'
    key_column = os.getenv('KEY_COLUMN')
    rows, column_names = fetch_data_from_db()
    json_data = convert_to_json(rows, column_names, key_column)
    write_json_to_file(json_data, output_file)

if __name__ == '__main__':
    main()
