import mysql.connector
import json
from dotenv import load_dotenv
import os
from decimal import Decimal
from datetime import datetime, date

# Load environment variables from .env file
load_dotenv()

# Custom JSON encoder to handle Decimal and datetime objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(CustomJSONEncoder, self).default(obj)

# Function to fetch data from the database
def fetch_data_from_db():
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = conn.cursor()
    
    # Use table name from .env
    query = 'SELECT * FROM ' + os.getenv('TABLE_NAME') + ';' 
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    conn.close()
    
    return rows, column_names

# Function to convert data to JSON format with a specified key column
def convert_to_json(rows, column_names, key_column):
    key_column_index = column_names.index(key_column)
    data_dict = {}
    for row in rows:
        key = row[key_column_index]
        data_dict[key] = {column_names[i]: row[i] for i in range(len(column_names))}
    
    # Serialize dictionary to JSON with custom encoder and Unicode handling
    return json.dumps(data_dict, indent=4, ensure_ascii=False, cls=CustomJSONEncoder)

# Function to write JSON data to a file
def write_json_to_file(json_data, output_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(json_data)

# Main function
def main():
    output_file = os.getenv('OUTPUT_FILE_NAME') + '.json'
    key_column = os.getenv('KEY_COLUMN')
    rows, column_names = fetch_data_from_db()
    json_data = convert_to_json(rows, column_names, key_column)
    write_json_to_file(json_data, output_file)

if __name__ == '__main__':
    main()
