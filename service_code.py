import psycopg2
import requests
import pandas as pd
import matplotlib.pyplot as plt
import schedule
import time

db_name = 'gios_data'
db_user = 'postgres'
db_password = 'kochamadb'
db_host = '172.31.48.1'
db_port = '5000'

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS air_quality(
    station_id INTEGER,
    date_time TIMESTAMP,
    index_value DECIMAL,
    index_name VARCHAR(255),
    FOREIGN KEY (station_id)
        REFERENCES stations (station_id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
''')
conn.commit()

def get_all_ids():
    response = requests.get("https://api.gios.gov.pl/pjp-api/rest/station/findAll")
    data = response.json()

    ids = [station['id'] for station in data]
    return ids

all_ids = get_all_ids()

def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def insert_data_to_db(conn, data):
    cur = conn.cursor()
    if data['AqIndex'] and data['AqIndex']['Wartość indeksu'] is not None:
        cur.execute(f'''
            INSERT INTO air_quality (station_id, date_time, index_value, index_name)
            VALUES ({data['AqIndex']['Identyfikator stacji pomiarowej']}, '{data['AqIndex']['Data wykonania obliczeń indeksu']}', {data['AqIndex']['Wartość indeksu']}, '{data['AqIndex']['Nazwa kategorii indeksu']}');
        ''')
        conn.commit()

def job():
    for station_id in all_ids:
        api_url = f'https://api.gios.gov.pl/pjp-api/v1/rest/aqindex/getIndex/{station_id}'
        data = fetch_data_from_api(api_url)
        if data:
            insert_data_to_db(conn, data)

schedule.every(1).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
