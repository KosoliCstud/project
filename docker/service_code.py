import psycopg2
import requests
import schedule
import time 
import datetime


def get_all_ids():
    response = requests.get("https://api.gios.gov.pl/pjp-api/rest/station/findAll")
    data = response.json()

    ids = [station['id'] for station in data]
    return ids[:5]


def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def insert_data_to_db(conn, data):
    with conn:
        with conn.cursor() as cur:
            if data['AqIndex'] and data['AqIndex']['Wartość indeksu'] is not None:
                cur.execute(f'''
                    INSERT INTO air_quality (station_id, date_time, index_value, index_name)
                    VALUES ({data['AqIndex']['Identyfikator stacji pomiarowej']}, '{datetime.datetime.now()}', {data['AqIndex']['Wartość indeksu']}, '{data['AqIndex']['Nazwa kategorii indeksu']}');
                ''')
                conn.commit()

def job():
    for station_id in get_all_ids():
        api_url = f'https://api.gios.gov.pl/pjp-api/v1/rest/aqindex/getIndex/{station_id}'
        data = fetch_data_from_api(api_url)
        if data:
            insert_data_to_db(conn, data)

if __name__ == '__main__':
    
    db_name = 'gios_data'
    db_user = 'postgres'
    db_password = 'kochamadb'
    db_host = 'postgres-db'
    db_port = '5432'

    time.sleep(5)   # Wait for the db to wake up
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    
    table_name = "air_quality"
    table_exists = False
    
    # schedule.every(1).hours.do(job)
    schedule.every(30).seconds.do(job)
    
    while True:
        time.sleep(1)
        if table_exists:
            schedule.run_pending()
        else:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE tablename = 'docker');")
                    table_exists = cur.fetchone()[0]
                    time.sleep(5)
