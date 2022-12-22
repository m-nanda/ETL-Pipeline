import os
import re
import json
from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import psycopg2
import psycopg2.extras as extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import pprint
import warnings
warnings.filterwarnings('ignore')

def dir_preparation():

    # prepare dir
    print(f'{"="*10} Preparing  Dump Directory {"="*10}')
    
    # get credential
    load_dotenv()
    DUMP_FOLDER = os.getenv('PROD_DUMP_FOLDER')
    DUMP_DIR = os.path.join(os.getcwd(), DUMP_FOLDER)
    
    # create dir if not exists
    if not os.path.isdir(DUMP_DIR):
        os.mkdir(DUMP_DIR)
        
    # clear files in dump directory
    old_files = os.listdir(DUMP_DIR)
    for item in old_files:
        os.remove(f'{DUMP_DIR}/{item}')
    
    print('Directory Ready!')

def scrape_google_finance():

    # Scraping
    print(f'{"="*10} Extract USD-IDR Rate Exchage {"="*10}')

    # get usd idr rate exchange
    try: 
        usdidr_rate = requests.get('https://www.google.com/finance/quote/USD-IDR')
        soup = BeautifulSoup(usdidr_rate.content, 'html.parser')
        pattern = r'(?<=\>)(.*)(?=\<)'
        page = str(soup.find('div', class_='YMlKec fxKbKc'))
        usd_idr_rate = re.findall(pattern, page)[0]
        pprint.pprint(f'Scraped Data: {usd_idr_rate}')
    except:
        usd_idr_rate = '15,000'
        print(f'Can\'t scraping google finance, use default exchange rate: {usd_idr_rate}')

    # Save file to dump folder
    load_dotenv()
    DUMP_FOLDER = os.getenv('PROD_DUMP_FOLDER')
    DUMP_DIR = os.path.join(os.getcwd(), DUMP_FOLDER)
    with open(f'{DUMP_DIR}/1_exchange_rate.txt', 'w') as outfile:
        outfile.write(usd_idr_rate)
    outfile.close()

def extract_coindesk():

    # Extract
    print(f'{"="*10} Extract from Coindesk API {"="*10}')

    # get data from api
    url_api = 'https://api.coindesk.com/v1/bpi/currentprice.json'
    response = urlopen(url_api)
    data = json.loads(response.read())

    # print for checking
    print('Extracted Data:')
    pprint.pprint(data)

    # save extracted data to dump folder
    load_dotenv()
    DUMP_FOLDER = os.getenv('PROD_DUMP_FOLDER')
    DUMP_DIR = os.path.join(os.getcwd(), DUMP_FOLDER)
    data_file = json.dumps(data, indent=2)
    with open(f'{DUMP_DIR}/1_extracted_data.json', 'w') as outfile:
        outfile.write(data_file)
    outfile.close()

def transform():

    # Transform
    print(f'{"="*10} Transforming All Data {"="*10}')

    # read saved file
    load_dotenv()
    DUMP_FOLDER = os.getenv('PROD_DUMP_FOLDER')
    DUMP_DIR = os.path.join(os.getcwd(), DUMP_FOLDER)
    with open(f'{DUMP_DIR}/1_extracted_data.json', 'r') as openfile:
        data = json.load(openfile)
    openfile.close()
    with open(f'{DUMP_DIR}/1_exchange_rate.txt', 'r') as openfile:
        usd_idr_rate = openfile.readlines()
    openfile.close()

    # convert rate to float **
    usd_idr_rate = round(float(''.join(usd_idr_rate).replace(',','')), 2)

    # convert datetime to custom format **
    temp = datetime.strptime(data['time']['updated'][:-4], '%b %d, %Y %H:%M:%S')
    data['time']['updated'] = datetime.strftime(temp, format='%Y-%m-%d %H:%M:%S')
    data['time']['updatedISO'] = datetime.strftime(datetime.fromisoformat(
                                    data['time']['updatedISO']), format='%Y-%m-%d %H:%M:%S'
                                )
    # current time **
    local_tz = timezone('Asia/Jakarta')
    last_update = datetime.fromisoformat(datetime.strftime(datetime.now(), 
                                        format='%Y-%m-%d %H:%M:%S')).astimezone(local_tz)
    last_update = datetime.strftime(last_update, format='%Y-%m-%d %H:%M:%S')

    # filtering data **
    data_filter = {
        'disclaimer' : data['disclaimer'], 
        'chart_name' : data['chartName'], 
        'time_updated' : data['time']['updated'], 
        'time_updated_iso' : data['time']['updatedISO'], 
        'bpi_usd_code' : data['bpi']['USD']['code'], 
        'bpi_usd_rate_float' : round(data['bpi']['USD']['rate_float'], 2), 
        'bpi_usd_description' : data['bpi']['USD']['description'],  
        'bpi_gbp_code' :  data['bpi']['GBP']['code'], 
        'bpi_gbp_rate_float' : round(data['bpi']['GBP']['rate_float'], 2),
        'bpi_gbp_description' : data['bpi']['GBP']['description'],  
        'bpi_eur_code' : data['bpi']['EUR']['code'], 
        'bpi_eur_rate_float' : round(data['bpi']['EUR']['rate_float'], 2),
        'bpi_eur_description': data['bpi']['EUR']['description'],
        'bpi_idr_rate_float': round(data['bpi']['USD']['rate_float']*usd_idr_rate, 2),
        'last_update': last_update
    }

    # convert to df
    data_filter
    data_filter_df = pd.DataFrame.from_dict(data_filter, orient='index').T
    data_filter_df.to_csv(f'{DUMP_DIR}/2_transformed_data.csv', index=False)
    with open(f'{DUMP_DIR}/2_transformed_exchange_rate.txt', 'w') as outfile:
        outfile.write(str(usd_idr_rate))
    outfile.close()

    # print for checking
    print('Transformed Data:')
    pprint.pprint(f'Rate Exchange: {usd_idr_rate}')
    print('Main Data:')
    pprint.pprint(data_filter)

    # ** : for consistency data

def load():

    # Load
    print(f'{"="*10} Loading Data to Database {"="*10}')

    # get credential
    load_dotenv()
    USER = os.getenv('PROD_USER') 
    PASS = os.getenv('PROD_PASS') 
    HOST = os.getenv('PROD_HOST') 
    PORT = os.getenv('PROD_PORT') 
    DB = os.getenv('PROD_DB') 
    TABLE = os.getenv('PROD_TABLE') 
    DUMP_FOLDER = os.getenv('PROD_DUMP_FOLDER')
    DUMP_DIR = os.path.join(os.getcwd(), DUMP_FOLDER)

    # create connection
    conn = psycopg2.connect(
                database=DB, 
                user=USER, 
                password=PASS, 
                host=HOST, 
                port=PORT
            )

    # read saved file
    data_filter_df = pd.read_csv(f'{DUMP_DIR}/2_transformed_data.csv')
    tuples =  [tuple(x) for x in data_filter_df.to_numpy()] 
    cols = ','.join(list(data_filter_df.columns))

    # query to insert data
    q =  "INSERT INTO %s(%s) VALUES %%s" % (TABLE, cols) 
    cursor = conn.cursor()

    # inserting / loading data
    try:
        extras.execute_values(cursor, q, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f'Error: {e}')
        conn.rollback()
        cursor.close()
    cursor.close()

    # get all loaded data from db
    q = f'SELECT * FROM {TABLE} ORDER BY last_update;'
    loaded_data = pd.read_sql(q, conn)
    loaded_data.to_csv(f'{DUMP_DIR}/3_loaded_data.csv', index=False)
    
    # print for checking
    print('All Loaded Data:')
    pprint.pprint(loaded_data)


default_args = {
    'owner': 'M-Nanda',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'tags':['batching', 'api', 'scraping']
}

with DAG(dag_id='api_source_etl', default_args=default_args,
        schedule_interval='@hourly', catchup=False) as dag:

    dir_prep = PythonOperator(
        task_id='prepare_dump_directory',
        python_callable=dir_preparation
    )
    extract_1 = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_coindesk
    )
    extract_2 = PythonOperator(
        task_id='scraping_data',
        python_callable=scrape_google_finance
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )
    load = PythonOperator(
        task_id='load_data',
        python_callable=load,
    )
    
    # pipeline
    dir_prep >> [extract_1, extract_2] >> transform >> load