import os
import requests
import re
import time
import mysql.connector
from mysql.connector import errorcode
from urllib import request as urllib_req
from bs4 import BeautifulSoup
from threading import Thread

# --- CONSTANTS ---

RATE_LIMIT_DELAY = 0.1 # seconds
URL = 'https://celestrak.com/pub/satcat.txt'

SQL_CONFIG = {
    'user': 'root',
    'password': 'pass.word'
}

SQL_DB = 'satellites'

TABLE = (
    "CREATE TABLE IF NOT EXISTS `links` ("
    " `obj_no` varchar(20) NOT NULL,"
    " `nssdc` varchar(128),"
    " `celestrak` varchar(128),"
    " `wikipedia` varchar(128))"
    " ENGINE=InnoDB")

# --- VARIABLES ---

regex_celestrak = re.compile(r'[^\d-]+')
buffer = []
total = 0
total_processed = 0

# --- FUNCTIONS ---

def create_database(cursor):
    try:
        cursor.execute(
            f"CREATE DATABASE {SQL_DB} DEFAULT CHARACTER SET 'utf8'")
    except mysql.connector.Error as err:
        print(f'Failed creating database: {err}')
        os._exit(1)

def valid_nssdc(obj_id):
    # validate NSSDC description page
    link = link_to_nssdc(obj_id)
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    title = soup.find('title')
    if 'Error' in title.next:
        return False
    content = soup.find('div', id='contentwrapper')
    if obj_id in content.contents[0].text:
        return True
    else:
        return False

def valid_celestrak(obj_id):
    # validate Celestrak description page
    link = link_to_celestrak(obj_id)
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    title = soup.find('title')
    clean_id = regex_celestrak.sub('', obj_id)

    if clean_id != '' and clean_id in title.next:
        return True
    else:
        return False

def valid_wikipedia(obj_id):
    # validate Wikipedia description page
    link = link_to_wikipedia(obj_id)
    page = requests.get(link)
    if page.status_code == 200:
        return True
    else:
        return False
    
def link_to_nssdc(obj_id):
    # create valid link to NSSDC page
    return f'https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={obj_id}'

def link_to_celestrak(obj_id):
    # create valid link to Celestrak page
    year = obj_id.split('-')[0]
    clean_id = regex_celestrak.sub('', obj_id)
    return f'https://celestrak.com/satcat/{year}/{clean_id}.php'

def link_to_wikipedia(obj_id):
    # create valid link to Wikipedia page
    return f'https://en.wikipedia.org/wiki/{obj_id}'

def check_sat(obj_id):
    global buffer, total_processed
    entry = {}
    entry['id'] = obj_id
    # check NSSDC
    if valid_nssdc(obj_id):
        entry['nssdc'] = link_to_nssdc(obj_id)
    else:
        entry['nssdc'] = None
    # check Celestrak
    if valid_celestrak(obj_id):
        entry['celestrak'] = link_to_celestrak(obj_id)
    else:
        entry['celestrak'] = None
    # check Wikipedia
    if valid_wikipedia(obj_id):
        entry['wikipedia'] = link_to_wikipedia(obj_id)
    else:
        entry['wikipedia'] = None
    # save result to DB
    buffer.append(entry)
    total_processed += 1

def progress():
    # wait for process to finish
    while True:
        time.sleep(0.1)
        print(f'Processed {total_processed}/{total} satellites.', end='\r')
        if total_processed == total:
            break


if __name__ == '__main__':

    # connect to server
    try:
        cnx = mysql.connector.connect(**SQL_CONFIG)
        cursor = cnx.cursor()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Something is wrong with your user name or password')
        else:
            print(err)
        print('Script is shutting down.')
        os._exit(1)

    # load DB and initialize table
    try:
        cursor.execute(f'USE {SQL_DB}')
    except mysql.connector.Error as err:
        print(f'Database {SQL_DB} does not exist.')
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print(f'Database {SQL_DB} created successfully.')
            cnx.database = SQL_DB
        else:
            print(err)
            os._exit(1)
    cursor.execute(TABLE)

    # load sat txt file
    _file = urllib_req.urlopen(URL)
    _buffer = []

    print(f'Generating list of satellites...')
    for _line in _file:
        line = _line.decode("utf-8")
        x = line.split(' ')
        sat = x[0]
        total += 1
        _buffer.append(sat)
        print(f'{total}', end='\r')
        if total == 200: break # TEST LIMIT: comment out this line to run full scan
    
    print(f'{total} satellites found.')
    Thread(target=progress).start()
    for sat in _buffer:
        Thread(target=check_sat, args=(sat,)).start()
        time.sleep(RATE_LIMIT_DELAY)
    
    # wait for threads to finish
    while total_processed < total:
        time.sleep(1)

    print('Saving to database...', end='')


    # clear current records
    clear_table = ("TRUNCATE TABLE links")
    cursor.execute(clear_table)

    # save to DB
    add_entry = ("INSERT INTO links "
                "(obj_no, nssdc, celestrak, wikipedia) "
                "VALUES (%s, %s, %s, %s)")
    for _x in buffer:
        values = (_x['id'], _x['nssdc'], _x['celestrak'], _x['wikipedia'])
        cursor.execute(add_entry, values)

    cnx.commit()
    print('done')

    cursor.close()
    cnx.close()


    print('All satellites saved successfully!')