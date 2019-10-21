import os
import requests
import re
import time
import urllib
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

def failsafe_request(url):
    i = 0
    while True:
        if i == 10: return None
        i += 1
        try:
            req = requests.get(url)
            return req
        except:
            print(f'\nFailed to load page ({url}). Retry number {i}...')

def valid_nssdc(obj_id):
    # validate NSSDC description page
    link = link_to_nssdc(obj_id)
    page = failsafe_request(link)
    if page is not None:
        soup = BeautifulSoup(page.content, 'html.parser')
        title = soup.find('title')
        if 'Error' in title.next:
            return False
        content = soup.find('div', id='contentwrapper')
        if obj_id in content.contents[0].text:
            return True
        else:
            return False
    return False

def valid_celestrak(obj_id):
    # validate Celestrak description page
    link = link_to_celestrak(obj_id)
    page = failsafe_request(link)
    if page is not None:
        soup = BeautifulSoup(page.content, 'html.parser')
        title = soup.find('title')
        clean_id = regex_celestrak.sub('', obj_id)
        try:
            if clean_id != '' and clean_id in title.next:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            return False
    else:
        return False

def valid_wikipedia(sat):
    # validate Wikipedia description page
    links = []
    # add object number links and name links
    links.append(link_to_wikipedia(sat['code']))
    links.append(
        link_to_wikipedia(
            neat_for_url(sat['name'])
        )
    )
    for link in links:
        page = failsafe_request(link)
        if page is not None:
            if page.status_code == 200:
                return link
    return False

def neat_for_url(sat_name):
    # create valid url ending
    _ = sat_name.replace(' ', '_')
    result = urllib.parse.quote(_, safe='')
    return result.capitalize().strip()
    
def link_to_nssdc(obj_id):
    # create valid link to NSSDC page
    return f'https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={obj_id}'

def link_to_celestrak(obj_id):
    # create valid link to Celestrak page
    year = obj_id.split('-')[0]
    clean_id = regex_celestrak.sub('', obj_id)
    return f'https://celestrak.com/satcat/{year}/{clean_id}.php'

def link_to_wikipedia(identifier):
    # create valid link to Wikipedia page
    return f'https://en.wikipedia.org/wiki/{identifier}'

def check_sat(sat_details):
    global buffer, total_processed
    obj_id = sat_details['code']
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
    wiki_check = valid_wikipedia(sat_details)
    if wiki_check is False:
        entry['wikipedia'] = None
    else:
        entry['wikipedia'] = wiki_check
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
    char_buffer = ''
    for _line in _file:
        line = _line.decode("utf-8")
        # get sat code and name
        sat_code = line[0:10].strip()
        sat_name = line[23:47].strip()
        # save to buffer
        sat_details = {'code': sat_code, 'name': sat_name}
        _buffer.append(sat_details)
        total += 1
        print(f'{total}', end='\r')
        #if total == 10000: break # TEST LIMIT: comment out this line to run full scan
    
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