#!/usr/bin/env python
from hashlib import md5
import os
import sys
from lib import Database
from lib import logger
import pandas as pd
from datetime import datetime

log = logger(__name__)
CONFIG = os.path.abspath("config.yaml")


def fingerprint_line(line):
    """ Creates a unique signature from a line."""
    return md5(line.encode("utf-8")).hexdigest()


def load_ucs_satdb_data():
    log.info("Fetching UCSATDB data and loading into memory...")
    satdb_url = "https://s3.amazonaws.com/ucs-documents/nuclear-weapons/sat-database/5-9-19-update/UCS_Satellite_Database_4-1-2019.txt"
    satdb = pd.read_csv(satdb_url, delimiter="\t", encoding="Windows-1252")
    satdb = satdb.iloc[:, :35]
    return satdb


def load_celestrak_satcat_data():
    log.info("Fetching CELESTRAK SAT CAT data and loading into memory...")
    satcat_url = "https://www.celestrak.com/pub/satcat.txt"
    satcat = pd.read_csv(
        satcat_url, engine="python", delimiter=r"\n", encoding="Windows-1252"
    )
    return satcat


def fix_discrepencies(satdb, satcat):
    log.info("Fixing discrepencies in the data...")
    # discrepencies_url = "http://celestrak.com/pub/UCS-SD-Discrepancies.txt"
    # discrepencies = pd.read_csv(
    #     discrepencies_url, delim_whitespace=True, encoding="Windows-1252"
    # )
    return (satdb, satcat)


def format(val):
    if pd.isna(val):
        return None

    if type(val) is int or type(val) is float:
        return val

    val = val.strip()

    try:
        return int(val.replace(",", ""))
    except:
        pass

    try:
        return float(val.replace(",", ""))
    except:
        pass

    try:
        return datetime.strptime(val, "%m/%d/%y").date()
    except:
        pass

    try:
        return datetime.strptime(val, "%m/%d/%Y").date()
    except:
        pass

    try:
        return datetime.strptime(val, "%Y/%m/%d").date()
    except:
        pass

    if not val or val == "N/A":
        return None

    return val


def update_ucs_satdb_table(Database, df):
    log.info("Updating the ucs_satdb table...")

    total_rows = 0
    data_batch = []
    for row in df.itertuples(index=False, name=None):
        record_fingerprint = fingerprint_line("".join(str(e) for e in row))
        savable = [format(i) for i in row] + [record_fingerprint]

        data_batch.append(savable)
        total_rows = total_rows + 1

        if len(data_batch) >= 100:
            db.add_ucs_satdb_batch(data_batch)
            data_batch = []

    db.add_ucs_satdb_batch(data_batch)
    log.info(f"{total_rows} added to ucs satdb")


def parse_celestrak_row(line):
    intl_desg = line[0:11]
    norad_number = line[13:18]

    multiple_name_flag = line[19]
    if not multiple_name_flag:
        multiple_name_flag = 0
    else:
        multiple_name_flag = 1

    payload_flag = line[20]
    if not payload_flag:
        payload_flag = 0
    else:
        payload_flag = 1

    ops_status_code = line[21]
    name = line[23:47]
    source = line[49:54]
    launch_date = line[56:66]
    launch_site = line[69:73]
    decay_date = line[75:85]
    orbit_period_minutes = line[87:94]
    inclination_deg = line[96:101]
    apogee = line[103:109]
    perigee = line[111:117]
    radar_crosssec = line[119:127]
    orbit_status_code = line[129:132]

    satcat_tuple = (
        intl_desg,
        norad_number,
        multiple_name_flag,
        payload_flag,
        ops_status_code,
        name,
        source,
        launch_date,
        launch_site,
        decay_date,
        orbit_period_minutes,
        inclination_deg,
        apogee,
        perigee,
        radar_crosssec,
        orbit_status_code,
    )
    return satcat_tuple


def update_celestrak_satcat_table(Database, df):
    log.info("Updating the celestrak_satcat table...")

    data_batch = []
    total_rows = 0
    for row in df.itertuples(index=False, name=None):
        row = parse_celestrak_row(row[0])
        record_fingerprint = fingerprint_line("".join(str(e) for e in row))
        savable = [format(i) for i in row] + [record_fingerprint]

        data_batch.append(savable)
        total_rows = total_rows + 1

        if len(data_batch) >= 100:
            db.add_celestrak_satcat_batch(data_batch)
            data_batch = []

    db.add_celestrak_satcat_batch(data_batch)
    log.info(f"{total_rows} added to celestrak satcat")


db = Database(CONFIG)
db.create_celestrak_satcat_table()
db.create_ucs_satdb_table()

satdb = load_ucs_satdb_data()
satcat = load_celestrak_satcat_data()

satdb, satcat = fix_discrepencies(satdb, satcat)

update_ucs_satdb_table(db, satdb)
update_celestrak_satcat_table(db, satcat)

log.info("Script Complete")
sys.exit(0)
