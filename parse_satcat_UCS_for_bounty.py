#!/usr/bin/env python
# Template code for start of bounty - extracted from other source, and has not been tested in this form

from hashlib import md5
import logging
import os
from lib import Database

# import urllib.request
# import csv
import pandas as pd
import numpy as np


log = logging.getLogger(__name__)


def fingerprint_file(file):
    """Open, read file and calculate MD5 on its contents"""
    with open(file, "rb") as fd:
        # read contents of the file
        _file_data = fd.read()
        # pipe contents of the file through
        file_fingerprint = md5(_file_data).hexdigest()
    return file_fingerprint


def fingerprint_line(line):
    """ Creates a unique signature from a line."""
    return md5(line.encode("utf-8")).hexdigest()

    def populate_SATCATtable(self):
        # Set up database connection
        db = Database(dbname, dbtype, dbhostname, dbusername, dbpassword)
        db.createSATCATtable()

        satcat_file_fingerprint = fingerprint_file("data/satcat.txt")
        with open("data/satcat.txt") as file:
            entry_batch = 0
            for line in file:
                entry_batch += 1
                intl_desg = line[0:11].strip()
                norad_number = int(line[13:18].strip())

                multiple_name_flag = line[19].strip()
                if not multiple_name_flag:
                    multiple_name_flag = 0
                else:
                    multiple_name_flag = 1

                payload_flag = line[20].strip()
                if not payload_flag:
                    payload_flag = 0
                else:
                    payload_flag = 1

                ops_status_code = line[21].strip()
                name = line[23:47].strip()
                source = line[49:54].strip()
                launch_date = line[56:66].strip()

                decay_date = line[75:85].strip()
                if not decay_date:
                    decay_date = "0000-00-00"

                try:
                    orbit_period_minutes = float(line[87:94].strip())
                except ValueError:
                    orbit_period_minutes = -1

                try:
                    inclination_deg = float(line[96:101])
                except ValueError:
                    inclination_deg = -1

                try:
                    apogee = int(line[103:109])
                except ValueError:
                    apogee = -1

                try:
                    perigee = int(line[111:117])
                except ValueError:
                    perigee = -1

                try:
                    radar_crosssec = float(line[119:127])
                except ValueError:
                    radar_crosssec = -1

                orbit_status_code = line[129:132].strip()

                record_fingerprint = fingerprint_line(line)

                satcat_tuple = (
                    intl_desg,
                    norad_number,
                    multiple_name_flag,
                    payload_flag,
                    ops_status_code,
                    name,
                    source,
                    launch_date,
                    decay_date,
                    orbit_period_minutes,
                    inclination_deg,
                    apogee,
                    perigee,
                    radar_crosssec,
                    orbit_status_code,
                    satcat_file_fingerprint,
                    record_fingerprint,
                )

                satcatid = db.addSATCATentry(satcat_tuple)
                print(satcat_tuple)
                if entry_batch > 100:
                    db.conn.commit()
                    entry_batch = 0
        db.conn.commit()


def populate_UCSSATDBtable():
    # Set up database connection
    db = Database(dbname, dbtype, dbhostname, dbusername, dbpassword)
    db.createUCSSATDBtable()

    file_to_import = "data/UCS_Satellite_Database_4-1-2019.txt"

    ucsdb_file_fingerprint = fingerprint_file(file_to_import)
    # FIXME: There still appear to be some character encoding issues around "Earth's geomagnetic field"
    with open(
        file_to_import, "r", encoding="latin-1"
    ) as file:  # FIXME: Doesn't work with UTF-8 type on import (it should)
        entry_batch = 0
        for line in file:
            entry_batch += 1
            if entry_batch == 1:
                continue
            fields = line.split("\t")

            # The source CSV file has many more columns encoded than actual valid data
            good_part = fields[0:35]

            record_fingerprint = fingerprint_line(line)

            ucsdb_tuple = tuple(good_part) + (
                ucsdb_file_fingerprint,
                record_fingerprint,
            )

            ucsdbid = db.addUCSDBentry(ucsdb_tuple)
            print(ucsdb_tuple)
            if entry_batch > 100:
                db.conn.commit()
                entry_batch = 0
    db.conn.commit()


config = os.path.abspath("config.yaml")
db = Database(config)
db.createSATCATtable()
db.createUCSSATDBtable()


# print(text[0:5000])

satdb_url = "https://s3.amazonaws.com/ucs-documents/nuclear-weapons/sat-database/5-9-19-update/UCS_Satellite_Database_4-1-2019.txt"
satdb = pd.read_csv(satdb_url, delimiter="\t", encoding="Windows-1252")

print("\n\n\n")
satdb = satdb.iloc[:20, :36]
# satdb.dropna(how='all', axis=1)
print(satdb.describe())
for row in satdb.itertuples(index=False, name=None):
    savable = [i if pd.notna(i) else None for i in row]
    savable = savable + ["line fingerprint", "file_fingerprint", "time"]
    db.addUCSDBentry(savable)
# satdb.to_sql("ucs_satdb", con=db.engine)

# satcat_url = "https://www.celestrak.com/pub/satcat.txt"
# satdb = pd.read_csv(satcat_url, delimiter="\t", encoding="Windows-1252")
# print(satdb.describe())
