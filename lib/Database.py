# from sqlalchemy import create_engine
import pymysql.cursors
import pandas as pd
import logging
from yaml import load, Loader

# dbname, dbtype, dbhostname, dbusername, dbpassword
# DB


class Database:
    def __init__(self, db_config_path):
        f = open(db_config_path)
        db_config = load(f, Loader=Loader)["Database"]
        f.close()

        self._dbname = db_config["name"]
        self._dbtype = db_config.get("type")
        self._dbhostname = db_config.get("hostname")
        self._dbusername = db_config.get("username")
        self._dbpassword = db_config.get("password")
        self.charset_string = "CHARSET=utf8 ENGINE=Aria;"
        self.increment = " AUTO_INCREMENT"

        self.conn = pymysql.connect(
            host=self._dbhostname,
            user=self._dbusername,
            password=self._dbpassword,
            database=self._dbname,
            charset="utf8",
            use_unicode=True,
        )
        # connect = lambda: pymysql.connect(
        #     host=self._dbhostname,
        #     user=self._dbusername,
        #     password=self._dbpassword,
        #     database=self._dbname,
        #     charset="utf8",
        #     use_unicode=True,
        # )
        # self.engine = create_engine("mysql+pymysql://", creator=connect)
        # self.conn = self.engine.connect()
        self.c = self.conn.cursor()
        # self.c = self.engine.connect()

        # self.c_addSATCAT_query = self.conn.cursor()
        # self.c_addUCSDB_query = self.conn.cursor()

    def createSATCATtable(self):
        """ Celestrak SATCAT """

        print("Creating Celestrak SAT CAT table...")

        # TODO: make another table from the multiple_name_flag data in https://celestrak.com/pub/satcat-annex.txt
        createquery = (
            """CREATE TABLE IF NOT EXISTS celestrak_satcat (
          satcat_id               INTEGER """
            + self.increment
            + """,
          intl_desg               VARCHAR(11) NOT NULL,
          norad_num               MEDIUMINT UNSIGNED NOT NULL,
          multiple_name_flag      TINYINT(1) UNSIGNED NOT NULL,
          payload_flag            TINYINT(1) UNSIGNED NOT NULL,
          ops_status_code         VARCHAR(24),
          name                    VARCHAR(24) NOT NULL,
          source                  CHAR(5),
          launch_date             DATE,
          decay_date              DATE,
          orbit_period_minutes    MEDIUMINT,
          inclination_deg         DOUBLE,
          apogee                  DOUBLE,
          perigee                 DOUBLE,
          radar_crosssec          DOUBLE,
          orbit_status_code       CHAR(3),
          line_fingerprint        CHAR(32) NOT NULL,
          file_fingerprint        CHAR(32) NOT NULL,
          import_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          PRIMARY KEY (`satcat_id`),
          KEY `celestrak_SATCAT_intl_desg_idx` (`intl_desg`(11)) USING BTREE,
          KEY `celestrak_SATCAT_norad_num_idx` (`norad_num`) USING BTREE,
          KEY `celestrak_SATCAT_name_idx` (`name`) USING BTREE,
          KEY `celestrak_SATCAT_orbit_status_code_idx` (`orbit_status_code`) USING BTREE
      )"""
            + self.charset_string
        )
        self.c.execute(createquery)
        # self.conn.commit()

    def createUCSSATDBtable(self):
        """ Union of Concerned Scientists Satellite Database """

        print("Creating Union of Concerned Scientists Satellite Database table...")

        # FIXME: Need to optimize these auto-gen types
        createquery = (
            """CREATE TABLE IF NOT EXISTS ucs_satdb (
          satdb_id              INTEGER PRIMARY KEY"""
            + self.increment
            + """,
          name text DEFAULT NULL,
          country_registered text DEFAULT NULL,
          country_owner text DEFAULT NULL,
          owner_operator text DEFAULT NULL,
          users text DEFAULT NULL,
          purpose text DEFAULT NULL,
          purpose_detailed text DEFAULT NULL,
          orbit_class text DEFAULT NULL,
          orbit_type text DEFAULT NULL,
          GEO_longitude int(11) DEFAULT NULL,
          perigee_km int(11) DEFAULT NULL,
          apogee_km int(11) DEFAULT NULL,
          eccentricity float DEFAULT NULL,
          inclination_degrees float DEFAULT NULL,
          period_minutes int(11) DEFAULT NULL,
          launch_mass_kg int(11) DEFAULT NULL,
          dry_mass_kg text DEFAULT NULL,
          power_watts text DEFAULT NULL,
          launch_date DATE DEFAULT NULL,
          expected_lifetime_years text DEFAULT NULL,
          contractor text DEFAULT NULL,
          contractor_country text DEFAULT NULL,
          launch_site text DEFAULT NULL,
          launch_vehicle text DEFAULT NULL,
          international_designator text DEFAULT NULL,
          norad_number int(11) DEFAULT NULL,
          comments text DEFAULT NULL,
          detailed_comments text DEFAULT NULL,
          source_1 text DEFAULT NULL,
          source_2 text DEFAULT NULL,
          source_3 text DEFAULT NULL,
          source_4 text DEFAULT NULL,
          source_5 text DEFAULT NULL,
          source_6 text DEFAULT NULL,
          source_7 text DEFAULT NULL,
          line_fingerprint        CHAR(32) NOT NULL,
          file_fingerprint        CHAR(32) NOT NULL,
          import_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          KEY `ucs_SATDB_satdb_id_idx` (`satdb_id`) USING BTREE,
          KEY `ucs_SATDB_norad_number_idx` (`norad_number`) USING BTREE,
          KEY `ucs_SATDB_international_designator_idx` (`international_designator`(11)) USING BTREE
      )"""
            + self.charset_string
        )
        self.c.execute(createquery)
        # self.conn.commit()

    def addSATCATentry(self, newentryTuple):
        """ Add an SATCAT entry to the database """
        self._satcatid = (
            0
        )  # Set this as a variable in case we want to generate our own in the future

        try:
            self.c_addSATCAT_query.execute(self.addSATCAT_query, newentryTuple)
        except Exception as e:
            logging.error("MYSQL ERROR: {}".format(e))
        return True

    def addUCSDBentry(self, newentryTuple):
        """ Add an UCS DB entry to the database """
        self._satcatid = (
            0
        )  # Set this as a variable in case we want to generate our own in the future

        query = """
        INSERT INTO ucs_satdb VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        print(newentryTuple)
        try:
            with self.conn.cursor() as c:
                c.execute(query, newentryTuple)
        except Exception as e:
            logging.error("MYSQL ERROR: {}".format(e))
        # finally:
        #     self.conn.close()

    def fixUCSDB_from_SATCAT(self):
        """ TODO """
        pass

    def update_SATCAT(self):
        """ TODO """
        pass

    def update_UCSDB(self):
        """ TODO """
        pass
