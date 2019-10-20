# from sqlalchemy import create_engine
import pymysql.cursors
import pandas as pd
import sys
from .logger import logger
from yaml import load, Loader

log = logger(__name__)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


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

        log.info("Connecting to the Database...")

        self.conn = pymysql.connect(
            host=self._dbhostname,
            user=self._dbusername,
            password=self._dbpassword,
            database=self._dbname,
            charset="utf8",
            use_unicode=True,
        )

    def checkTableExists(self, tablename):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = %s
                    """,
                    (tablename),
                )
                if cursor.fetchone()[0] == 1:
                    return True
        except Exception as e:
            log.error("MYSQL ERROR: {}".format(e))
            return False

        return False

    def create_celestrak_satcat_table(self):
        """ Celestrak SATCAT """

        if self.checkTableExists("celestrak_satcat"):
            log.info("Celestrak SAT CAT table found.")
            return

        log.info("Creating Celestrak SAT CAT table...")

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
          launch_site             VARCHAR(11),
          decay_date              DATE,
          orbit_period_minutes    FLOAT,
          inclination_deg         FLOAT,
          apogee                  MEDIUMINT UNSIGNED,
          perigee                 MEDIUMINT UNSIGNED,
          radar_crosssec          DOUBLE,
          orbit_status_code       CHAR(3),
          line_fingerprint        CHAR(32) NOT NULL,
          import_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          PRIMARY KEY (`satcat_id`),
          KEY `celestrak_SATCAT_intl_desg_idx` (`intl_desg`(11)) USING BTREE,
          KEY `celestrak_SATCAT_norad_num_idx` (`norad_num`) USING BTREE,
          KEY `line_fingerprint` (`line_fingerprint`) USING BTREE,
          KEY `celestrak_SATCAT_name_idx` (`name`) USING BTREE,
          KEY `celestrak_SATCAT_orbit_status_code_idx` (`orbit_status_code`) USING BTREE
      )"""
            + self.charset_string
        )

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(createquery)
                self.conn.commit()
        finally:
            True

    def create_ucs_satdb_fixed_table(self, ext=""):
        self.create_ucs_satdb_table("_fixed")

    def create_ucs_satdb_table(self, ext=""):
        """ Union of Concerned Scientists Satellite Database """

        if self.checkTableExists("ucs_satdb" + ext):
            log.info("UCS SAT DB" + ext + " table found.")
            return

        log.info("Creating Union of Concerned Scientists Satellite Database table...")

        # FIXME: Need to optimize these auto-gen types
        createquery = (
            """CREATE TABLE IF NOT EXISTS ucs_satdb"""
            + ext
            + """(
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
          import_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          KEY `ucs_SATDB_satdb_id_idx` (`satdb_id`) USING BTREE,
          KEY `line_fingerprint` (`line_fingerprint`) USING BTREE,
          KEY `ucs_SATDB_norad_number_idx` (`norad_number`) USING BTREE,
          KEY `ucs_SATDB_international_designator_idx` (`international_designator`(11)) USING BTREE
      )"""
            + self.charset_string
        )

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(createquery)
                self.conn.commit()
        finally:
            True

    def add_celestrak_satcat_batch(self, data_batch):
        """ Add an SATCAT entry to the database """

        find_query = """
            SELECT * FROM `celestrak_satcat`
            WHERE `line_fingerprint`=%s
        """

        insert_query = """
        INSERT INTO celestrak_satcat VALUES
            (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, NULL)
        """
        data_to_update = []
        try:
            for data in data_batch:
                with self.conn.cursor() as cursor:
                    cursor.execute(find_query, (data[-1],))
                    existing_row = cursor.fetchone()
                    if not existing_row:
                        data_to_update.append(data)
            if len(data_to_update) > 0:
                for data in batch(data_to_update, 100):
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_query, data)
                        self.conn.commit()
                log.info(f"{len(data_to_update)} rows added to celestrak_satcat")
        except Exception as e:
            log.error("MYSQL ERROR: {}".format(e))

    def add_ucs_satdb_fixed_batch(self, data_batch):
        """ Add an UCS DB Fixed entry to the database """

        find_query = """
            SELECT * FROM `ucs_satdb_fixed`
            WHERE `line_fingerprint`=%s
        """

        find_with_norad = """
            SELECT * FROM `ucs_satdb_fixed`
            WHERE `norad_number`=%s
        """

        delete_by_norad = """
            DELETE FROM ucs_satdb_fixed
            WHERE norad_number=%s;
        """

        insert_query = """
        INSERT INTO ucs_satdb_fixed VALUES
            (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, NULL)
        """

        update_query = """
        UPDATE ucs_satdb_fixed
        SET
            name = %s,
            country_registered = %s,
            country_owner = %s,
            owner_operator = %s,
            users = %s,
            purpose = %s,
            purpose_detailed = %s,
            orbit_class = %s,
            orbit_type = %s,
            GEO_longitude = %s,
            perigee_km = %s,
            apogee_km = %s,
            eccentricity = %s,
            inclination_degrees = %s,
            period_minutes = %s,
            launch_mass_kg = %s,
            dry_mass_kg = %s,
            power_watts = %s,
            launch_date = %s,
            expected_lifetime_years = %s,
            contractor = %s,
            contractor_country = %s,
            launch_site = %s,
            launch_vehicle = %s,
            international_designator = %s, 
            norad_number = %s,
            comments = %s,
            detailed_comments = %s,
            source_1 = %s,
            source_2 = %s,
            source_3 = %s,
            source_4 = %s,
            source_5 = %s,
            source_6 = %s,
            source_7 = %s,
            line_fingerprint = %s,
            import_timestamp = NOW()
        WHERE norad_number = %s;
        """

        data_to_insert = []
        data_to_update = []
        try:
            for data in data_batch:
                with self.conn.cursor() as cursor:
                    norad_number = data[25]
                    fingerprint = data[-1]
                    cursor.execute(find_query, (fingerprint,))
                    existing_fingerprint = cursor.fetchone()

                    if not existing_fingerprint:
                        cursor.execute(find_with_norad, (norad_number,))
                        existing_row = cursor.fetchone()
                        if existing_row:
                            cursor.execute(delete_by_norad, (norad_number))
                            log.info(
                                f"""
                                Removing existing entries with norad number {norad_number}.
                                Satellite with Norad Number {norad_number} will be updated.
                                """
                            )
    
                            # data_to_update.append(data + [norad_number])
                        data_to_insert.append(data)

            if len(data_to_insert) > 0:
                for data in batch(data_to_insert, 100):
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_query, data)
                        self.conn.commit()
                log.info(f"{len(data_to_insert)} rows added to ucs_satdb_fixed")
            if len(data_to_update) > 0:
                for data in batch(data_to_update, 100):
                    with self.conn.cursor() as cursor:
                        cursor.executemany(update_query, data)
                        self.conn.commit()
                log.info(f"{len(data_to_update)} rows updated in ucs_satdb_fixed")
        except Exception as e:
            log.error("MYSQL ERROR: {}".format(e))

    def add_ucs_satdb_batch(self, data_batch):
        """ Add an UCS DB entry to the database """

        find_query = """
            SELECT * FROM `ucs_satdb`
            WHERE `line_fingerprint`=%s
        """

        insert_query = """
        INSERT INTO ucs_satdb VALUES
            (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, NULL)
        """
        data_to_update = []
        try:
            for data in data_batch:
                with self.conn.cursor() as cursor:
                    cursor.execute(find_query, (data[-1],))
                    existing_row = cursor.fetchone()
                    if not existing_row:
                        data_to_update.append(data)
            if len(data_to_update) > 0:
                for data in batch(data_to_update, 100):
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_query, data)
                        self.conn.commit()
                log.info(f"{len(data_to_update)} rows added to ucs_satdb")
        except Exception as e:
            log.error("MYSQL ERROR: {}".format(e))
