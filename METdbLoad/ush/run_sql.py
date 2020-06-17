#!/usr/bin/env python3

"""
Program Name: run_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Connect and disconnect to/from a SQL database.
Parameters: N/A
Input Files: connection data
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
import logging
import time
from datetime import timedelta
from multiprocessing import Process
import pymysql

import constants as CN


class Sql_Worker(Process):
    """
    Sql_Worker is a Multiprocess Thread that performs either a load data infile operation or a series of inserts.
    These operations can be long lived and can also be concurrent so we do them in a process thread.
    """

    def __init__(self, sql_cur, raw_data, col_list, sql_table, sql_query, local_infile):
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = "worker-" + sql_table
        self.sql_cur = sql_cur
        self.raw_data = raw_data
        self.col_list = col_list
        self.sql_table = sql_table
        self.sql_query = sql_query
        self.local_infile = local_infile

    def run(self):
        """ given a dataframe of raw_data with specific columns to write to a sql_table,
            write to a csv file and use local data infile for speed if allowed.
            otherwise, do an executemany to use a SQL insert statement to write data
            We do this in a separate process so that the database writes can happen in parallel,
            this is possible because the load tables should be to different tables.
        """
        try:
            if self.local_infile == 'ON':
                # later in development, may wish to delete these files to clean up when done
                # add the pid so that multiple processes won't overwrite data
                _tmpfileName = ""
                try:
                    _tmpfileName = os.getenv('HOME') + '/METdbLoad_' + str(os.getpid()) + '_' + self.sql_table + '.csv'
                    logging.info("processing Sql_Worker thread %s on pid %s", self.threadName, str(os.getpid()))
                    # write the data out to a csv file, use local data infile to load to database
                    self.raw_data[self.col_list].to_csv(_tmpfileName, na_rep=CN.MV_NOTAV,
                                                        index=False, header=False, sep=CN.SEP)
                    self.sql_cur.execute(CN.LD_TABLE.format(_tmpfileName, self.sql_table, CN.SEP))
                    # remove the csv file
                except:
                    logging.error("*** %s in sqlWorker write_to_sql ***", sys.exc_info()[0])
                finally:
                    if os.path.exists(_tmpfileName):
                        os.remove(_tmpfileName)
            else:
                # fewer permissions required, but slower
                # Make sure there are no NaN values
                raw_data = self.raw_data.fillna(CN.MV_NOTAV)
                # only line_data has timestamps in dataframe - change to strings
                if 'line_data' in self.sql_table:
                    raw_data['fcst_valid_beg'] = raw_data['fcst_valid_beg'].astype(str)
                    raw_data['fcst_valid_end'] = raw_data['fcst_valid_end'].astype(str)
                    raw_data['fcst_init_beg'] = raw_data['fcst_init_beg'].astype(str)
                    raw_data['obs_valid_beg'] = raw_data['obs_valid_beg'].astype(str)
                    raw_data['obs_valid_end'] = raw_data['obs_valid_end'].astype(str)

                # make a copy of the dataframe that is a list of lists and write to database
                dfile = raw_data[self.col_list].values.tolist()
                self.sql_cur.executemany(self.sql_query, dfile)
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in sqlWorker write_to_sql ***", sys.exc_info()[0])


class Run_Sql:
    """ Class to connect and disconnect to/from a SQL database
        Returns:
           N/A
    """

    def __init__(self):
        # Default to False since it requires extra permission
        self.local_infile = False
        self.conn = None
        self.cur = None
        self.xml_connection = None

    def sql_on(self, connection):
        """ method to connect to a SQL database
            Returns:
               N/A
        """
        self.xmlConnection = connection
        try:

        # Connect to the database using connection info from XML file
            self.conn = pymysql.connect(host=self.xmlConnection['db_host'],
                                        port=self.xmlConnection['db_port'],
                                        user=self.xmlConnection['db_user'],
                                        passwd=self.xmlConnection['db_password'],
                                        db=self.xmlConnection['db_name'],
                                        local_infile=True)

        except pymysql.OperationalError as pop_err:
            logging.error("*** %s in run_sql ***", str(pop_err))
            sys.exit("*** Error when connecting to database")

        try:

            self.cur = self.conn.cursor()

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logging.error("*** %s in run_sql ***", sys.exc_info()[0])
            sys.exit("*** Error when creating cursor")

        # look at database to see whether we can use the local infile method
        self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
        result = self.cur.fetchall()
        self.local_infile = result[0][1]
        logging.debug("local_infile is %s", result[0][1])

    def sql_off(self):
        """ method to commit data and disconnect from a SQL database
            Returns:
               N/A
        """

        self.conn.commit()

        self.cur.close()
        self.conn.close()

    def get_next_id(self, table, field):
        """ given a field for a table, find the max field value and return it plus one.
            Returns:
               next valid id to use in an id field in a table
        """
        # get the next valid id. Set it to zero (first valid id) if no records yet
        try:
            next_id = 0
            query_for_id = "SELECT MAX(" + field + ") from " + table
            self.cur.execute(query_for_id)
            result = self.cur.fetchone()
            if result[0] is not None:
                next_id = result[0] + 1
            return next_id

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logging.error("*** %s in write_sql_data get_next_id ***", sys.exc_info()[0])

    def write_to_sql(self, raw_data, col_list, sql_table, sql_query):
        """ given a dataframe of raw_data with specific columns to write to a sql_table,
            write to a csv file and use local data infile for speed if allowed.
            otherwise, do an executemany to use a SQL insert statement to write data
        """

        try:
            worker = Sql_Worker(self.cur, raw_data, col_list, sql_table, sql_query, self.local_infile)
            worker.start()
            worker.join()
        except:
            e = sys.exc_info()
            logging.error("*** %s Run_Sql error occurred in write_to_sql ***", e[0])

    def apply_indexes(self, drop):
        """
        If user sets tag apply_indexes to true, try to create all indexes
        If user sets tag drop_indexes to true, try to drop all indexes
        """
        logging.debug("[--- Start apply_indexes ---]")

        apply_time_start = time.perf_counter()

        try:
            if drop:
                sql_array = CN.DROP_INDEXES_QUERIES
                logging.info("--- *** --- Dropping Indexes --- *** ---")
            else:
                sql_array = CN.CREATE_INDEXES_QUERIES
                logging.info("--- *** --- Loading Indexes --- *** ---")
            if not self.conn.open:
                self.conn = pymysql.connect(host=self.xmlConnection['db_host'],
                                            port=self.xmlConnection['db_port'],
                                            user=self.xmlConnection['db_user'],
                                            passwd=self.xmlConnection['db_password'],
                                            db=self.xmlConnection['db_name'],
                                            local_infile=True)
                self.cur = self.conn.cursor()
            for sql_cmd in sql_array:
                self.cur.execute(sql_cmd)

        except pymysql.InternalError:
            if drop:
                logging.error("*** Index to drop does not exist in run_sql apply_indexes ***")
            else:
                logging.error("*** Index to add already exists in run_sql apply_indexes ***")
        finally:
            self.conn.commit()
            self.cur.close()
            self.conn.close()

        apply_time_end = time.perf_counter()
        apply_time = timedelta(seconds=apply_time_end - apply_time_start)

        logging.info("    >>> Apply time: %s", str(apply_time))

        logging.debug("[--- End apply_indexes ---]")
