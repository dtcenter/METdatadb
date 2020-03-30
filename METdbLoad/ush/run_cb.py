#!/usr/bin/env python3

"""
Program Name: run_cb.py
Contact(s): Randy Pierce
Abstract:
History Log:  Initial version
Usage: Connect and disconnect to/from a CB database.
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


import constants as CN


class RunCB:
    """ Class to connect and disconnect to/from a CB database
        Returns:
           N/A
    """

    def __init__(self):
        # Default to False since it requires extra permission
        self.local_infile = False
        self.conn = None
        self.cur = None

    def cb_on(self, connection):
        """ method to connect to a CB database
            Returns:
               N/A
        """

        try:
            logging.info('Connecting to couchbase')
        # Connect to the database using connection info from XML file
        #     self.conn = pymycb.connect(host=connection['db_host'],
        #                                 port=connection['db_port'],
        #                                 user=connection['db_user'],
        #                                 passwd=connection['db_password'],
        #                                 db=connection['db_name'],
        #                                 local_infile=True)

        except:
            logging.error("*** %s in run_cb ***")
            sys.exit("*** Error when connecting to database")

        try:

            self.cur = self.conn.cursor()

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logging.error("*** %s in run_cb ***", sys.exc_info()[0])

        # look at database to see whether we can use the local infile method
        self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
        result = self.cur.fetchall()
        self.local_infile = result[0][1]
        logging.debug("local_infile is %s", result[0][1])

    @staticmethod
    def cb_off(conn, cur):
        """ method to commit data and disconnect from a CB database
            Returns:
               N/A
        """

        conn.commit()

        cur.close()
        conn.close()

    @staticmethod
    def get_next_id(table, field, cb_cur):
        """ given a field for a table, find the max field value and return it plus one.
            Returns:
               next valid id to use in an id field in a table
        """
        # get the next valid id. Set it to zero (first valid id) if no records yet
        try:
            next_id = 0
            query_for_id = "SELECT MAX(" + field + ") from " + table
            cb_cur.execute(query_for_id)
            result = cb_cur.fetchone()
            if result[0] is not None:
                next_id = result[0] + 1
            return next_id

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_cb_data get_next_id ***", sys.exc_info()[0])

    @staticmethod
    def write_to_cb(raw_data, col_list, cb_table, cb_query, cb_cur, local_infile):
        """ given a dataframe of raw_data with specific columns to write to a cb_table,
            write to a csv file and use local data infile for speed if allowed.
            otherwise, do an executemany to use a CB insert statement to write data
        """

        try:
            if local_infile == 'ON':
                # later in development, may wish to delete these files to clean up when done
                tmpfile = os.getenv('HOME') + '/METdbLoad_' + cb_table + '.csv'
                # write the data out to a csv file, use local data infile to load to database
                raw_data[col_list].to_csv(tmpfile, na_rep=CN.MV_NOTAV,
                                          index=False, header=False, sep=CN.SEP)
                cb_cur.execute(CN.LD_TABLE.format(tmpfile, cb_table, CN.SEP))
            else:
                # fewer permissions required, but slower
                # Make sure there are no NaN values
                raw_data = raw_data.fillna(CN.MV_NOTAV)

                # only line_data has timestamps in dataframe - change to strings
                if 'line_data' in cb_table:
                    raw_data['fcst_valid_beg'] = raw_data['fcst_valid_beg'].astype(str)
                    raw_data['fcst_valid_end'] = raw_data['fcst_valid_end'].astype(str)
                    raw_data['fcst_init_beg'] = raw_data['fcst_init_beg'].astype(str)
                    raw_data['obs_valid_beg'] = raw_data['obs_valid_beg'].astype(str)
                    raw_data['obs_valid_end'] = raw_data['obs_valid_end'].astype(str)

                # make a copy of the dataframe that is a list of lists and write to database
                dfile = raw_data[col_list].values.tolist()
                cb_cur.executemany(cb_query, dfile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])
