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

# couchbase
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator



class RunCB:
    """ Class to connect and disconnect to/from a CB database
        Returns:
           N/A
    """

    def __init__(self):
        # Default to False since it requires extra permission
        self.conn = None

    def cb_on(self, connection):
        """ method to connect to a CB database
            Returns:
               N/A
        """

        try:
            logging.info('RunCB cb_on - Connecting to couchbase')
            cluster = Cluster('couchbase://' + connection['db_host'])
            authenticator = PasswordAuthenticator(connection['db_user'], connection['db_password'])
            cluster.authenticate(authenticator)
            self.conn = cluster.open_bucket('m-data')
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s in run_cb ***", str(e))
            sys.exit("*** Error when connecting to database")

    @staticmethod
    def cb_off(conn):
        """ method to commit data and disconnect from a CB database
            Returns:
               N/A
        """
        try:
            logging.info('RunCB cb_off - disconnecting couchbase')
            conn._close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])


    def save_row(self, row):
        logging.info('RunCB save row - writing documents to couchbase')

    @staticmethod
    def write_cb(raw_data):
        """ given a dataframe of raw_data with specific columns to write to cb documents,
        """
        try:
            logging.info('RunCB write_cb - writing documents to couchbase')
            # process stat_files
            if raw_data.stat_data.empty == False:
                logging.info('RunCB write_cb - writing stat_data')
                #get the file_rows - need them to get the file name extension
                file_dict = raw_data.data_files[['file_row', 'filename']].to_dict()
                for i in raw_data.stat_files.iterrows():
                    logging.info('RunCB write_cb - writing stat_data')


        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])
