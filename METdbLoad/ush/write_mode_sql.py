#!/usr/bin/env python3

"""
Program Name: write_mode_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write mode files (cts and object) to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of mode cts and object lines
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import logging
import time
from datetime import timedelta
import pandas as pd

import constants as CN

from run_sql import RunSql

class WriteModeSql:
    """ Class to write mode files (cts and object) to a SQL database
        Returns:
           N/A
    """
    @staticmethod
    def write_mode_data(load_flags, cts_data, obj_data, sql_cur, local_infile):
        """ write mode files (cts and object) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_mode_sql ---]")

        write_time_start = time.perf_counter()

        try:

            all_pair = pd.DataFrame()

            sql_met = RunSql()

            # --------------------
            # Write Mode Headers
            # --------------------

            # get the unique mode headers from cts_data and obj_data
            if not cts_data.empty:
                mode_headers = cts_data[CN.MODE_HEADER_FIELDS[1:]]
            if not obj_data.empty:
                mode_headers = mode_headers.append(obj_data[CN.MODE_HEADER_FIELDS[1:]],
                                                   ignore_index=True)
            # restore to original order now that cts and obj are recombined
            mode_headers = mode_headers.sort_values(by=[CN.DATA_FILE_ID, CN.LINENUMBER])
            # get unique values, keeping the first of the duplicate records
            mode_headers = mode_headers.drop_duplicates(CN.MODE_HEADER_KEYS, keep='first')
            mode_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            mode_headers[CN.MODE_HEADER_ID] = CN.NO_KEY

            # get the next valid mode header id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.MODE_HEADER, CN.MODE_HEADER_ID, sql_cur)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["mode_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in mode_headers.iterrows():
                    sql_cur.execute(CN.Q_MHEADER, data_line.values[:-1].tolist())
                    result = sql_cur.fetchone()

                    # If you find a match, put the key into the mode_headers dataframe
                    if sql_cur.rowcount > 0:
                        mode_headers.loc[mode_headers.index[row_num], CN.MODE_HEADER_ID] = result[0]
                    else:
                        mode_headers.loc[mode_headers.index[row_num], CN.MODE_HEADER_ID] = \
                            row_num + next_header_id
            else:
                # When all new headers, add the next id to the row number/index to make a new key
                mode_headers.loc[mode_headers.mode_header_id == CN.NO_KEY, CN.MODE_HEADER_ID] = \
                    mode_headers.index + next_header_id

            # get just the new headers with their keys
            new_headers = mode_headers[mode_headers[CN.MODE_HEADER_ID] > (next_header_id - 1)]
            logging.info("New mode headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                sql_met.write_to_sql(new_headers, CN.MODE_HEADER_FIELDS, CN.MODE_HEADER,
                                     CN.INS_MHEADER, sql_cur, local_infile)

            # --------------------
            # Write Line Data
            # --------------------

            # write the lines out to a CSV file, and then load them into database

            if not cts_data.empty:
                # put the header ids back into the dataframes
                cts_data = pd.merge(left=mode_headers, right=cts_data, on=CN.MODE_HEADER_KEYS)
                # round off floats
                cts_data = cts_data.round(decimals=5)
                sql_met.write_to_sql(cts_data, CN.MODE_CTS_FIELDS, CN.MODE_CTS_T,
                                     CN.INS_CHEADER, sql_cur, local_infile)

            if not obj_data.empty:
                # put the header ids back into the dataframes
                obj_data = pd.merge(left=mode_headers, right=obj_data, on=CN.MODE_HEADER_KEYS)
                # round off floats
                obj_data = obj_data.round(decimals=5)

                all_pair = obj_data[obj_data[CN.OBJECT_ID].str.contains('_')]
                obj_data = \
                    obj_data.drop(obj_data[obj_data[CN.OBJECT_ID].str.contains('_')].index)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mode_sql ***", sys.exc_info()[0])


        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_mode_sql ---]")
