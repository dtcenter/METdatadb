#!/usr/bin/env python3

"""
Program Name: write_file_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write data_file records to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of MET and VSDB lines
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import logging
import time
from datetime import timedelta
import getpass

import constants as CN

from run_sql import RunSql


class WriteFileSql:
    """ Class to write data_file records to a SQL database
        Returns:
           N/A
    """

    def __init__(self):
        self.sql_met = RunSql()

    def write_file_sql(self, load_flags, data_files, stat_data, mode_cts_data,
                       mode_obj_data, sql_cur, local_infile):
        """ write data_file records to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_file_sql ---]")

        write_time_start = time.perf_counter()

        try:

            # --------------------
            # Write Data Files
            # --------------------

            # get next valid data file id. data files start counting from 1
            next_file_id = self.sql_met.get_next_id(CN.DATA_FILE, CN.DATA_FILE_ID, sql_cur)
            if next_file_id == 0:
                next_file_id = 1

            # write out records for data files, but first:
            # check for duplicates if flag on - delete if found
            for row_num, file_line in data_files.iterrows():
                # look for existing data file record
                sql_cur.execute(CN.Q_FILE, [file_line[CN.FILEPATH], file_line[CN.FILENAME]])
                result = sql_cur.fetchone()

                # If you find a match, check the force_dup_file tag/flag
                if sql_cur.rowcount > 0:
                    if not load_flags['force_dup_file']:
                        # delete line data rows that match index of duplicated file
                        if not stat_data.empty:
                            stat_data = stat_data.drop(stat_data[stat_data.file_row ==
                                                                 file_line.file_row].index)
                        if not mode_cts_data.empty:
                            mode_cts_data = \
                                mode_cts_data.drop(mode_cts_data[mode_cts_data.file_row ==
                                                                 file_line.file_row].index)
                        if not mode_obj_data.empty:
                            mode_obj_data = \
                                mode_obj_data.drop(mode_obj_data[mode_obj_data.file_row ==
                                                                 file_line.file_row].index)
                        data_files = data_files.drop(row_num)
                        logging.warning("!!! Duplicate file %s without FORCE_DUP_FILE tag",
                                        file_line[CN.FULL_FILE])
                    else:
                        # With duplicate files allowed, save the existing id for the file
                        data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = result[0]
                # Not a duplicate - give it a new id
                else:
                    data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = \
                        row_num + next_file_id

            # end for row_num, file_line

            if not data_files.empty:

                # reset indexes in case any records were dropped
                stat_data.reset_index(drop=True, inplace=True)
                mode_cts_data.reset_index(drop=True, inplace=True)
                mode_obj_data.reset_index(drop=True, inplace=True)

                # Replace the temporary id value with the actual index in the line data
                for row_num, row in data_files.iterrows():
                    if not stat_data.empty:
                        stat_data.loc[stat_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                      CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mode_cts_data.empty:
                        mode_cts_data.loc[mode_cts_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                          CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mode_obj_data.empty:
                        mode_obj_data.loc[mode_obj_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                          CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]

                # get just the new data files
                new_files = data_files[data_files[CN.DATA_FILE_ID] >= next_file_id]

                # write the new data files out to the sql database
                if not new_files.empty:
                    self.sql_met.write_to_sql(new_files, CN.DATA_FILE_FIELDS, CN.DATA_FILE,
                                              CN.INS_DATA_FILES, sql_cur, local_infile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_file_sql ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_file_sql ---]")

        return data_files, stat_data, mode_cts_data, mode_obj_data

    def write_metadata_sql(self, load_flags, data_files, group, description,
                           load_note, xml_str, sql_cur):
        """ write metadata and instance info records to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_metadata_sql ---]")

        write_time_start = time.perf_counter()

        try:

            # --------------------
            # Write Metadata - group and description
            # --------------------

            # insert or update the group and description fields in the metadata table
            if group != CN.DEFAULT_DATABASE_GROUP:
                sql_cur.execute(CN.Q_METADATA)
                result = sql_cur.fetchone()

                # If you find a match, update the category and description
                if sql_cur.rowcount > 0:
                    change_metadata = False
                    if group != result[0]:
                        change_metadata = True
                    if description != result[1]:
                        change_metadata = True
                    if change_metadata:
                        sql_cur.execute(CN.UPD_METADATA, [group, description])
                # otherwise, insert the category and description
                else:
                    sql_cur.execute(CN.INS_METADATA, [group, description])

            # --------------------
            # Write Instance Info
            # --------------------

            if load_flags['load_xml'] and not data_files.empty:
                update_date = data_files[CN.LOAD_DATE].iloc[0]
                next_instance_id = self.sql_met.get_next_id(CN.INSTANCE_INFO, CN.INSTANCE_INFO_ID,
                                                            sql_cur)
                sql_cur.execute(CN.INS_INSTANCE, [next_instance_id, getpass.getuser(), update_date,
                                                  load_note, xml_str])

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_metadata_sql ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_metadata_sql ---]")
