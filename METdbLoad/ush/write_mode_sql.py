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

from run_sql import Run_Sql

class WriteModeSql:
    """ Class to write mode files (cts and object) to a SQL database
        Returns:
           N/A
    """

    def __init__(self):
        self.sql_met = Run_Sql()

    def write_mode_data(self, load_flags, cts_data, obj_data, sql_cur, local_infile):
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
            mode_headers.drop_duplicates(CN.MODE_HEADER_KEYS, keep='first', inplace=True)
            mode_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            mode_headers[CN.MODE_HEADER_ID] = CN.NO_KEY

            # get the next valid mode header id. Set it to zero (first valid id) if no records yet
            next_header_id = self.sql_met.get_next_id(CN.MODE_HEADER, CN.MODE_HEADER_ID, sql_cur)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["mode_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in mode_headers.iterrows():
                    data_line[CN.FCST_VALID] = \
                        data_line[CN.FCST_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    data_line[CN.FCST_INIT] = data_line[CN.FCST_INIT].strftime("%Y-%m-%d %H:%M:%S")
                    data_line[CN.OBS_VALID] = data_line[CN.OBS_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    # when n_valid and grid_res are null, query needs 'is null'
                    if data_line[CN.N_VALID] == CN.MV_NULL and data_line[CN.GRID_RES] == CN.MV_NULL:
                        sql_cur.execute(CN.QN_MHEADER,
                                        [data_line[CN.VERSION],
                                         data_line[CN.MODEL]] + data_line.values[7:-1].tolist())
                    else:
                        sql_cur.execute(CN.Q_MHEADER, data_line.values[3:-1].tolist())
                    result = sql_cur.fetchone()

                    # If you find a match, put the key into the mode_headers dataframe
                    if sql_cur.rowcount > 0:
                        mode_headers.loc[mode_headers.index[row_num], CN.MODE_HEADER_ID] = result[0]
                    # otherwise create the next id and put it in
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
                self.sql_met.write_to_sql(new_headers, CN.MODE_HEADER_FIELDS, CN.MODE_HEADER,
                                     CN.INS_MHEADER, sql_cur, local_infile)

            # --------------------
            # Write Line Data
            # --------------------

            # write the lines out to a CSV file, and then load them into database

            if not cts_data.empty:
                # put the header ids back into the dataframes
                cts_data = pd.merge(left=mode_headers, right=cts_data, on=CN.MODE_HEADER_KEYS)

                self.sql_met.write_to_sql(cts_data, CN.MODE_CTS_FIELDS, CN.MODE_CTS_T,
                                     CN.INS_CHEADER, sql_cur, local_infile)

            if not obj_data.empty:
                # MET has a different column name than METviewer
                obj_data = obj_data.rename(columns={'axis_ang': 'axis_avg'})
                # put the header ids back into the dataframes
                obj_data = pd.merge(left=mode_headers, right=obj_data, on=CN.MODE_HEADER_KEYS)
                mode_headers = mode_headers.iloc[0:0]

                # intensity values can be NA, which causes MySQL warning
                # replace is done to achieve desired MySQL output of NULL
                obj_data.replace({'intensity_10': CN.NOTAV, 'intensity_25': CN.NOTAV,
                                  'intensity_50': CN.NOTAV, 'intensity_75': CN.NOTAV,
                                  'intensity_90': CN.NOTAV, 'intensity_nn': CN.NOTAV},
                                 CN.MV_NULL, inplace=True)

                # pairs have an underscore in the object id - singles do not
                all_pair = obj_data[obj_data[CN.OBJECT_ID].str.contains(CN.U_SCORE)].copy()
                obj_data.drop(obj_data[obj_data[CN.OBJECT_ID].str.contains(CN.U_SCORE)].index,
                              inplace=True)

                # reset the index so mode_obj_ids are set correctly
                obj_data.reset_index(drop=True, inplace=True)

                # get next valid mode object id. Set it to zero (first valid id) if no records yet
                next_line_id = sql_met.get_next_id(CN.MODE_SINGLE_T, CN.MODE_OBJ_ID, sql_cur)

                # create the mode_obj_ids using the dataframe index and next valid id
                obj_data[CN.MODE_OBJ_ID] = obj_data.index + next_line_id

                # create defaults for flags
                obj_data[CN.SIMPLE_FLAG] = 1
                obj_data[CN.FCST_FLAG] = 0
                obj_data[CN.MATCHED_FLAG] = 0

                # Set simple flag to zero if object id starts with C
                if obj_data.object_id.str.startswith('C').any():
                    obj_data.loc[obj_data.object_id.str.startswith('C'),
                                 CN.SIMPLE_FLAG] = 0

                # Set fcst flag to 1 if object id contains an F
                if obj_data.object_id.str.contains('F').any():
                    obj_data.loc[obj_data.object_id.str.contains('F'),
                                 CN.FCST_FLAG] = 1

                # Set matched flag to 1 if object cat has neither underscore nor 000
                if (~obj_data.object_cat.str.contains(CN.U_SCORE)).sum() > 0:
                    if (~obj_data.object_cat.str.contains(CN.T_ZERO)).sum() > 0:
                        obj_data.loc[~obj_data.object_cat.str.contains(CN.U_SCORE) &
                                     ~obj_data.object_cat.str.contains(CN.T_ZERO),
                                     CN.MATCHED_FLAG] = 1

                # write out the mode single objects
                sql_met.write_to_sql(obj_data, CN.MODE_SINGLE_FIELDS, CN.MODE_SINGLE_T,
                                     CN.INS_SHEADER, sql_cur, local_infile)
            if not all_pair.empty:

                all_pair.reset_index(drop=True, inplace=True)

                # split out the paired object ids for processing
                all_pair[[CN.F_OBJECT_ID, CN.O_OBJECT_ID]] = \
                    all_pair[CN.OBJECT_ID].str.split(CN.U_SCORE, expand=True)

                # split out the paired cats for processing
                all_pair[[CN.F_OBJECT_CAT, CN.O_OBJECT_CAT]] = \
                    all_pair[CN.OBJECT_CAT].str.split(CN.U_SCORE, expand=True)

                # get only the single object columns needed to find mode object ids
                obj_data = obj_data[[CN.MODE_HEADER_ID, CN.OBJECT_ID, CN.MODE_OBJ_ID]]
                # rename the object id column to match forecasts
                obj_data.columns = [CN.MODE_HEADER_ID, CN.F_OBJECT_ID, CN.MODE_OBJ_ID]

                # get mode objects ids for forecasts
                all_pair = pd.merge(left=all_pair, right=obj_data,
                                    on=[CN.MODE_HEADER_ID, CN.F_OBJECT_ID])
                all_pair.rename(columns={CN.MODE_OBJ_ID: CN.MODE_OBJ_FCST_ID}, inplace=True)

                # rename the object id column to match observations
                obj_data.rename(columns={CN.F_OBJECT_ID: CN.O_OBJECT_ID}, inplace=True)

                # get mode objects ids for observations
                all_pair = pd.merge(left=all_pair, right=obj_data,
                                    on=[CN.MODE_HEADER_ID, CN.O_OBJECT_ID])
                all_pair.rename(columns={CN.MODE_OBJ_ID: CN.MODE_OBJ_OBS_ID}, inplace=True)

                all_pair[CN.SIMPLE_FLAG] = 1
                # Set simple flag to zero if object id starts with C
                if all_pair.f_object_id.str.startswith('C').any() and \
                        all_pair.o_object_id.str.startswith('C').any():
                    all_pair.loc[all_pair.f_object_id.str.startswith('C') &
                                 all_pair.o_object_id.str.startswith('C'),
                                 CN.SIMPLE_FLAG] = 0

                all_pair[CN.MATCHED_FLAG] = 0
                if (~all_pair.f_object_cat.str.contains(CN.T_ZERO)).sum() > 0:
                    if (all_pair.f_object_cat.str[2:] == all_pair.o_object_cat.str[2:]).any():
                        all_pair.loc[~all_pair.f_object_cat.str.contains(CN.T_ZERO) &
                                     (all_pair.f_object_cat.str[2:] ==
                                      all_pair.o_object_cat.str[2:]),
                                     CN.MATCHED_FLAG] = 1

                # write out the mode pair objects
                sql_met.write_to_sql(all_pair, CN.MODE_PAIR_FIELDS, CN.MODE_PAIR_T,
                                     CN.INS_PHEADER, sql_cur, local_infile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mode_sql ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_mode_sql ---]")
