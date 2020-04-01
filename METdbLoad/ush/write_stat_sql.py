#!/usr/bin/env python3

"""
Program Name: write_stat_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write stat files (MET and VSDB) to a SQL database.
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
import numpy as np
import pandas as pd

import constants as CN

from run_sql import RunSql

class WriteStatSql:
    """ Class to write stat files (MET and VSDB) to a SQL database
        Returns:
           N/A
    """

    @staticmethod
    def write_sql_data(load_flags, stat_data, sql_cur, local_infile):
        """ write stat files (MET and VSDB) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_sql_data ---]")

        write_time_start = time.perf_counter()

        try:

            sql_met = RunSql()

            # --------------------
            # Write Stat Headers
            # --------------------

            # find the unique headers for this current load job
            # for now, including VERSION to make pandas code easier - unlike MVLoad
            stat_headers = stat_data[CN.STAT_HEADER_KEYS].drop_duplicates()
            stat_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            stat_headers[CN.STAT_HEADER_ID] = CN.NO_KEY

            # get the next valid stat header id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.STAT_HEADER, CN.STAT_HEADER_ID, sql_cur)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["stat_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in stat_headers.iterrows():
                    sql_cur.execute(CN.Q_HEADER, data_line.values[:-1].tolist())
                    result = sql_cur.fetchone()

                    # If you find a match, put the key into the stat_headers dataframe
                    if sql_cur.rowcount > 0:
                        stat_headers.loc[stat_headers.index[row_num], CN.STAT_HEADER_ID] = result[0]
                    else:
                        stat_headers.loc[stat_headers.index[row_num], CN.STAT_HEADER_ID] = \
                        row_num + next_header_id
            else:
                # When all new headers, add the next id to the row number/index to make a new key
                stat_headers.loc[stat_headers.stat_header_id == CN.NO_KEY, CN.STAT_HEADER_ID] = \
                    stat_headers.index + next_header_id

            # get just the new headers with their keys
            new_headers = stat_headers[stat_headers[CN.STAT_HEADER_ID] > (next_header_id - 1)]
            logging.info("New headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                sql_met.write_to_sql(new_headers, CN.STAT_HEADER_FIELDS, CN.STAT_HEADER,
                                     CN.INS_HEADER, sql_cur, local_infile)

            # put the header ids back into the dataframe of all the line data
            stat_data = pd.merge(left=stat_data, right=stat_headers)

            # --------------------
            # Write Line Data
            # --------------------

            # find all of the line types in the data
            line_types = stat_data.line_type.unique()

            # process one kind of line data at a time
            for line_type in line_types:

                all_var = pd.DataFrame()

                # use the UC line type to index into the list of table names
                line_table = CN.LINE_TABLES[CN.UC_LINE_TYPES.index(line_type)]

                # get the line data of just this type and re-index
                line_data = stat_data[stat_data[CN.LINE_TYPE] == line_type].copy()
                line_data.reset_index(drop=True, inplace=True)
                logging.info("%s: %s rows", line_type, str(len(line_data.index)))

                # change all Not Available values to METviewer not available (-9999)
                line_data = line_data.replace('NA', CN.MV_NOTAV)

                # change float numbers to have limited digits after the decimal point
                # line_data = np.round(line_data, decimals=7)
                line_data[CN.COL_NUMS[3:]] = line_data[CN.COL_NUMS[3:]].astype(float)
                line_data = line_data.round(decimals=5)

                # Only variable length lines have a line_data_id
                if line_type in CN.VAR_LINE_TYPES:
                    # Get next valid line data id. Set it to zero (first valid id) if no records yet
                    next_line_id = \
                        sql_met.get_next_id(line_table, CN.LINE_DATA_ID, sql_cur)
                    logging.debug("next_line_id is %s", next_line_id)

                    # try to keep order the same as MVLoad
                    line_data = line_data.sort_values(by=[CN.DATA_FILE_ID, CN.LINE_NUM])
                    line_data.reset_index(drop=True, inplace=True)

                    line_data[CN.LINE_DATA_ID] = line_data.index + next_line_id

                    # index of the first column of the repeating variables
                    var_index = line_data.columns.get_loc(CN.LINE_VAR_COUNTER[line_type]) + 1

                    # There are 10 extra variables after n_thresh in PSTD records
                    if line_type == CN.PSTD:
                        var_index = var_index + 10

                    # need this later for old RHIST
                    orig_index = var_index

                    # process each variable line one at a time for different versions
                    for row_num, file_line in line_data.iterrows():
                        # how many sets of repeating variables
                        var_count = int(file_line[CN.LINE_VAR_COUNTER[line_type]])
                        # these two variable line types are one group short
                        if line_type in [CN.PJC, CN.PRC]:
                            var_count = var_count - 1

                        # reset to original value
                        var_index = orig_index

                        # older versions of RHIST have varying ECNT data in them
                        if line_type == CN.RHIST and file_line[CN.VERSION] in CN.RHIST_OLD:
                            var_count = int(file_line['3'])
                            var_index = orig_index + 2
                            if file_line[CN.VERSION] in CN.RHIST_5:
                                var_index = var_index + 1
                            if file_line[CN.VERSION] in CN.RHIST_6:
                                var_index = var_index + 2
                        # MCTC needs an i and a j counter
                        if line_type == CN.MCTC:
                            basic_count = var_count
                            var_count = var_count * var_count
                        # The number of variables in the repeats
                        var_repeats = CN.LINE_VAR_REPEATS[line_type]
                        # number of sets of variables times the number of variables in the sets
                        repeat_width = int(var_count * var_repeats)

                        # pull out just the repeating data
                        list_var_data = file_line.iloc[var_index:var_index + repeat_width]
                        # put it into the right number of rows and columns
                        var_data = \
                            pd.DataFrame(list_var_data.values.reshape(var_count, var_repeats))

                        # for older versions of RHIST, blank out repeating fields in line data
                        if line_type == CN.RHIST and file_line[CN.VERSION] in CN.RHIST_OLD:
                            line_data.iloc[row_num, var_index:var_index + repeat_width] = \
                                CN.MV_NOTAV

                        # for stat file versions of PSTD, blank out variable fields in line data
                        if line_type == CN.PSTD and file_line[CN.VERSION] != 'V01':
                            line_data.iloc[row_num, var_index:var_index + repeat_width] = \
                                CN.MV_NOTAV

                        # add on the first two fields - line data id, and i value
                        var_data.insert(0, CN.LINE_DATA_ID, file_line[CN.LINE_DATA_ID])
                        var_data.insert(1, 'i_value', var_data.index + 1)

                        # MCTC has i and j counters where j increments faster
                        if line_type == CN.MCTC:
                            var_data.loc[:, 'i_value'] = \
                                np.repeat(np.array(range(1, basic_count + 1)), basic_count)
                            j_indices = np.resize(range(1, basic_count + 1), var_count)
                            var_data.insert(2, 'j_value', j_indices)

                        if line_type == CN.ORANK:
                            # move the values after the variable length data to the left
                            var_end = var_index + repeat_width
                            line_data.iloc[row_num, var_index:var_index + 7] = \
                                line_data.iloc[row_num, var_end:var_end + 7].values

                        # collect all of the variable data for a line type
                        all_var = all_var.append(var_data, ignore_index=True)

                    # end for row_num, file_line

                    if line_type == CN.RHIST:
                        # copy the RHIST columns and create ECNT lines from them
                        line_data2 = line_data[line_data[CN.VERSION].isin(CN.RHIST_OLD)].copy()
                        if not line_data2.empty:
                            line_data2.line_type = CN.ECNT

                            # put the fields in the correct order for ECNT
                            line_data2 = \
                                line_data2.rename(columns={'1':'2', '2':'4',
                                                           '3':'1', '4':'3',
                                                           '5':'7', '7':'5'})

                            # Write out the ECNT lines created from old RHIST lines
                            sql_met.write_to_sql(line_data2, CN.LINE_DATA_COLS[CN.ECNT],
                                                 CN.LINE_TABLES[CN.UC_LINE_TYPES.index(CN.ECNT)],
                                                 CN.LINE_DATA_Q[CN.ECNT], sql_cur, local_infile)

                            # copy the value of n_rank two columns earlier for old RHIST
                            line_data.loc[line_data[CN.VERSION].isin(CN.RHIST_OLD), '1'] = \
                                        line_data['3']

                # write the lines out to a CSV file, and then load them into database
                if not line_data.empty:
                    sql_met.write_to_sql(line_data, CN.LINE_DATA_COLS[line_type], line_table,
                                         CN.LINE_DATA_Q[line_type], sql_cur, local_infile)

                # if there are variable length records, write them out also
                if not all_var.empty:
                    all_var.columns = CN.LINE_DATA_VAR_FIELDS[line_type]
                    sql_met.write_to_sql(all_var, CN.LINE_DATA_VAR_FIELDS[line_type],
                                         CN.LINE_DATA_VAR_TABLES[line_type],
                                         CN.LINE_DATA_VAR_Q[line_type], sql_cur, local_infile)

            # end for line_type

            # write out line_data_perc records
            if CN.FCST_PERC in stat_data:
                if stat_data[CN.FCST_PERC].ne(CN.MV_NOTAV).any():
                    line_data2 = stat_data[stat_data[CN.FCST_PERC].ne(CN.MV_NOTAV) &
                                           stat_data[CN.FCST_PERC].notnull()].copy()

                    # Write out the PERC lines
                    sql_met.write_to_sql(line_data2, CN.LINE_DATA_COLS[CN.PERC],
                                         CN.LINE_TABLES[CN.UC_LINE_TYPES.index(CN.PERC)],
                                         CN.LINE_DATA_Q[CN.PERC], sql_cur, local_infile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_sql_data ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_sql_data ---]")
