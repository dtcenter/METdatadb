#!/usr/bin/env python3

"""
Program Name: met_db_load.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Load files into METdb
Parameters: -index
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=import-error
# pylint:disable=no-member
# imported modules exist
# constants exist in constants.py


import argparse
import logging
import time
from datetime import datetime
from datetime import timedelta
import sys

import constants as CN

from read_load_xml import XmlLoadFile
from read_data_files import ReadDataFiles
from run_sql import RunSql
from run_cb import RunCB
from write_file_sql import WriteFileSql
from write_stat_sql import WriteStatSql
from write_mode_sql import WriteModeSql

def main():
    """ Main program to load files into the METdb/METviewer database
        Returns:
           N/A
    """
    # use the current date/time (without a space) as part of the log filename
    begin_time = str(datetime.now())

    logging.basicConfig(level=logging.DEBUG)

    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin time: %s", begin_time)

    # time execution
    load_time_start = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="Please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="Only process index, do not load data")

    # get the command line arguments
    args = parser.parse_args()

    #
    #  Read the XML file
    #
    try:
        logging.debug("XML filename is %s", args.xmlfile)

        # instantiate a load_spec XML file
        xml_loadfile = XmlLoadFile(args.xmlfile)

        # read in the XML file and get the information out of its tags
        xml_loadfile.read_xml()

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading XML ***", sys.exc_info()[0])
        sys.exit("*** Error reading XML")

    # if -index is used, only process the index
    if args.index:
        logging.debug("-index is true - only process index")

    #
    #  Purge files if flags set to not load certain types of files
    #
    try:
        # If user set flags to not read files, remove those files from load_files list
        xml_loadfile.load_files = purge_files(xml_loadfile.load_files, xml_loadfile.flags)

        if not xml_loadfile.load_files:
            logging.warning("!!! No files to load")
            sys.exit("*** No files to load")

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error when removing files from load list per XML")

    # Set up indices to process some maximum number of files at a time
    set_count = 0
    first_file = 0
    last_file = len(xml_loadfile.load_files) - 1

    if last_file > CN.MAX_FILES:
        mid_file = first_file + CN.MAX_FILES
    else:
        mid_file = last_file

    while mid_file <= last_file:
        try:
            # Keep track of which set of files is being processed
            set_count = set_count + 1
            # Handle only 1 file, or more files
            if first_file == last_file:
                current_files = [xml_loadfile.load_files[first_file]]
            else:
                current_files = xml_loadfile.load_files[first_file:mid_file + 1]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main setting up loop ***", sys.exc_info()[0])
            sys.exit("*** Error when setting up loop")

        #
        #  Read the data files
        #
        try:

            # instantiate a read data files object
            file_data = ReadDataFiles()

            # read in the data files, with options specified by XML flags
            file_data.read_data(xml_loadfile.flags,
                                current_files,
                                xml_loadfile.line_types)

            if file_data.data_files.empty:
                logging.warning("!!! No files to load in current set %s", str(set_count))
                # move indices to the next set of files
                first_file, mid_file, last_file = next_set(first_file, mid_file, last_file)
                continue

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main reading data ***", sys.exc_info()[0])
            sys.exit("*** Error when reading data files")

        #
        #  Write the data to a database
        #
        try:

            if xml_loadfile.connection['db_management_system'] in CN.RELATIONAL:
                # for the first set of files, connect to the database
                if set_count == 1:
                    sql_run = RunSql()
                    sql_run.sql_on(xml_loadfile.connection)

                # write the data file records out. put data file ids into other dataframes
                write_file = WriteFileSql()
                updated_data = write_file.write_file_sql(xml_loadfile.flags,
                                                         file_data.data_files,
                                                         file_data.stat_data,
                                                         file_data.mode_cts_data,
                                                         file_data.mode_obj_data,
                                                         sql_run.cur,
                                                         sql_run.local_infile)

                file_data.data_files = updated_data[0]
                file_data.stat_data = updated_data[1]
                file_data.mode_cts_data = updated_data[2]
                file_data.mode_obj_data = updated_data[3]

                if file_data.data_files.empty:
                    logging.warning("!!! No data to load in current set %s", str(set_count))
                    # move indices to the next set of files
                    first_file, mid_file, last_file = next_set(first_file, mid_file, last_file)
                    continue

                if not file_data.stat_data.empty:
                    stat_lines = WriteStatSql()

                    stat_lines.write_sql_data(xml_loadfile.flags,
                                              file_data.stat_data,
                                              sql_run.cur,
                                              sql_run.local_infile)

                if not file_data.mode_cts_data.empty or not file_data.mode_obj_data.empty:
                    cts_lines = WriteModeSql()

                    cts_lines.write_mode_data(xml_loadfile.flags,
                                              file_data.mode_cts_data,
                                              file_data.mode_obj_data,
                                              sql_run.cur,
                                              sql_run.local_infile)

                if mid_file >= last_file:
                    write_file.write_metadata_sql(xml_loadfile.flags,
                                                  file_data.data_files,
                                                  xml_loadfile.group,
                                                  xml_loadfile.description,
                                                  xml_loadfile.load_note,
                                                  xml_loadfile.xml_str,
                                                  sql_run.cur)

                    if sql_run.conn.open:
                        sql_run.sql_off(sql_run.conn, sql_run.cur)

            else:
                if xml_loadfile.connection['db_management_system'] in CN.CB:
                    cb_run = RunCB()
                    # establish bucket connection
                    logging.info("establishing cb bucket connection")
                    cb_run.cb_on(xml_loadfile.connection)
                    #write_documents(file_data.stat_data)
                    logging.info("writing cb documents")
                    cb_run.write_cb(file_data)

            # move indices to the next set of files
            first_file, mid_file, last_file = next_set(first_file, mid_file, last_file)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main writing data ***", sys.exc_info()[0])
            sys.exit("*** Error when writing data to database")

    if sql_run.conn.open:
        sql_run.sql_off(sql_run.conn, sql_run.cur)

    load_time_end = time.perf_counter()
    load_time = timedelta(seconds=load_time_end - load_time_start)

    logging.info("    >>> Total load time: %s", str(load_time))
    logging.info("End time: %s", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")

def next_set(first_file, mid_file, last_file):
    """ move indices to next set of files
        Returns:
           N/A
    """
    first_file = mid_file + 1
    if first_file > last_file:
        mid_file = last_file + 1
    else:
        mid_file = first_file + CN.MAX_FILES
        if mid_file > last_file:
            mid_file = last_file
    return first_file, mid_file, last_file

def purge_files(load_files, xml_flags):
    """ remove any files from load list that user has disallowed in XML tags
        Returns:
           List with files user wants to load
    """

    updated_list = load_files

    try:
        # Remove names of MET and VSDB files if user set load_stat tag to false
        if not xml_flags["load_stat"]:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith(".stat") or
                                    item.lower().endswith(".vsdb"))]

        # Remove names of MODE files if user set load_mode tag to false
        if not xml_flags["load_mode"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("cts.txt") or
                                    item.lower().endswith("obj.txt"))]

        # Remove names of MTD files if user set load_mtd tag to false
        if not xml_flags["load_mtd"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("2d.txt") or
                                    "3d_s" in item.lower() or
                                    "3d_p" in item.lower())]

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in purge_files ***", sys.exc_info()[0])
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error in purge files")

    return updated_list


if __name__ == '__main__':
    main()
