"""
Program Name: Class Data_Type_Manager.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The manager runs as a thread and is given a list of file names. It maintains its own connection which it keeps open until it finishes.
As it reads the file it determines which concrete builder to use for each line.
When it finishes the file it converts the data to a document and upserts it.
        Attributes:
            file - a vsdb file
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""
import argparse
import logging
import time
import sys
from datetime import datetime
from datetime import timedelta

from queue import Queue

from read_load_xml import XmlLoadFile

from data_type_manager import Data_Type_Manager

def main():
    begin_time = str(datetime.now())
    logging.basicConfig(level=logging.DEBUG)
    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin time: %s", begin_time)
    # time execution
    load_time_start = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="Please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="Only process index, do not load data")
    parser.add_argument("-t", "--threads", type=int, default = 1, help = "Number of threads to use")
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

    # load the queue with filenames
    #Constructor for an infinite size FIFO queue
    q = Queue()
    for f in xml_loadfile.load_files:
        q.put(f)
    thread_limit = args.threads
    # instantiate data_type_manager pool - each data_type_manager is a thread that uses builders to process a file
    # Make the Pool of data_type_managers
    _dtm_list = []
    for _threadCount in range(thread_limit):
        dtmThread = Data_Type_Manager("Data_Type_Manager-" + str(_threadCount), xml_loadfile.connection, q)
        _dtm_list.append(dtmThread)
        dtmThread.start()
    # be sure to join all the threads to wait on them
    for dtm in _dtm_list:
        dtm.join()
    logging.info("finished starting threads")
    load_time_end = time.perf_counter()
    load_time = timedelta(seconds=load_time_end - load_time_start)

    logging.info("    >>> Total load time: %s", str(load_time))
    logging.info("End time: %s", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")

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
