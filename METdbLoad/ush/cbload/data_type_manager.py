"""
Program Name: Class DataTypeManager.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

IMPORTANT NOTE ABOUT PYTHON THREADS!!!
Please read https://docs.couchbase.com/python-sdk/2.0/threads.html
Due to the Global Interpreter Lock (GIL), only one python thread can execute Python code at a time.
See  https://docs.python.org/2/library/threading.html.
However Python also has multiprocessing which can be used if the memory footprint of the application is small enough.
This program uses multiprocessing which essentially uses an entire interpreter for each thread, thus avoiding the GIL.
Because of this the couchbase SDK recommends that a different bucket is used for each multiprocess thread and they are very
sure to point out that if you do not honor this your program WILL HAVE UNEXPECTED ERRORS! So if you modify this thread relationship
between the main program and the DataTypeManager be very very careful.
Observations have shown that two or four threads help reduce the execution time of the program significantly. More than that does not.
I would have left out threading all-together but someday we may decide to port this to a truly thread capable language.

Usage: The DataTypeManager extends Process - python multiprocess thread - and runs as a Process and pulls from a queue of file names.
It maintains its own connection to the database which it keeps open until it finishes.
It finishes and closes its database connection when the queue is empty.
It gets filenames serially from a queue that is shared by a thread pool of data_type_manager's and processes them one at a time.
As it reads the file it derives the data_type from the line  and uses a concrete builder to process the line.
The builders are instantiated once and kept in a map of objects for the duration of the programs life.
When it finishes a file it converts the data in the document_map into a document and "upserts" it to the database. MET files
can have many data_types in a single file, and each line contains and id that the concrete builder constructs. An id might be made up
for example of an init time, a data_type, a forecast variable, a level etc. So the document map structure ends up looking like...
document_map[data_type][id] which indexes a dictionary that has header field entries, and a data dictionary. This results in a
structure like  document_map[data_type][id][data] which is a dictionary that is keyed by forecast_lead. This means an individual
data record could be indexed by document_map[data_type][id][data][forecast_lead] which is a dictionary of data fields.
The records at the level of document_map[data_type][id] represent a couchbase document and are "upserted" keyed by id.
        Attributes:
            queue - a queue of filenames that are MET files.
            threadName - a threadName for logging and debugging purposes.
            connection_credentials - a set of connection_credentials that the DataTypeManager will use to connect to the database. This
            connection will be maintained until the thread terminates.
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import logging
import sys
import time
from multiprocessing import Process
import queue

from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator

import data_type_builder as DTB


class DataTypeManager(Process):
    """
    DataTypeManager is a Thread that manages an object pool of Data_Type_builders to build lines from MET files
    into documents that can be inserted into couchbase.
    This class will process files by collecting filenames - one at a time - from a filename
    queue. For each filename it will read the file line by line and process each line.
    From each line it derives the data_type which is a combination of file extension, version, and line_type.
    It uses the data_type to either retrieve the reference of a corresponding data_type_builder from the object
    pool or instantiate an appropriate data_type_builder and put it in the pool and retrieve its reference.
    It uses the data_type_builder to process a line and either start a new document_map entry, or add a data_record from
    the line to an existing data_map entry.
    """

    def __init__(self, name, connection_credentials, queue):
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.connection_credentials = connection_credentials
        # made this an instance variable because I don't know how to pass it into the run method
        self.queue = queue

        self.builder_map = {}
        self.document_map = {}
        self.database_name = ""

    # entry point of the thread. Is invoked automatically when the thread is started.
    def run(self):
        """
        This is the entry point for the DataTypeManager thread. It runs an infinite loop that only
        terminates when the file_name queue is empty. For each file_name it calls process_file with the file_name
        to process the file.
        """
        try:
            logging.info('data_type_manager - Connecting to couchbase')
            # get a reference to our cluster
            # derive the path to the public certificate for the host
            # NOTE this assumes that the public cert for the database host (which is probably a cluster)
            # has previously been put in /certs/hostname/hostname.pem
            cluster = Cluster('couchbases://' + self.connection_credentials['db_host'], ClusterOptions(
                PasswordAuthenticator(self.connection_credentials['db_user'],
                                      self.connection_credentials['db_password'],
                                      cert_path="/certs/adb-cb4.gsd.esrl.noaa.gov/adb-cb4.gsd.esrl.noaa.gov.pem")))
            self.database_name = self.connection_credentials['db_name']
            collection = cluster.bucket("mdata").default_collection()

            # infinite loop terminates when the queue is empty
            empty_count = 0
            while True:
                try:
                    file_name = self.queue.get_nowait()
                    self.process_file(file_name, collection)
                    self.queue.task_done()
                    empty_count = 0
                except queue.Empty:
                    if empty_count < 3:
                        empty_count += 1
                        logging.info('data_type_manager - got Queue.Empty - retrying: ' + str(empty_count) +
                                     " of 3 times")
                        time.sleep(1)
                        continue
                    else:
                        logging.info('data_type_manager - Queue empty - disconnecting couchbase')
                        break
        except:
            logging.error("*** %s Error in data_type_manager run ***", sys.exc_info()[0])
            logging.info('data_type_manager - disconnecting couchbase')

    # process a file line by line
    def process_file(self, file_name, collection):
        self.document_map = {}
        file_extension = file_name.split('.')[-1]
        file = open(file_name, 'r')
        for line in file:
            # derive the data_type
            line_parts = line.split()
            line_type = line_parts[6]
            version = line_parts[0]
            data_type = file_extension.upper() + '_' + version.upper() + '_' + line_type.upper()
            data_type_builder_name = data_type + "_builder"
            # get or instantiate the builder
            try:
                if data_type_builder_name in self.builder_map.keys():
                    builder = self.builder_map[data_type_builder_name]
                else:
                    builder_class = getattr(DTB, data_type_builder_name)
                    builder = builder_class()
                    self.builder_map[data_type_builder_name] = builder
                # process the line
                builder.handle_line(data_type, line, self.document_map, self.database_name)
            except:
                e = sys.exc_info()[0]
                logging.error("Exception instantiating builder: " + data_type_builder_name + " error: " + e)
        # all the lines are now processed for this file so write all the documents in the document_map
        try:
            logging.info(
                'data_type_manager writing documents for file :  ' + file_name + " threadName: " + self.threadName)
            for key in self.document_map.keys():
                try:
                    # this call is volatile i.e. it might change syntax in the future.
                    # if it does, please just fix it.
                    collection.upsert_multi(self.document_map[key])
                except:
                    e = sys.exc_info()[0]
                    logging.error("*** %s Error multi-upsert to Couchbase: in data_type_manager ***", str(e))
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s Error writing to Couchbase: in data_type_manager writing document ***", str(e))
        finally:
            # reset the document map
            self.document_map = {}
