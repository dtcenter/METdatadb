"""
Program Name: Class Data_Type_Manager.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The Data_Type_Manager extends threading.Thread and runs as a thread and pulls from a queue of file names.
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
            connection_credentials - a set of connection_credentials that the Data_Type_Manager will use to connect to the database. This
            connection will be maintained until the thread terminates.
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import logging

import sys
import threading

from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import data_type_builder as DTB


class Data_Type_Manager(threading.Thread):
    """
    Data_Type_Manager is a Thread that manages an object pool of Data_Type_builders to build lines from MET files
    into documents that can be inserted into couchbase.
    This class will process files by collecting filenames - one at a time - from a filename
    queue. For each filename it will read the file line by line and process each line.
    From each line it derives the data_type which is a combination of file extension, version, and line_type.
    It uses the data_type to either retrieve the reference of a corresponding data_type_builder from the object
    pool or instantiate an appropriate data_type_builder and put it in the pool and retrieve its reference.
    It uses the data_type_builder to process a line and either start a new document_map entry, or add a data_record from
    the line to an existing data_map entry.
    """
    def __init__(self, name, connection_credentials, q):
        # The Constructor for the RunCB class.
        threading.Thread.__init__(self)
        self.threadName = name
        self.connection_credentials = connection_credentials
        # made this an instance variable because I don't know how to pass it into the run method
        self.q = q
        self.builder_map = {}
        self.document_map = {}
        try:
            logging.info('data_type_manager - Connecting to couchbase')
            cluster = Cluster('couchbase://' + connection_credentials['db_host'])
            authenticator = PasswordAuthenticator(connection_credentials['db_user'],
                                                  connection_credentials['db_password'])
            cluster.authenticate(authenticator)
            self.database_name = connection_credentials['db_name']
            self.conn = cluster.open_bucket('mdata')
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s error in data_type_manager constructor ***", str(e))
            sys.exit("*** Error when connecting to database")


    # entry point of the thread. Is invoked automatically when the thread is started.
    def run(self):
        """
        This is the entry point for the Data_Type_Manager thread. It runs an infinite loop that only
        terminates when the fileName queue is empty. For each fileName it calls process_file with the fileName
        to process the file.
        """
        try:
            # infinite loop terminates when the queue is empty
            while True:
                logging.info("getting a filename - queue size is " + str(self.q.qsize()))
                if self.q.empty():
                    break
                fileName = self.q.get_nowait()
                self.process_file(fileName)
                self.q.task_done()
        except:
            logging.error("*** %s Error in data_type_manager run ***", sys.exc_info()[0])
            logging.info('data_type_manager - disconnecting couchbase')
            self.conn._close()

    # process a file line by line
    def process_file(self, fileName):
        self.document_map = {}
        file_extension = fileName.split('.')[-1]
        file = open(fileName, 'r')
        for line in file:
            # derive the data_type
            lparts = line.split()
            line_type = lparts[6]
            version = lparts[0]
            data_type = file_extension.upper() + '_' + version.upper() + '_' + line_type.upper()
            dataTypeBuilderName = data_type + "_builder"
            # get or instantiate the builder
            try:
                if (dataTypeBuilderName in self.builder_map.keys()):
                    builder = self.builder_map[dataTypeBuilderName]
                else:
                    builderClass = getattr(DTB, dataTypeBuilderName)
                    builder = builderClass()
                # process the line
                builder.handle_record_for_line_type(data_type, line, self.document_map, self.database_name)
            except:
                e = sys.exc_info()[0]
                logging.error("Exception instantiating builder: " + dataTypeBuilderName + " error: " + e)
        # all the lines are now processed for this file so write all the documents in the document_map
        try:
            logging.info(
                'data_type_manager writing documents for file :  ' + fileName + " threadName: " + self.threadName)
            for key in self.document_map.keys():
                self.conn.upsert_multi(self.document_map[key])
                # logging.info('data_type_manager fileName writing documents:' + key + ' size is ' + str(
                #     sys.getsizeof(self.document_map[key])))
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s Error writing to Couchbase: in data_type_manager writing document ***", str(e))
        finally:
            # reset the document map
            self.document_map = {}
