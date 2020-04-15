"""
Program Name: Class Data_Type_Manager.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The manager runs as a thread and is given a list of file names. It maintains its own connection which it keeps open until it finishes.
It gets filenames from a queue that is shared by a pool of data_type_manager's and processes them one at a time.
As it reads the file it determines which concrete builder to use for each line.
When it finishes the file it converts the data to a document and upserts it to the database.
        Attributes:
            file - a vsdb file
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import logging
import sys
import threading
from queue import Queue
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

import data_type_builder as DTB


class Data_Type_Manager(threading.Thread):
    def __init__(self, name, connection_credentials, q):
        #The Constructor for the RunCB class.
        threading.Thread.__init__(self)
        self.threadName = name
        self.connection_credentials = connection_credentials
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
            self.conn = cluster.open_bucket('m-data')
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s error in data_type_manager constructor ***", str(e))
            sys.exit("*** Error when connecting to database")


    def run(self):
        try:
            while True:
                logging.info("getting a filename - queue size is " + str(self.q.qsize()) )
                if self.q.empty():
                    break
                fileName = self.q.get_nowait()
                self.process_file(fileName)
                self.q.task_done()
        except:
            logging.error("*** %s Error in data_type_manager run ***", sys.exc_info()[0])
            logging.info('data_type_manager - disconnecting couchbase')
            self.conn._close()

    # process file
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
                builder.handle_record_for_line_type(data_type, line, self.document_map, self.database_name)
            except:
                e = sys.exc_info()[0]
                logging.error("Exception instantiating builder: " + dataTypeBuilderName + " error: " + e)

        # all the lines are now processed for this file so write the documents
        try:
            logging.info('data_type_manager writing documents for file :  ' + fileName + " threadName: " + self.threadName)
            for key in self.document_map.keys():
                self.conn.upsert_multi(self.document_map[key])
                logging.info('data_type_manager fileName writing documents:' + key + ' size is ' + str(
                    sys.getsizeof(self.document_map[key])))
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s Error writing to Couchbase: in data_type_manager writing document ***", str(e))
        finally:
            # reset the document map
            self.document_map = {}
