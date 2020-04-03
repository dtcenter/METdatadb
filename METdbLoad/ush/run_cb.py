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

import json
import logging
import sys

# couchbase
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

import constants as CN


# CB constants - basically extend CN
class CB:
    ID = "id"
    TYPE = "type"
    DATATYPE = "dataType"
    SUBSET = "subset"
    DATAFILE_ID = "dataFile_id"
    DATASOURCE_ID = "datasource_id"
    GEOLOCATION_ID = "geoLocation_id"
    DATA = "data"


class RunCB:
    """
        Class to handle converting the DataFrame to documents and writing (inserting) the resulting documents to a couchbase bucket.
        Attributes:
            CB : CB constants - extend CN.
            _document_map : temporary documents
            _database_name : database name from connection
    """


    _document_map = {} # temporary storage for documents
    _database_name = ""

    def __init__(self):
        """The Constructor for the RunCB class."""
        self.conn = None
        _document_map = {}
        _database_name = ""

    # connection management methods

    def cb_on(self, connection_credentials):
        """ Method to establish a connection to a CB database.
            Requires a connectionCredentials map.
        """
        try:
            logging.info('RunCB cb_on - Connecting to couchbase')
            cluster = Cluster('couchbase://' + connection_credentials['db_host'])
            authenticator = PasswordAuthenticator(connection_credentials['db_user'],
                                                  connection_credentials['db_password'])
            cluster.authenticate(authenticator)
            self._database_name = connection_credentials['db_name']
            self.conn = cluster.open_bucket('m-data')
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s in run_cb ***", str(e))
            sys.exit("*** Error when connecting to database")


    def cb_off(self, conn):
        """Method to write residual documents and disconnect from a CB database"""
        try:
            logging.info('RunCB cb_off - disconnecting couchbase')
            self.clean_up_temporary_documents()
            conn._close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])


    # document handling methods

    def clean_up_temporary_documents(self):
        """
            Method to insert any leftover documents and reset the temporary document storage.

            This method is not private because main program may want to insert the leftovers.
        """
        for key in self._document_map:
            self._writeDocument(self._document_map[key])
        _document_map = {}


    def _writeDocument(self, document):
        """ Private method to insert a document into the bucket."""
        try:
            self.conn.upsert(document[CB.ID], json.dumps(document))
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s Error writing to Couchbase: in RunCB writeDocument ***", str(e))


    def _get_id(self, data_type, row):
        """ Private method to derive a document id from the current row."""
        if (data_type in ['vsdb_V01_SL1L2', 'vsdb_V01_SAL1L2', 'vsdb_V01_VL1L2', 'vsdb_V01_VAL1L2']):
            id = "DD::" + \
                 row[CN.VERSION] + "::" + \
                 row[CN.LINE_TYPE] + "::" + \
                 row[CN.MODEL] + "::" + \
                 row[CN.VX_MASK] + "::" + \
                 row[CN.FCST_VAR] + "::" + \
                 row[CN.OBTYPE] + "::" + \
                 row[CN.FCST_LEV] + "::" + \
                 str(row[CN.FCST_VALID_BEG])
            return id
        else:
            logging.error("RunCB: _get_id: invalid dataType:" + data_type)


    def _start_new_document(self, row, data_type, data_record):
        """Private method to start a new document - some of these fields are specifc to CB documents so they are in a local constants structure."""
        if (data_type in ['vsdb_V01_SL1L2', 'vsdb_V01_SAL1L2', 'vsdb_V01_VL1L2', 'vsdb_V01_VAL1L2']):
            self._document_map[data_type] = {
                CB.ID: self._get_id(data_type, row),
                CB.TYPE: "DataDocument",
                CB.DATATYPE: data_type,
                CB.SUBSET: self._database_name,
                CB.DATAFILE_ID: "DF_id",
                CB.DATASOURCE_ID: "DS_id",
                CN.VERSION: row[CN.VERSION],
                CN.MODEL: row[CN.MODEL],
                CB.GEOLOCATION_ID: row[CN.VX_MASK],
                CN.OBTYPE: row[CN.OBTYPE],
                CN.FCST_VALID_BEG: str(row[CN.FCST_VALID_BEG]),
                CN.FCST_VAR: row[CN.FCST_VAR],
                CN.FCST_UNITS: row[CN.FCST_UNITS],
                CN.FCST_LEV: row[CN.FCST_LEV],
                CB.DATA: [data_record]
            }
        else:
            logging.error("RunCB: _start_new_document: invalid dataType:" + data_type)


    def _handle_record_for_line_type(self, data_type, row, data_fields):
        """
        Private method for line type handling.

        If a key in the temporary documents map exists, append the data portion of this row to the data array, using the data_fields list of row keys.
        Otherwise insert the current temporary document and start a new temporary document with the header and first data record from this row.
        """
        id = self._get_id(data_type, row)
        _data_record = {CN.FCST_LEAD: str(row[CN.FCST_LEAD]), "total": str(row[0])}
        _i = 1
        for key in data_fields:
            _data_record[key] = str(row[_i])
            _i += 1
        # python ternary - create the _document_map[data_type] dict or get its reference if it exists already
        self._document_map[data_type] = {} if not self._document_map.get(data_type) else self._document_map.get(
            data_type)
        if self._document_map.get(data_type) and self._document_map.get(data_type)[
            CB.ID] == id:  # document might be uninitialized
            self._document_map.get(data_type)['data'].append(
                _data_record)  # append the data_record to the document data list
        else:
            # Since the DataFrame was sorted this document is now finished (or brand new) and so here we will start a new record
            if self._document_map.get(data_type):  # if the document is initialized it has data i.e. it is finished
                self._writeDocument(self._document_map.get(
                    data_type))  # write the current document for this data_type - the document is not empty
            self._start_new_document(row, data_type, _data_record)  # start new document for this data_type


    def write_cb(self, raw_data):
        """
        Method for creating and writing a document.

        Given a dataframe of raw_data with specific columns, convert rows to documents(data record rows for a document are combined into one data array) and write to bucket,
        """
        try:
            logging.info('RunCB write_cb - writing documents to couchbase')
            if raw_data.stat_data.empty == False:
                # get the file_rows - need them to get the file name extension
                file_dict = raw_data.data_files[['file_row', 'filename']].to_dict()
                # sort the dataframe in place - order is very important to create the documents correctly
                raw_data.stat_data.sort_values(
                    by=[CN.VERSION, CN.LINE_TYPE, CN.MODEL, CN.VX_MASK, CN.FCST_VAR, CN.OBTYPE, CN.FCST_LEV],
                    ascending=True, inplace=True)
                # iterate all the rows
                for index, row in raw_data.stat_data.iterrows():
                    try:
                        file_type = file_dict[CN.FILENAME][row[CN.FILE_ROW]].split('.')[1]
                        line_type = row[CN.LINE_TYPE]
                        _data_type = file_type + '_' + row[CN.VERSION] + '_' + line_type
                        _data_fields = (list(set(CN.LINE_DATA_FIELDS[line_type]) - set(CN.TOT_LINE_DATA_FIELDS)))
                        self._handle_record_for_line_type(_data_type, row, _data_fields)
                    except:
                        e = sys.exc_info()[0]
                        logging.error("*** %s in write_cb ***", str(e))
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])
