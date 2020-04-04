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
            conn._close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_to_cb ***", sys.exc_info()[0])


    # document handling methods


    def _writeDocuments(self, filename):
        """ Private method to insert bulk documents into the bucket."""
        try:
            logging.info('RunCB _writeDocuments for file :  ' + filename)
            for key in self._document_map.keys():
                self.conn.upsert_multi(self._document_map[key])
                logging.info('RunCB _writeDocuments:' + key + ' size is ' + str(sys.getsizeof(self._document_map[key])))
        except:
            e = sys.exc_info()[0]
            logging.error("*** %s Error writing to Couchbase: in RunCB writeDocument ***", str(e))
        finally:
            # reset the document map
            self._document_map = {}


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


    def _start_new_document(self, id, row, data_type, data_record):
        """Private method to start a new document - some of these fields are specifc to CB documents so they are in a local constants structure."""
        if (data_type in ['vsdb_V01_SL1L2', 'vsdb_V01_SAL1L2', 'vsdb_V01_VL1L2', 'vsdb_V01_VAL1L2']):
            self._document_map[data_type][id] = {
                CB.ID: id,
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
                CB.DATA:{str(row[CN.FCST_LEAD]):data_record}
            }
        else:
            logging.error("RunCB: _start_new_document: invalid dataType:" + data_type)


    def _handle_record_for_line_type(self, data_type, row, data_fields):
        """
        Private method for line type handling.

        If a key in the temporary documents map exists, append the data portion of this row to the data array, using the data_fields list of row keys.
        Otherwise insert the current temporary document and start a new temporary document with the header and first data record from this row.
        """
        # derive the id for this record
        id = self._get_id(data_type, row)
        # derive the data_record for this record
        _data_record = {CN.FCST_LEAD: str(row[CN.FCST_LEAD]), "total": str(row[0])}
        _i = 1
        for key in data_fields:
            _data_record[key] = str(row[_i])
            _i += 1
        # python ternary - create the _document_map[data_type][id] dict or get its reference if it exists already
        self._document_map[data_type]={} if not self._document_map.get(data_type) else self._document_map.get(data_type)
        self._document_map[data_type][id]={} if not self._document_map[data_type].get(id) else self._document_map[data_type].get(id)
        if not self._document_map[data_type][id].get(CB.ID):  # document might be uninitialized
            self._start_new_document(id, row, data_type, _data_record)  # start new document for this data_type
        else:
            self._document_map[data_type][id][CB.DATA][str(row[CN.FCST_LEAD])]=_data_record  # append the data_record to the document data list


    def write_cb(self, raw_data):
        """
        Method for creating and writing a document.

        Given a dataframe of raw_data with specific columns, convert rows to documents(data record rows for a document are combined into one data array) and write to bucket,
        """
        try:
            logging.info('RunCB write_cb - processing documents for couchbase - raw_data.data_files size is '+ str(raw_data.data_files.memory_usage(index=True).sum()))
            logging.info('RunCB write_cb - processing documents for couchbase - raw_data.stat_data size is '+ str(raw_data.stat_data.memory_usage(index=True).sum()))
            logging.info('RunCB write_cb - processing documents for couchbase - raw_data.mode_cts_data size is ' + str(raw_data.mode_cts_data.memory_usage(index=True).sum()))
            logging.info('RunCB write_cb - processing documents for couchbase - raw_data.mode_obj size is '+ str(raw_data.mode_obj_data.memory_usage(index=True).sum()))
            if raw_data.stat_data.empty == False:
                # get the file_rows - need them to get the file name extension
                file_dict = raw_data.data_files[[CN.FILEPATH, CN.FILE_ROW, CN.FILENAME]].to_dict()
                # sort the dataframe in place - order is very important to create the documents correctly
                #raw_data.stat_data.sort_values(by=[CN.FILE_ROW, CN.VERSION, CN.LINE_TYPE, CN.MODEL, CN.VX_MASK, CN.FCST_VAR, CN.OBTYPE, CN.FCST_LEV], ascending=True, inplace=True)
                raw_data.stat_data.sort_values(by=[CN.FILE_ROW], ascending=True, inplace=True)
                # iterate all the rows
                _follow_file_index = 0
                _current_file_index = 0
                for index, row in raw_data.stat_data.iterrows():
                    try:
                        _current_file_index = row[CN.FILE_ROW]
                        if index == 0:
                            _follow_file_index = _current_file_index
                        _filename = file_dict[CN.FILENAME][_current_file_index]
                        file_type = _filename.split('.')[1]
                        line_type = row[CN.LINE_TYPE]
                        _data_type = file_type + '_' + row[CN.VERSION] + '_' + line_type
                        _data_fields = (list(set(CN.LINE_DATA_FIELDS[line_type]) - set(CN.TOT_LINE_DATA_FIELDS)))
                        if _current_file_index != _follow_file_index:
                            # the row has changed - write the documents for this file
                            _filenamefq = file_dict[CN.FILEPATH][_follow_file_index] + '/' + file_dict[CN.FILENAME][_follow_file_index]
                            self._writeDocuments(_filenamefq) # will write out the current documents for this file and reset the document map.
                            _follow_file_index = _current_file_index
                        self._handle_record_for_line_type(_data_type, row, _data_fields)
                    except:
                        e = sys.exc_info()[0]
                        logging.error("*** %s in write_cb ***", str(e))
                _filenamefq = file_dict[CN.FILEPATH][_current_file_index] + '/' + file_dict[CN.FILENAME][_current_file_index]
                self._writeDocuments(_filenamefq)  # will write out the last documents and reset the document map.
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in run_cb write_cb ***", sys.exc_info()[0])
