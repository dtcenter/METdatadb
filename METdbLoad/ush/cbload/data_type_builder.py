"""
Program Name: Class Data_Type_Builder.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: Abstract Data_Type_Builder has common code for concrete data_type_builders. A pool of instantiated data_type_builders is
        used by a Data_Type_Manager to parse a MET output file and produce a set of documents suitable for insertion into couchbase.
        Concrete instantiations convert a line of a MET output file to a document map that is keyed by a document ID.
        Each concrete builder must over ride the function ...
        def handle_line(self, data_type, line, document_map, database_name):
        ....
        which will be called from the Data_Type_MAnager like this ...
        builder.handle_line(data_type, line, document_map, database_name)
        ....
        where builder is the concrete builder instance, line is the line to be parsed, data_type is the data_type of the line,
        data_base name is just a name that will be used in the subset field of an output document, and document_map
        is a map that is maintained ny the builder manager.
        Each concrete builder must create these two lists on construction...
            self.header_field_names = a standardized ordered list of HEADER fields for this data_type.
            self.data_field_names = a standardized ordered list of DATA fields for this data_type.
            The order of these fields is specific to the lines of the MET input file for a given data_type.

        Each concrete subclass method handle_line must derive from the line
            a record which is a dictionary of fields that is keyed by header fields and data fields and contains values
            that are parsed from the line.
            The handle_line method then uses the record HEADER fields to create a unique id specific to the data_type.
            The record and id are then used to either start a new entry into the document_map[data_type] dictionary that is keyed by the
            id, or to derive and add a data_record dictionary from the record to the document_map[data_type][id][data] dictionary.

        Attributes:
        self.header_field_names - the ordered list of HEADER field names
        self.data_field_names - the  ordered list of DATA field names

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import logging
import re
import sys
from abc import ABC

import constants as CN
import cb_constants as CB


class Data_Type_Builder(ABC):
    # Abstract Class for data_type builders
    def __init__(self):
        # The Constructor for the RunCB class.
        self.header_field_names = None
        self.data_field_names = None

    # common helper methods for VSDB_V01_L1L2 line types i.e. SL1L2, SAL1L2, VL1L2, VAL1L2
    def get_ID_VSDB_V01_L1L2(self, record):
        # Private method to derive a document id from the current line.
        id = "DD::" + \
             record[CN.VERSION] + "::" + \
             record[CN.LINE_TYPE] + "::" + \
             record[CB.SUBSET] + "::" + \
             record[CN.MODEL] + "::" + \
             record[CN.VX_MASK] + "::" + \
             record[CN.FCST_VAR] + "::" + \
             record[CN.OBTYPE] + "::" + \
             record[CN.FCST_LEV] + "::" + \
             str(record[CN.FCST_VALID_BEG])
        return id

    def get_data_record_VSDB_V01_L1L2(self, record):
        try:
            data_record = {CN.FCST_LEAD: str(record[CN.FCST_LEAD])}  # want to include FCST_LEAD
            for key in self.data_field_names:
                try:
                    data_record[key] = str(record[key])
                except:  # there might not be a filed (sometimes vsdb records are truncated)
                    data_record[key] = None
            return data_record
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " + self.__class__.__name__ + " get_data_record_VSDB_V01_L1L2 error: " + e)
            return {}

    def parse_line_to_record_VSDB_V01_L1L2(self, line, database_name):
        document_fields = self.header_field_names + self.data_field_names
        self._record = {}
        record_fields = ' '.join(re.split("\s|=", line)).split()
        i = 0
        while (i < len(document_fields) - 1):
            try:  # index of record might be out of range since VSDB files often do have the last field
                self._record[document_fields[i]] = record_fields[i]
            except:
                self._record[document_fields[i]] = None
            i = i + 1
        self._record[CB.SUBSET] = database_name
        return self._record

    def start_new_document_VSDB_V01_L1L2(self, data_type, record, document_map, database_name):
        # Private method to start a new document - some of these fields are specifc to CB documents so they are in a local constants structure.
        try:
            data_record = self.get_data_record_VSDB_V01_L1L2(record)
            keys = record.keys()
            id = self.get_ID_VSDB_V01_L1L2(record)
            document_map[data_type][id] = {
                CB.ID: id,
                CB.TYPE: "DataDocument",
                CB.DATATYPE: data_type,
                CB.SUBSET: database_name,
                CB.DATAFILE_ID: "DF_id",  # placeholder this is To Be Determined!   TODO!!!
                CB.DATASOURCE_ID: "DS_id",  # placeholder this is To Be Determined!   TODO!!!
                CN.VERSION: record[CN.VERSION] if CN.VERSION in keys else None,
                CN.MODEL: record[CN.MODEL] if CN.MODEL in keys else None,
                CB.GEOLOCATION_ID: record[CN.VX_MASK] if CN.VX_MASK in keys else None,
                CN.OBTYPE: record[CN.OBTYPE] if CN.OBTYPE in keys else None,
                CN.FCST_VALID_BEG: str(record[CN.FCST_VALID_BEG]) if CN.FCST_VALID_BEG in keys else None,
                CN.FCST_VAR: record[CN.FCST_VAR] if CN.FCST_VAR in keys else None,
                CN.FCST_UNITS: record[CN.FCST_UNITS] if CN.FCST_UNITS in keys else None,
                CN.FCST_LEV: record[CN.FCST_LEV] if CN.FCST_LEV in keys else None,
                #CB.DATA: {record[CN.FCST_LEAD]: data_record}
                CB.DATA: [data_record]
            }
            # logging.info("started record for document")
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " + self.__class__.__name__ + " start_new_document_VSDB_V01_L1L2 error: " + e)

    def handle_line(self, data_type, line, document_map, database_name):
        pass


# Concrete data_type builders:
# Each data_type builder has to be able to do two things.
# one: construct the self._document_field_names list that is an ordered list of field names, first header then data fields,
# that correlates positionally to each line of a specific builder type i.e. VSDB_V001_SL1L2.
# using standardized names from the cn constants
# two: implement _handle_line(self, data_type, record):
# where data_type is the datatype of a given line i.e. VSDB_V001_SL1L2 and record is a map derived from the parsed line and the self._document_field_names
#
class VSDB_V01_SL1L2_builder(Data_Type_Builder):
    # This data_type builder can leverage the parent self.start_new_document_VSDB_V01_L1L2, and
    # self._handle_line_VSDB_V01_L1L2 because they are same for several data types.
    def __init__(self):
        super(VSDB_V01_SL1L2_builder, self).__init__()
        # derive my headers and data fields - don't know why total is not part of CN.LINE_DATA_FIELDS[CN.SL1L2]
        self.header_field_names = CN.VSDB_HEADER
        self.data_field_names = [CN.TOTAL_LC] + (
            list(set(CN.LINE_DATA_FIELDS[CN.SL1L2]) - set(CN.TOT_LINE_DATA_FIELDS)))

    def handle_line(self, data_type, line, document_map, database_name):
        try:
            record = self.parse_line_to_record_VSDB_V01_L1L2(line, database_name)
            # derive the id for this record
            id = self.get_ID_VSDB_V01_L1L2(record)
            # python ternary - create the document_map[data_type][id] dict or get its reference if it exists already
            document_map[data_type] = {} if not document_map.get(data_type) else document_map.get(data_type)
            document_map[data_type][id] = {} if not document_map[data_type].get(id) else document_map[data_type].get(id)
            if not document_map[data_type][id].get(CB.ID):  # document might be uninitialized
                # start new document for this data_type
                self.start_new_document_VSDB_V01_L1L2(data_type, record, document_map, database_name)
            else:
                # append the data_record to the document data array
                document_map[data_type][id][CB.DATA].append(self.get_data_record_VSDB_V01_L1L2(record))
            # logging.info("added data record to document")
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder: " + self.__class__.__name__ + " error: " + e)


class VSDB_V01_SAL1L2_builder(Data_Type_Builder):
    # This data_type builder can leverage the parent self.start_new_document_VSDB_V01_L1L2, and
    # self._handle_line_VSDB_V01_L1L2 because they are same for several data types.
    def __init__(self):
        super(VSDB_V01_SAL1L2_builder, self).__init__()
        # derive my headers and data fields - don't know why total is not part of CN.LINE_DATA_FIELDS[CN.SL1L2]
        self.header_field_names = CN.VSDB_HEADER
        self.data_field_names = [CN.TOTAL_LC] + (
            list(set(CN.LINE_DATA_FIELDS[CN.SAL1L2]) - set(CN.TOT_LINE_DATA_FIELDS)))

    def handle_line(self, data_type, line, document_map, database_name):
        try:
            record = self.parse_line_to_record_VSDB_V01_L1L2(line, database_name)
            # derive the id for this record
            id = self.get_ID_VSDB_V01_L1L2(record)
            # python ternary - create the document_map[data_type][id] dict or get its reference if it exists already
            document_map[data_type] = {} if not document_map.get(data_type) else document_map.get(
                data_type)
            document_map[data_type][id] = {} if not document_map[data_type].get(id) else document_map[
                data_type].get(id)
            if not document_map[data_type][id].get(CB.ID):  # document might be uninitialized
                self.start_new_document_VSDB_V01_L1L2(data_type, record, document_map,
                                                      database_name)  # start new document for this data_type
            else:
                # append the data_record to the document data array
                document_map[data_type][id][CB.DATA].append(self.get_data_record_VSDB_V01_L1L2(record))
            # logging.info("added data record to document")
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder: " + self.__class__.__name__ + " error: " + e)


class VSDB_V01_VL1L2_builder(Data_Type_Builder):
    # This data_type builder can leverage the parent self.start_new_document_VSDB_V01_L1L2, and
    # self._handle_line_VSDB_V01_L1L2 because they are same for several data types.
    def __init__(self):
        super(VSDB_V01_VL1L2_builder, self).__init__()
        # derive my headers and data fields - don't know why total is not part of CN.LINE_DATA_FIELDS[CN.SL1L2]
        self.header_field_names = CN.VSDB_HEADER
        self.data_field_names = [CN.TOTAL_LC] + (
            list(set(CN.LINE_DATA_FIELDS[CN.VL1L2]) - set(CN.TOT_LINE_DATA_FIELDS)))

    def handle_line(self, data_type, line, document_map, database_name):
        try:
            record = self.parse_line_to_record_VSDB_V01_L1L2(line, database_name)
            # derive the id for this record
            id = self.get_ID_VSDB_V01_L1L2(record)
            # python ternary - create the document_map[data_type][id] dict or get its reference if it exists already
            document_map[data_type] = {} if not document_map.get(data_type) else document_map.get(
                data_type)
            document_map[data_type][id] = {} if not document_map[data_type].get(id) else document_map[
                data_type].get(id)
            if not document_map[data_type][id].get(CB.ID):  # document might be uninitialized
                self.start_new_document_VSDB_V01_L1L2(data_type, record, document_map,
                                                      database_name)  # start new document for this data_type
            else:
                # append the data_record to the document data array
                document_map[data_type][id][CB.DATA].append(self.get_data_record_VSDB_V01_L1L2(record))
            # logging.info("added data record to document")
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder: " + self.__class__.__name__ + " error: " + e)


class VSDB_V01_VAL1L2_builder(Data_Type_Builder):
    # This data_type builder can leverage the parent self.start_new_document_VSDB_V01_L1L2, and
    # self._handle_line_VSDB_V01_L1L2 because they are same for several data types.
    def __init__(self):
        super(VSDB_V01_VAL1L2_builder, self).__init__()
        # derive my headers and data fields - don't know why total is not part of CN.LINE_DATA_FIELDS[CN.SL1L2]
        self.header_field_names = CN.VSDB_HEADER
        self.data_field_names = [CN.TOTAL_LC] + (
            list(set(CN.LINE_DATA_FIELDS[CN.VAL1L2]) - set(CN.TOT_LINE_DATA_FIELDS)))

    def handle_line(self, data_type, line, document_map, database_name):
        try:
            record = self.parse_line_to_record_VSDB_V01_L1L2(line, database_name)
            # derive the id for this record
            id = self.get_ID_VSDB_V01_L1L2(record)
            # python ternary - create the document_map[data_type][id] dict or get its reference if it exists already
            document_map[data_type] = {} if not document_map.get(data_type) else document_map.get(
                data_type)
            document_map[data_type][id] = {} if not document_map[data_type].get(id) else document_map[
                data_type].get(id)
            if not document_map[data_type][id].get(CB.ID):  # document might be uninitialized
                self.start_new_document_VSDB_V01_L1L2(data_type, record, document_map,
                                                      database_name)  # start new document for this data_type
            else:
                # append the data_record to the document data array
                document_map[data_type][id][CB.DATA].append(self.get_data_record_VSDB_V01_L1L2(record))
            # logging.info("added data record to document")
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder: " + self.__class__.__name__ + " error: " + e)
