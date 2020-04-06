#!/usr/bin/env python3

"""
Program Name: read_load_xml.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read load_spec XML file
Parameters: N/A
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
from pathlib import Path
import logging
from lxml import etree

import constants as CN


class XmlLoadFile:
    """! Class to read in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self, xmlfile):
        # set the defaults
        self.xmlfilename = xmlfile

        self.connection = {}
        self.connection['db_host'] = None
        self.connection['db_port'] = CN.SQL_PORT
        self.connection['db_name'] = None
        self.connection['db_user'] = None
        self.connection['db_password'] = None
        self.connection['db_management_system'] = "mysql"

        self.db_driver = None
        self.insert_size = 1
        self.load_note = None
        self.group = CN.DEFAULT_DATABASE_GROUP
        self.description = "None"
        self.xml_str = None

        self.flags = {}
        self.flags['line_type_load'] = False
        self.flags['load_stat'] = True
        self.flags['load_mode'] = True
        self.flags['load_mtd'] = True
        self.flags['load_mpr'] = False
        self.flags['load_orank'] = False
        self.flags['force_dup_file'] = False
        self.flags['verbose'] = False
        self.flags['stat_header_db_check'] = True
        self.flags['mode_header_db_check'] = True
        self.flags['mtd_header_db_check'] = True
        self.flags['drop_indexes'] = False
        self.flags['apply_indexes'] = False
        self.flags['load_xml'] = True

        self.load_files = []
        self.line_types = []

    def read_xml(self):
        """! Read in load_spec xml file, store values as class attributes
            Returns:
               N/A
        """

        logging.debug("[--- Start read_xml ---]")

        try:

            # check for existence of XML file
            if not Path(self.xmlfilename).is_file():
                sys.exit("*** XML file " + self.xmlfilename + " can not be found!")

            # parse the XML file
            parser = etree.XMLParser(remove_comments=True)
            tree = etree.parse(self.xmlfilename, parser=parser)
            root = tree.getroot()

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in XML file!")

        folder_template = None
        template_fills = {}

        try:
            # extract values from load_spec XML tags, store in attributes of class XmlLoadFile
            for child in root:
                if child.tag.lower() == "connection":
                    for subchild in list(child):
                        if subchild.tag.lower() == "host":
                            host_and_port = subchild.text.split(":")
                        elif subchild.tag.lower() == "user":
                            self.connection['db_user'] = subchild.text
                        elif subchild.tag.lower() == "password":
                            self.connection['db_password'] = subchild.text
                        elif subchild.tag.lower() == "database":
                            self.connection['db_name'] = subchild.text
                        elif subchild.tag.lower() == "management_system":
                            self.connection['db_management_system'] = subchild.text
                    # separate out the port if there is one
                    self.connection['db_host'] = host_and_port[0]
                    if len(host_and_port) > 1:
                        self.connection['db_port'] = int(host_and_port[1])
                    if (not self.connection['db_host']) or (not self.connection['db_name']):
                        logging.warning("!!! XML must include host and database tags")
                    if (not self.connection['db_user']) or (not self.connection['db_password']):
                        logging.warning("!!! XML must include user and passsword tags")
                    if not self.connection['db_name'].startswith("mv_"):
                        logging.warning("!!! Database not visible unless name starts with mv_")
                elif child.tag.lower() == "load_files":
                    for subchild in list(child):
                        self.load_files.append(subchild.text)
                elif child.tag.lower() == "folder_tmpl":
                    folder_template = child.text
                # get the values to fill in to the folder template
                elif child.tag.lower() == "load_val":
                    for subchild in list(child):
                        template_key = subchild.get("name")
                        template_values = []
                        for template_value in list(subchild):
                            template_values.append(template_value.text)
                        template_fills[template_key] = template_values
                elif child.tag.lower() == "verbose":
                    if child.text.lower() == CN.LC_TRUE:
                        self.flags['verbose'] = True
                elif child.tag.lower() == "drop_indexes":
                    if child.text.lower() == CN.LC_TRUE:
                        self.flags['drop_indexes'] = True
                elif child.tag.lower() == "apply_indexes":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['apply_indexes'] = False
                elif child.tag.lower() == "stat_header_db_check":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['stat_header_db_check'] = False
                elif child.tag.lower() == "mode_header_db_check":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['mode_header_db_check'] = False
                elif child.tag.lower() == "mtd_header_db_check":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['mtd_header_db_check'] = False
                elif child.tag.lower() == "load_stat":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['load_stat'] = False
                elif child.tag.lower() == "load_mode":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['load_mode'] = False
                elif child.tag.lower() == "load_mtd":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['load_mtd'] = False
                elif child.tag.lower() == "load_mpr":
                    if child.text.lower() == CN.LC_TRUE:
                        self.flags['load_mpr'] = True
                elif child.tag.lower() == "load_orank":
                    if child.text.lower() == CN.LC_TRUE:
                        self.flags['load_orank'] = True
                elif child.tag.lower() == "force_dup_file":
                    if child.text.lower() == CN.LC_TRUE:
                        self.flags['force_dup_file'] = True
                elif child.tag.lower() == "insert_size":
                    if child.text.isdigit():
                        self.insert_size = int(child.text)
                # group and description for putting databases into groups/categories
                elif child.tag.lower() == "group":
                    self.group = child.text
                elif child.tag.lower() == "description":
                    self.description = child.text
                # load_note and load_xml are used to put a note in the database
                elif child.tag.lower() == "load_note":
                    self.load_note = child.text
                elif child.tag.lower() == "load_xml":
                    if child.text.lower() == CN.LC_FALSE:
                        self.flags['load_xml'] = False
                # MET line types to load. If omitted, all line types are loaded
                elif child.tag.lower() == "line_type":
                    self.flags['line_type_load'] = True
                    for subchild in list(child):
                        self.line_types.append(subchild.text.upper())
                else:
                    logging.warning("!!! Unknown tag: %s", child.tag)

            # if requested, get a string of the XML to put in the database
            if self.flags['load_xml']:
                self.xml_str = etree.tostring(tree).decode().replace('\n', '').replace(' ', '')

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading XML file!")

        logging.debug("db_name is: %s", self.connection['db_name'])

        # generate all possible path/filenames from folder template
        if folder_template is not None:
            self.load_files = self.filenames_from_template(folder_template, template_fills)

        # this removes duplicate file names. do we want that?
        if self.load_files is not None:
            self.load_files = list(dict.fromkeys(self.load_files))

        # remove directory names
        self.load_files = [lf for lf in self.load_files if '.' in lf.split('/')[-1]]

        logging.debug("Initial number of files: %s", str(len(self.load_files)))

        logging.debug("[--- End read_xml ---]")


    @staticmethod
    def filenames_from_template(folder_template, template_fills):
        """! given a folder template and the values to fill in, generates list of filenames
            Returns:
               list of filenames
        """
        logging.debug("folder template is: %s", folder_template)

        try:

            fills_open = folder_template.count("{")
            if fills_open != folder_template.count("}"):
                raise ValueError("mismatched curly braces")
            # remove any fill values that are not in the template
            if template_fills:
                copy_template_fills = dict(template_fills)
                for key in copy_template_fills:
                    if key not in folder_template:
                        del template_fills[key]
            if fills_open > len(template_fills):
                raise ValueError("not enough template fill values")
            # generate a list of directories with all combinations of values filled in
            load_dirs = [folder_template]
            for key in template_fills:
                alist = []
                for fvalue in template_fills[key]:
                    for tvalue in load_dirs:
                        alist.append(tvalue.replace("{" + key + "}", fvalue))
                load_dirs = alist
            # find all files in directories, append path to them, and put on load_files list
            file_list = []
            for file_dir in load_dirs:
                if os.path.exists(file_dir):
                    for file_name in os.listdir(file_dir):
                        file_list.append(file_dir + "/" + file_name)

        except ValueError as value_error:
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML folder templates!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML folder templates!")

        return file_list
