.. _installation:

Software Installation/Getting Started
=====================================

Introduction
____________

This chapter describes how to install and get started using METdatadb.
METdatadb has been developed and tested on Mac and Linux operating
systems.  Support for additional platforms may be added in future releases.

METdbload is a Python3 program that requires some extra packages to be
available on the user's computer prior to installation.  METdbload is part
of the METdatadb database package. Additional METdatadb programs that work
with the data are planned.

Requirements
____________

METdatadb relies on the following tools. These must be installed and tested
prior to installing METdatadb:

**MySQL or MariaDB** - download and install the latest version. This can be
on a separate computer. Use "SET GLOBAL max_allowed_packet=110000000;" by
typing the command in the CLI and/or make the corresponding edit to
/etc/my.cnf, so that the change persists after the next reboot.

**AuroraDB** could alternately be used as a database in the cloud.

**Python 3.6+** - Python 3.6 or higher must be installed. METdatadb also
requires the Python packages pymysql, pandas, numpy, and lxml.

Installation
____________

Download
^^^^^^^^

Download METdatadb into an installation directory.  Programs can be run from
the ush subdirectory.

.. code-block:: ini

  git clone https://github.com/dtcenter/METdatadb [install]/METdatadb
  cd [install]/METdatadb/METdbLoad/ush

Create Database
^^^^^^^^^^^^^^^

Data must be loaded into a database which has the prefix \'\mv_\'\,
e.g. mv_met_data. This database must be structured with the METviewer
mv_mysql.sql schema:

.. code-block:: ini

  cd [install]/metviewer
  mysql -u[db_username] -p[db_password] -e'create database [db_name];'
  mysql -u[db_username] -p[db_password] [db_name] < sql/mv_mysql.sql


Create an XML load specification document which contains information about your
MET data. Run METdbload using the XML load specification as input (called
[load_xml] here) and monitor progress:

.. code-block:: ini

  python met_db_load.py [load_xml] [optional redirection of output, e.g. &> log/load_[date].log &]'
