Load Data
=========

METdbload is used to insert MET output data into the database. The usage
statement:

.. code-block:: ini

  INFO:root:--- *** --- Start METdbLoad --- *** ---

  usage: met_db_load.py [-h] [-index] xmlfile [tmpdir [tmpdir ...]]

  positional arguments:
    xmlfile     Please provide required xml load_spec filename
    tmpdir      Optional - when different directory wanted for tmp file

  optional arguments:
    -h, --help  show this help message and exit
    -index      Only process index, do not load data

The **xmlfile** passes information about the MET output files to load
into the database to METdbload. It is an XML file whose top-level
tag is <load_spec> and it contains the following elements, divided into
functional sections:

  * **<connection>:** Please reference the :numref:`common` documentation.

  * **<date_list>:** Please reference the :numref:`common` documentation.

  * **<load_stat>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load STAT data.

  * **<load_mode>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load MODE data.

  * **<load_mtd>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load MODE TD data.

  * **<load_mpr>:** **TRUE** or **FALSE**, this option indicates whether or not
    to load matched pair data.

  * **<load_orank>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load observed rank data.

  * **<force_dup_file>:** **TRUE** or **FALSE**, this option indicates whether
    or not to force load paths/files that are already present.

  * **<verbose>:** **TRUE** or **FALSE**, this option indicates the desired
    volume of output from the load module, with TRUE resulting in more
    information and FALSE resulting in less information.

  * **<insert_size>:** An integer indicating the number of MET output file rows
    that are inserted with each INSERT statement. This value is most often 1.

  * **<stat_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for stat header information should be
    performed - **WARNING:** enabling this feature could significantly
    increase load time.

    **NOTE:** **<stat_header_table_check>** has been removed; remove it
    from the XML load specification document.

  * **<mode_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for MODE header information should be
    performed - **WARNING:** enabling this feature could significantly
    increase load time.

  * **<mtd_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for MODE TD header information should
    be performed - **WARNING:** enabling this feature could significantly
    increase load time.

  * **<drop_indexes>:** **TRUE** or **FALSE**, this option indicates whether
    database indexes should be dropped prior to loading new data.

  * **<load_indexes>:** **TRUE** or **FALSE**, this option indicates whether
    database indexes should be created after loading new data.

  * **<group>:** The name of the group for the user interface.

  * **<description>:** A short description of the database.

    * **<load_files>:** A list structure containing individual MET output
      files to load into the database.

    * **</load_files>:** Follows the list of files after the previous
      tag, to end the list.

    * **<file>:** Contains a single MET output file to load.

  * **<folder_tmpl>:** A template string describing the file structure of
    the input MET files, which is populated with values specified in
    the **<load_val>** tag structure.

    * **<load_val>:** A tree structure containing values used to populate
      the **<folder_tmpl>** template.

      * **<field>:** A template value, its name is specified by the attribute
	name, and its values are specified by its children **<val>** tags.

        * **<val>:** A single template value which will slot into the template
	  in the value specified by the parent field's name.

        * **<date_list>:** Specifies a previously declared **<date_list>**
	  element, using the name attribute, which represents a list of dates
	  in a particular format.

      * **<line_type>:** A list structure containing the MET output file line
	types to load. If omitted, all line types are loaded.

        * **<val>:** Contains a single MET output file line type to be loaded,
	  for example, CNT.

    * **<load_note>:** If present, creates a record in the instance_info
      database database table with a note containing the body of this tag

    * **<load_xml>:   TRUE** or **FALSE**, this option indicates whether or
      not to save the load xml; only effective if **<load_note>** is present
      - default: TRUE

  **Note**
  If <folder_tmpl> is used, at least one <load_val> entry should be present.
  For example, if the path is:

  .. code-block:: XML

    <folder_tmpl>/path/to/data</folder_tmpl>

  change it to

  .. code-block:: XML

    <folder_tmpl>/path/to/{type}</folder_tmpl>
    <load_val>
       <field name="type">
          <val>data</val>
       </field>
    </load_val>


Example
_______

Here is a simple example:

.. code-block:: XML

  <load_spec>
    <connection>
      <host>kemosabe:3306</host>
      <database>mv_db_hwt</database>
      <user>pgoldenb</user>
      <password>pgoldenb</password>
    </connection>

    <date_list name="folder_dates">
      <start>2010051914</start>
      <end>2010051915</end>
      <inc>3600</inc>
      <format>yyyyMMddHH</format>
    </date_list>


    <verbose>false</verbose>
    <insert_size>1</insert_size>
    <mode_header_db_check>true</mode_header_db_check>
    <drop_indexes>false</drop_indexes>
    <apply_indexes>true</apply_indexes>
    <group>Group name</group>
    <load_stat>true</load_stat>
    <load_mode>true</load_mode>
    <load_mtd>true</load_mtd>
    <load_mpr>false</load_mpr>

    <folder_tmpl>/d1/data/{model}/{vx_mask}/{valid_time}</folder_tmpl>
    <load_val>
      <field name="model">
        <val>arw</val>
        <val>nmm</val>
      </field>

      <field name="valid_time">
        <date_list name="folder_dates"/>
      </field>

      <field name="vx_mask">
         <val>FULL</val>
         <val>SWC</val>
      </field>
    </load_val>
  </load_spec>


In this example, the load module would attempt to load any files with the
suffix .stat in the following folders.

.. code-block:: ini

  /d1/data/arw/FULL/2010051914
  /d1/data/arw/SWC/2010051914
  /d1/data/nmm/FULL/2010051914
  /d1/data/nmm/SWC/2010051914
  /d1/data/arw/FULL/2010051915
  /d1/data/arw/SWC/2010051915
  /d1/data/nmm/FULL/2010051915
  /d1/data/nmm/SWC/2010051915
  ...

Troubleshooting
_______________
.. _test:

.. list-table::

  * -  Error:
    -  ** ERROR: Caught class
       com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException:
       Duplicate entry
       'CT07-NMM-LIN-R2-0-2005-07-15 12:00:00-2005-07-15 12:00:00-0-2005'
       for key 2
  * - Solution:
    - This error is caused by trying to insert a stat_header record into
      the database when an identical one already exists. If identical
      stat_header information is present in more than one stat file, set
      the <stat_header_db_check> value to true. This setting will reduce
      performance, because the stat_header table is checked for duplicate
      stat_header each time a row is inserted. However, if a stat_header
      row already exists in the table with the insert information, then
      the existing record will be used instead of trying to insert a
      dupilcate.

