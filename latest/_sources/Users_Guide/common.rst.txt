.. _common:

Common XML Structures
=====================

METdbload and the batch plotting module both use XML input files. Some XML
structures can be used in either input file. These common structures are
documented below.

| **<connection>:** Information about the database connection.
|
|       **<host>:** Database hostname and port number in [host]:[port] format.
|       **<database>:** One or more existing database names that start with \'\mv_\'\  separated by commas. Only one name for loading.
|       **<user>:** Database user.
|       **<password>:** Database user's password.
|       **<management_system>:** Database type. Can be mysql, mariadb, or aurora.
|
| **</connection>**
|
| **<date_list>:** A structure that specifies a list of date strings in a certain format. Has a name attribute.
|
|       **<start>:** The start date and time of the date list, specified in the format given by the tag **<format>**
|       **<end>:** The end date and time, specified in the format given by the tag **<format>**
|       **<inc>:** The increment, in seconds, between successive members of the date list.
|       **<format>:** The date format, specified by the java class SimpleDateFormat, example yyyyMMddHH.
|
| **</date_list>**
