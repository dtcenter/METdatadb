#!/usr/bin/env python3

"""
Program Name: setup.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: setup file
Parameters: N/A
Input Files: N/A
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

from setuptools import setup, find_packages

setup(
    name='METdbLoad',
    version='0.1.0',
    description='Rewrite of Java MVLoad for METviewer and METexpress database load',
    author='Venita Hagerty',
    author_email='venita.hagerty@noaa.gov',
    packages=find_packages(exclude=['doc', 'tests']),
    long_description=open('README.md').read(), install_requires=['pandas', 'numpy', 'pymysql']
)
