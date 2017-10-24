#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.models
.. moduleauthor:: Tom Weitzel

Model classes used in various lostifier functions.
"""


class DbConnectionArguments(object):
    """
    A class that contains the necessary arguments for connecting to the database.
    """
    def __init__(self, dbhost: str, dbport: int, dbname: str, dbuser: str, dbpassword: str):
        """
        Constructor

        :param dbhost: The database host.
        :type dbhost: ``str``
        :param dbport: The database port.
        :type dbhost: ``int``
        :param dbname: The database name.
        :type dbname: ``str``
        :param dbuser: The database username.
        :type dbuser: ``str``
        :param dbpassword: The database password.
        :type dbpassword: ``str``
        """
        super().__init__()
        self._dbhost = dbhost
        self._dbport = int(dbport)
        self._dbname = dbname
        self._dbuser = dbuser
        self._dbpassword = dbpassword

    @property
    def dbhost(self) -> str:
        """
        Gets the dbhost name.

        :return: The db host name.
        :rtype: ``str``
        """
        return self._dbhost

    @property
    def dbport(self) -> int:
        """
        Gets the database port number.

        :return: The database port number.
        :rtype: ``int``
        """
        return self._dbport

    @property
    def dbname(self) -> str:
        """
        Gets the database name.

        :return: The database name.
        :rtype: ``str``
        """
        return self._dbname

    @property
    def dbuser(self) -> str:
        """
        Gets the database username.

        :return: The database user name.
        :rtype: ``str``
        """
        return self._dbuser

    @property
    def dbpassword(self) -> str:
        """
        Gets the database password.

        :return: The database password.
        :rtype: ``str``
        """
        return self._dbpassword


class CoverageArguments(DbConnectionArguments):
    """
    A class to package up the command arguments for performing the coverage load.
    """
    def __init__(self, shp: str, csv: str, dbhost: str, dbport: int, dbname: str, dbuser: str, dbpassword: str):
        """
        Constructor

        :param shp: The path to the shapefile.
        :type shp: ``str``
        :param csv: The path to the csv file.
        :type csv: ``str``
        :param dbhost: The database host.
        :type dbhost: ``str``
        :param dbport: The database port.
        :type dbhost: ``int``
        :param dbname: The database name.
        :type dbname: ``str``
        :param dbuser: The database username.
        :type dbuser: ``str``
        :param dbpassword: The database password.
        :type dbpassword: ``str``
        """
        super().__init__(dbhost, dbport, dbname, dbuser, dbpassword)
        self._shp = shp
        self._csv = csv

    @property
    def shp(self) -> str:
        """
        Gets the path to the shapefile.

        :return: The path to the shapefile.
        :rtype ``str``
        """
        return self._shp

    @property
    def csv(self) -> str:
        """
        Gets the path to the csv file.

        :return: The path to the csv file.
        :rtype: ``str``
        """
        return self._csv


class BulkloadArguments(DbConnectionArguments):
    """
    A class to package up the command arguments for performing the bulkload.
    """
    def __init__(self, fgdb: str, dbhost: str, dbport: int, dbname: str, dbuser: str, dbpassword: str):
        """
        Constructor.

        :param fgdb: The path to the file geodatabase with bulk data.
        :type fgdb: ``str``
        :param dbhost: The database host.
        :type dbhost: ``str``
        :param dbport: The database port.
        :type dbhost: ``int``
        :param dbname: The database name.
        :type dbname: ``str``
        :param dbuser: The database username.
        :type dbuser: ``str``
        :param dbpassword: The database password.
        :type dbpassword: ``str``
        """
        super().__init__(dbhost, dbport, dbname, dbuser, dbpassword)
        self._fgdb = fgdb

    @property
    def fgdb(self) -> str:
        """
        Gets the path to the file geodatabase.

        :return: The path to the file geodatabase.
        :rtype: ``str``
        """
