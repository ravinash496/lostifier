#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.dbinit
.. moduleauthor:: Tom Weitzel

Initializes the ECRF and LVF database.
"""

import logging
import psycopg2 as psycopg2


class EcrfDbInitializer(object):
    def __init__(self,
                 host: str='localhost',
                 port: int=5432,
                 database_name: str='srgis',
                 user_name: str=None,
                 password: str=None):
        """
        Constructor

        :param host: The host name of the database server.
        :type host: ``str``
        :param port: The port the database is listening on.
        :type port: ``str``
        :param database_name: The name of the database.
        :type database_name: ``str``
        :param user_name: The database connection user name.
        :type user_name: ``str``
        :param password: The database connection password.
        :type password: ``str``
        """
        self._host = host
        self._database_name = database_name
        self._port = port
        self._user_name = user_name
        self._password = password
        self._root_connection_string = 'host={0} user={1} password={2} dbname={3} port={4}'.format(
            self._host, self._user_name, self._password, 'postgres', self._port
        )

        self._connection_string = 'host={0} user={1} password={2} dbname={3} port={4}'.format(
            self._host, self._user_name, self._password, self._database_name, self._port
        )

        # Set up the base logging.
        self._logger = logging.getLogger('lostifier.dbinit.EcrfDbInitializer')
        self._logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(logging.DEBUG)
        consolehandler.setFormatter(formatter)
        self._logger.addHandler(consolehandler)

    def _execute_command(self, conn_string: str, command: str):
        """
        Executes the given sql command.

        :param conn_string: The connection string for the db to connect to.
        :param command: The command to execute.
        :return:
        """
        try:
            self._logger.info('Executing command . . .')
            with psycopg2.connect(conn_string) as con:
                con.autocommit = True
                with con.cursor() as cursor:
                    cursor.execute(command)
            self._logger.info('Done')
        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
            raise

    def _db_exists(self) -> bool:
        """
        Check to see if the database exists.

        :return: True if the database exists, false otherwise.
        :rtype: bool
        """
        exists = False
        try:
            self._logger.info('Checking for existence of {} database . . .'.format(self._database_name))
            with psycopg2.connect(self._root_connection_string) as con:
                con.autocommit = True
                with con.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) != 0 FROM pg_catalog.pg_database WHERE datname = '{0}'".format(self._database_name))
                    exists_row = cursor.fetchone()
                    exists = exists_row[0]
            self._logger.info('Database already exists.' if exists else 'Database does not exist.')
        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
            raise

        return exists

    def initialize(self):
        """
        Do all the things to set up the database.

        :return:
        """

        if not self._db_exists():
            self._logger.info('Creating the {0} database . . .'.format(self._database_name))
            self._execute_command(self._root_connection_string, 'CREATE DATABASE {0};'.format(self._database_name))
            self._logger.info('{0} database created.'.format(self._database_name))

        self._logger.info('Installing postgis extension . . .')
        self._execute_command(self._connection_string, 'CREATE EXTENSION IF NOT EXISTS postgis;')

        self._logger.info('Installing fuzzystrmatch extension . . .')
        self._execute_command(self._connection_string, 'CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;')

        self._logger.info('Setting up schemas . . .')
        schemas_command = """
            CREATE SCHEMA IF NOT EXISTS active;
            CREATE SCHEMA IF NOT EXISTS provisioning;
            ALTER DATABASE {0} SET SEARCH_PATH TO public, active;
            """.format(self._database_name)
        self._execute_command(self._connection_string, schemas_command)
        self._logger.info('Schemas up.')

        provisioning_history = """CREATE TABLE IF NOT EXISTS public.provisioning_history
                        (
                            ID uuid,
                            layer character varying(75) COLLATE pg_catalog."default",
                            load_type character varying(75) COLLATE pg_catalog."default",
                            row_count numeric(1000,0),
                            start_time timestamp with time zone,
                            end_time timestamp with time zone,
                            status character varying(75) COLLATE pg_catalog."default",
                            messages character varying(150) COLLATE pg_catalog."default"
                        )"""

        self._execute_command(self._connection_string, provisioning_history)
        self._logger.info('provisioning history table created')
        self._logger.info('{0} database up and ready for action!'.format(self._database_name))




