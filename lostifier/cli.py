#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: coverage
.. moduleauthor:: Tom Weitzel

Utilities for initializing the coverage database and loading it with data.
"""

from lostifier.exception import InvalidParameterException
from lostifier.models import CoverageArguments
from lostifier.command import LoadInvoker
from lostifier.coverage import CoverageLoaderCommand, CivicCoverageLoader, GeodeticCoverageLoader
from cement.core import backend
from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose


class GisLoaderBaseController(CementBaseController):
    """
    The base controller for the GIS loader utility.
    """
    class Meta:
        label = 'base'
        description = 'ECRF and LVF GIS Data Loader Utility'

        config_defaults = dict(
            foo='bar',
            something_else='some other default'
        )

        arguments = [
            (['-fgdb', '--filegeodatabase'], dict(action='store', help='The path to the input file geodatabase.')),
            (['-hn', '--hostname'], dict(action='store_true', help='The database host name.')),
            (['-p', '--port'], dict(action='store_true', help='The database port.')),
            (['-d', '--database'], dict(action='store_true', help='The name of the database.')),
            (['-u', '--username'], dict(action='store_true', help='The database username.')),
            (['-pwd', '--password'], dict(action='store_true', help='The database password.')),
        ]

    @expose(hide=True, aliases=['run'])
    def default(self):
        self.app.log.info('Inside base.default function.')
        if self.app.pargs.csv:
            self.app.log.info("Received option 'csv' with value '%s'." % self.app.pargs.csv)

        if self.app.pargs.shp:
            self.app.log.info("Received option 'shp' with value '%s'." % self.app.pargs.shp)

    @expose(help="Load a full GIS dataset.")
    def load_full(self):
        self.app.log.info("Inside load_full function.")

    @expose(help="Load geodetic coverage data..")
    def load_changeonly(self):
        self.app.log.info("Inside load_changeonly function.")


class GisLoaderApp(CementApp):
    class Meta:
        label = 'gis_loader'
        base_controller = GisLoaderBaseController


class CoverageLoaderBaseController(CementBaseController):
    """
    The base controller for the coverage loader utility.
    """
    class Meta:
        label = 'base'
        description = 'ECRF and LVF Coverage Regions Loader Utility'

        config_defaults = dict(
            foo='bar',
            something_else='some other default'
        )

        arguments = [
            (['-c', '--csv'], dict(action='store', help='The path to the civic coverage CSV file.')),
            (['-s', '--shp'], dict(action='store', help='The path to the geodetic coverage shapefile.')),
            (['-hn', '--hostname'], dict(action='store', help='The database host name.')),
            (['-p', '--port'], dict(action='store', help='The database port.')),
            (['-d', '--database'], dict(action='store', help='The name of the database.')),
            (['-u', '--username'], dict(action='store', help='The database username.')),
            (['-pwd', '--password'], dict(action='store', help='The database password.')),
        ]

    @expose(hide=True, aliases=['run'])
    def default(self):
        self.app.log.info('Attempting to load both civic and geodetic coverage data . . .')
        self.load_civic()
        self.load_geodetic()

    @expose(help="Load civic coverage data.")
    def load_civic(self):
        self.app.log.info("Loading civic coverage data . . .")
        command_args = self._package_args(require_civic=True)
        receiver = CivicCoverageLoader(command_args)
        invoker = LoadInvoker()
        invoker.execute(CoverageLoaderCommand(receiver))
        self.app.log.info("Finished loading civic coverage data..")

    @expose(help="Load geodetic coverage data..")
    def load_geodetic(self):
        self.app.log.info("Loading geodetic coverage data . . .")
        command_args = self._package_args(require_geodetic=True)
        receiver = GeodeticCoverageLoader(command_args)
        invoker = LoadInvoker()
        invoker.execute(CoverageLoaderCommand(receiver))
        self.app.log.info("Finished loading geodetic coverage data .")

    def _package_args(self, require_civic:bool = False, require_geodetic:bool = False) -> CoverageArguments:
        """
        Uses the CLI arguments to create an instance of CoverageArguments.

        :return: An instance of CoverageArguments filled out with all of the arguments.
        :rtype: :py:class:``CoverageArguments``
        """
        if require_civic and not self.app.pargs.csv:
            self.app.log.error('No csv file path passed for civic coverage data.')
            raise InvalidParameterException('Missing required argument -c, --csv.')

        if require_geodetic and not self.app.pargs.shp:
            self.app.log.error('No shp file path passed for geodetic coverage data.')
            raise InvalidParameterException('Missing required argument -s, --shp.')

        if not self.app.pargs.hostname \
                or not self.app.pargs.port \
                or not self.app.pargs.database \
                or not self.app.pargs.username \
                or not self.app.pargs.password:
            self.app.log.error('Missing one or more required database parameters.')
            raise InvalidParameterException('Missing one of --hostname, --port, --database, --username, or --password.')

        return CoverageArguments(self.app.pargs.shp,
                                 self.app.pargs.csv,
                                 self.app.pargs.hostname,
                                 self.app.pargs.port,
                                 self.app.pargs.database,
                                 self.app.pargs.username,
                                 self.app.pargs.password)


class CoverageLoaderApp(CementApp):
    class Meta:
        label = 'coverage_loader'
        base_controller = CoverageLoaderBaseController
