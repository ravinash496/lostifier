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
from lostifier.bulkload import BulkLoader
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
            (['-hn', '--hostname'], dict(action='store', help='The database host name.')),
            (['-p', '--port'], dict(action='store', help='The database port.')),
            (['-d', '--database'], dict(action='store', help='The name of the database.')),
            (['-u', '--username'], dict(action='store', help='The database username.')),
            (['-pwd', '--password'], dict(action='store', help='The database password.')),
        ]

    @expose(hide=True, aliases=['run'])
    def default(self):
        self.app.log.info('No command specified, proceeding with full GIS data load.')
        self.load_full()

    @expose(help="Load a full GIS dataset.")
    def load_full(self):
        self.app.log.info("Beginning full GIS dataset load.")
        try:
            bulkloader = self._build_bulkloader()
            bulkloader.full_gdb_import()
        except Exception:
            print('An error was encountered and the process has been terminated.')
            raise

    @expose(help="Load geodetic coverage data..")
    def load_changeonly(self):
        self.app.log.info("Beginning change only GIS dataset load.")
        try:
            bulkloader = self._build_bulkloader()
            bulkloader.change_only_gdb_import()
        except Exception:
            print('An error was encountered and the process has been terminated.')
            raise

    def _build_bulkloader(self) -> BulkLoader:
        """

        :return:
        """
        if not self.app.pargs.filegeodatabase \
                or not self.app.pargs.hostname \
                or not self.app.pargs.port \
                or not self.app.pargs.database \
                or not self.app.pargs.username \
                or not self.app.pargs.password:
            self.app.log.error('Missing one or more required parameters.')
            raise InvalidParameterException(
                'Missing one of --filegeodatabase, --hostname, --port, --database, --username, or --password.'
            )

        # The list of layers we want to load.
        layers_to_load = [
            'CountyBoundary', 'UnIncCommBoundary', 'IncMunicipalBoundary', 'StateBoundary', 'RoadCenterline', 'SSAP'
        ]

        return BulkLoader(
            self.app.pargs.filegeodatabase,
            self.app.pargs.hostname,
            self.app.pargs.database,
            self.app.pargs.port,
            self.app.pargs.username,
            self.app.pargs.password,
            'provisioning',
            layers_to_load)


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

    def _package_args(self, require_civic: bool = False, require_geodetic: bool = False) -> CoverageArguments:
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
