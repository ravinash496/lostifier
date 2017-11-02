#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: coverage
.. moduleauthor:: Tom Weitzel

Classes for performing loads of coverage data.
"""

from lostifier.command import LoadCommand
from lostifier.db.csv.datasource import CsvDataSource
from lostifier.db.pg.datasource import PostgresTabularDataSource, PostgisDataSource
from lostifier.db.shp.datasource import ShpDataSource
from lostifier.models import CoverageArguments
from abc import ABCMeta, abstractmethod
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geometry

Base = declarative_base()


class CivicCoverage(Base):
    __tablename__ = 'civiccoverage'

    id = Column(Integer, primary_key=True, nullable=False)
    lostserver = Column(String)
    serviceurn = Column(String)
    country = Column(String)
    a1 = Column(String)
    a2 = Column(String)
    a3 = Column(String)
    a4 = Column(String)
    a5 = Column(String)


class CivicCoverageDataSource(PostgresTabularDataSource):
    """
    Implements the final bits for saving civic coverage data to the database.
    """
    def __init__(self, host='localhost',
                 port=5432,
                 dbname='srgis',
                 user=None,
                 password=None):
        """
        Constructor.

        """
        super().__init__(host, port, dbname, user, password)

    def model_class(self):
        """
        Gets the SQLAlchemy model for the derived class.

        :return: The SQLAlchemy entity class for the derived tabular data source.
        """
        return CivicCoverage

    def fix_model(self, model: CivicCoverage):
        """
        Allows for the model class to be 'fixed up' prior to being saved.

        :param model: An instance of the target model class.
        :return: The fixed up instance of the model.
        """
        if model.serviceurn is None:
            model.serviceurn = 'urn:nena:service:sos'
        if model.country == '*':
            model.country = None
        if model.a1 == '*':
            model.a1 = None
        if model.a2 == '*':
            model.a2 = None
        if model.a3 == '*':
            model.a3 = None
        if model.a4 == '*':
            model.a4 = None
        if model.a5 == '*':
            model.a5 = None

        return model


class CoverageLoaderReceiver(object):
    """
    Base class for coverage loader receivers (the things that do the work.)
    """

    __metaclass__ = ABCMeta

    def __init__(self, coverage_args: CoverageArguments):
        """
        Constructor

        :param coverage_args: Object containing all of the arguments needed to load the coverage data.
        :type coverage_args: :py:class:`CoverageArguments`
        """
        self._coverage_args = coverage_args

    @abstractmethod
    def load_coverage(self):
        """
        Do the work of loading stuff.

        """
        pass


class CivicCoverageLoader(CoverageLoaderReceiver):
    """
    Class that can load civic coverage data.
    """
    def __init__(self, coverage_args: CoverageArguments):
        """
        Constructor

        :param coverage_args: Object containing all of the arguments needed to load the coverage data.
        :type coverage_args: :py:class:`CoverageArguments`
        """
        super().__init__(coverage_args)

    def load_coverage(self):
        """
        Loads civic coverage data.

        """
        print("Loading civic coverage data.")
        source = CsvDataSource(self._coverage_args.csv, has_header=True)
        dest = CivicCoverageDataSource(
            self._coverage_args.dbhost,
            self._coverage_args.dbport,
            self._coverage_args.dbname,
            self._coverage_args.dbuser,
            self._coverage_args.dbpassword)

        source.open()
        dest.open()

        dest.copy_rows(source)


class GeodeticCoverageLoader(CoverageLoaderReceiver):
    """
    Class that can load geodetic coverage data.
    """

    def __init__(self, coverage_args: CoverageArguments):
        """
        Constructor

        :param coverage_args: Object containing all of the arguments needed to load the coverage data.
        :type coverage_args: :py:class:`CoverageArguments`
        """
        super().__init__(coverage_args)

    def load_coverage(self):
        """
        Loads geodetic coverage data.

        """
        source = ShpDataSource(self._coverage_args.shp)

        dest = PostgisDataSource(
            self._coverage_args.dbhost,
            self._coverage_args.dbport,
            self._coverage_args.dbname,
            self._coverage_args.dbuser,
            self._coverage_args.dbpassword)

        source.open()
        dest.open()

        dest.copy_layers(source)


class CoverageLoaderCommand(LoadCommand):
    """
    Command object for loading civic coverage data.
    """
    def __init__(self, receiver: CoverageLoaderReceiver):
        """
        Constructor.

        :param receiver:
        """
        super().__init__(receiver, 'Coverage Loader Command')

    def execute(self):
        """"
        Call the receiver to do the work.

        """
        self._receiver.load_coverage()
