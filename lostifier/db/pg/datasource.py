#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.db.pg.datasource
.. moduleauthor:: Tom Weitzel

Datasource for tabular data in postgres/postgis.
"""

from lostifier.db.datasource import TabularDataSource, GisDataSource
from abc import ABCMeta, abstractmethod
from osgeo import ogr, gdal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PostgisDataSource(object):
    """
    Abstract base class for GIS data sources.
    """
    __metaclass__ = ABCMeta

    def __init__(self,
                 host='localhost',
                 port=5432,
                 dbname='srgis',
                 user=None,
                 password=None):
        """
        Initializes the coverage loader with database connection information.

        :param host: The name of the database host.
        :type host: ``str``
        :param port: The database port.
        :type port: ``int``
        :param dbname: The ECRF/LVF database name.
        :type dbname: ``str``
        :param user: The database user.
        :type user: ``str``
        :param password: The user's password.
        :type password: ``str``
        """
        self._connection_string = 'PG: host={0} user={1} password={2} dbname={3} port={4}'.format(
            host, user, password, dbname, port
        )
        self._conn = None

    def open(self):
        """
        Opens up a connection to the given datasource.  Implementation will depend on the underlying data.

        :param args:
        :param kwargs:
        :return:
        """
        self._conn = ogr.Open(self._connection_string)

    def get_layers(self):
        """
        Get all of the layers in the datasource.

        :return: A list of ogr layer objects.
        """
        layers = []
        for layer in self._conn:
            layers.append(layer)

        return layers

    def get_layer_by_name(self, name):
        """
        Get a layer with the given name.

        :param name: The name of the layer.
        :type name: ``str``
        :return: The layer.
        :rtype: An ogr layer object.
        """
        self._conn.GetLayer(name)

    def copy_layer(self, source: GisDataSource, layer_name: str):
        """
        Copies a single layer from the source.
        :param source: Another instance of a GisDataSource derived class.
        :type source: :py:class``GisDataSource``
        :param layer_name: The name of the layer to copy from the source.
        :type layer_name: ``str``
        :return: The name of the layer copied.
        :rtype: ``str``
        """
        options = ['OVERWRITE=YES']

        layer = source.get_layer_by_name(layer_name)
        if layer is not None:
            tablename = self._conn.CopyLayer(layer, layer_name, options).GetName()

        return tablename

    def copy_layers(self, source: GisDataSource):
        """
        Copy layers from the given data source.

        :param source: Another instance of a GisDataSource derived class.
        :type source: :py:class``GisDataSource``
        :return: A list of the names of the layers copied.
        :rtype: ``list[str]``
        """
        copied_layers = []
        options = ['OVERWRITE=YES']
        layers = source.get_layers()
        for layer in layers:
            name = layer.GetName()
            copied_layer = self._conn.CopyLayer(layer, name, options).GetName()
            copied_layers.append(copied_layer)

        return copied_layers


class PostgresTabularDataSource(TabularDataSource):
    """
    Base class for Postgres-based tabular data sources.
    """

    __metaclass__ = ABCMeta

    def __init__(self,
                 host='localhost',
                 port=5432,
                 dbname='srgis',
                 user=None,
                 password=None):
        """
        Initializes the coverage loader with database connection information.

        :param host: The name of the database host.
        :type host: ``str``
        :param port: The database port.
        :type port: ``int``
        :param database: The ECRF/LVF database name.
        :type database: ``str``
        :param user: The database user.
        :type user: ``str``
        :param password: The user's password.
        :type password: ``str``
        """
        self._db_connection_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, dbname)
        self._engine = None

    def open(self):
        """
        Opens up a connection to the given datasource.  Implementation will depend on the underlying data.

        :param args:
        :param kwargs:
        :return:
        """
        self._engine = create_engine(self._db_connection_string, echo=True)
        self.model_class().__table__.create(bind=self._engine, checkfirst=True)

    def copy_rows(self, source: TabularDataSource):
        """
        Copy rows from the given data source.

        :param source: Another instance of a TabularDataSource derived class.
        :type source: :py:class`TabularDataSource`
        :param selector: A function that can be used to select which rows to copy.
        :type selector: ``function``
        :return:
        """

        session = sessionmaker()
        session.configure(bind=self._engine)
        s = session()

        names = [x.lower() for x in source.get_field_names()]
        start_index = 0
        num_to_fetch = 10
        keep_going = True
        rows_to_add = []
        while keep_going:
            rows = source.get_rows(start_index, num_to_fetch)
            for row in rows:
                new_model = self._map_row(names, row)
                rows_to_add.append(new_model)

            if len(rows) < num_to_fetch:
                keep_going = False
            else:
                start_index += num_to_fetch

        s.add_all(rows_to_add)
        s.flush()
        s.commit()

    def get_rows(self, start_index, count):
        """
        Get the contenet of rows.

        :param start_index: The zero-based index of the starting row.
        :type start_index: ``int``
        :param count: The number of rows to fetch.
        :type count: ``int``
        :return: The number of rows retrieved.
        :rtype: ``int``
        """
        pass

    @abstractmethod
    def model_class(self):
        """
        Gets the SQLAlchemy model for the derived class.

        :return: The SQLAlchemy entity class for the derived tabular data source.
        """
        pass

    @abstractmethod
    def fix_model(self, model):
        """
        Allows for the model class to be 'fixed up' prior to being saved.

        :param model: An instance of the target model class.
        :return: The fixed up instance of the model.
        """
        pass

    def _map_row(self, names, values):
        """
        Maps a single fow into the model class.

        :param names: The names of the columns.
        :type names: ``[str]``
        :param values: The values for each column.
        :type values: ''[str]``
        :return: An instance of the model class.
        """
        model_cls = self.model_class()
        model = model_cls()

        row_dict = dict(zip(names, values))

        for name in row_dict.keys():
            if hasattr(model, name):
                setattr(model, name, row_dict[name])

        return self.fix_model(model)







