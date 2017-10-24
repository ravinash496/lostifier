#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.db
.. moduleauthor:: Tom Weitzel

Base classes and utilities to help with sources of GIS data.
"""

from abc import ABCMeta, abstractmethod


class GisDataSource(object):
    """
    Abstract base class for GIS data sources.
    """
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        """
        Nothing to see here.

        """

    @abstractmethod
    def open(self):
        """
        Opens up a connection to the given datasource.  Implementation will depend on the underlying data.

        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abstractmethod
    def get_layers(self):
        """
        Get all of the layers in the datasource.

        :return: A list of ogr layer objects.
        """
        pass

    @abstractmethod
    def get_layer_by_name(self, name):
        """
        Get a layer with the given name.

        :param name: The name of the layer.
        :type name: ``str``
        :return: The layer.
        :rtype: An ogr layer object.
        """
        pass

    @abstractmethod
    def copy_layer(self, source, layer_name):
        """
        Copies a single layer from the source.
        :param source: Another instance of a GisDataSource derived class.
        :type source: :py:class``GisDataSource``
        :param layer_name: The name of the layer to copy from the source.
        :type layer_name: ``str``
        :return: The name of the layer copied
        :rtype: ``str``
        """
        pass

    @abstractmethod
    def copy_layers(self, source):
        """
        Copy layers from the given data source.

        :param source: Another instance of a GisDataSource derived class.
        :type source: :py:class``GisDataSource``
        :return: A list of the names of the layers copied.
        :rtype: ``list[str]``
        """
        pass


class TabularDataSource(object):
    """
    Abstract base class for tabular data sources.
    """
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        super().__init__()
        pass

    @abstractmethod
    def open(self):
        """
        Opens up a connection to the given datasource.  Implementation will depend on the underlying data.

        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abstractmethod
    def copy_rows(self, source):
        """
        Copy rows from the given data source.

        :param source: Another instance of a TabularDataSource derived class.
        :type source: :py:class`TabularDataSource`
        :return:
        """
        pass

    @abstractmethod
    def get_field_names(self):
        """
        Get the names of the fields if available.
        :return: A list of the names of the fields.
        :rtype: ``list[str]``
        """
        pass

    @abstractmethod
    def get_rows(self, start_index, count) -> list:
        """
        Get the content of rows.

        :param start_index: The zero-based index of the starting row.
        :type start_index: ``int``
        :param count: The number of rows to fetch.
        :type count: ``int``
        :return: A list of row values as lists.
        :rtype: ``[][]``
        """
        pass
